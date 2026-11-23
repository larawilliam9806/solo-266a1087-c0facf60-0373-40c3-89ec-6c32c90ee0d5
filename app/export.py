"""GET /groups 的定长内存流式导出。

设计约束（与需求逐条对应）：

- 固定两条 SQL，查询次数不随输出条目数增长：
    1. strata 按 (group_id, stratum_no) 有序扫描；
    2. graph_edges LEFT JOIN edge_support 按
       (group_id, before_no, after_no, record_no) 有序扫描。
- 两条游标在同一个连接、同一个读事务里交错推进，共享同一份 WAL 快照：
  组内层位、连线与每条线的全部依据记录号必然来自同一份已提交数据；
  扫描期间其他连接继续提交的现场订正不会混入本次响应。
- 游标按批 fetchmany，归并时内存中只保留当前组/边的状态；
  生成的 JSON 边生成边写入 SpooledTemporaryFile（超过阈值自动落盘），
  应用额外内存不随输出条目数增长。
- 取数与 JSON 组装全部完成、读事务提交且连接关闭之后，才开始向客户端
  重放磁盘缓冲。客户端接收慢或暂停只会阻塞文件重放，不会延长数据库读
  事务，也不会妨碍 SQLite 回收 WAL。
- 取数或组装失败时，缓冲尚未开始发送，异常直接向上抛出由路由层转成
  HTTP 500；无论成功、失败还是客户端中途断开，临时文件与连接都会释放。
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from collections import deque
from collections.abc import Iterator
from typing import Any, BinaryIO, Deque

from . import db

# 每批从 SQLite 拉取的行数；内存占用上限约为 BATCH_SIZE 个 Row，与总条目数无关
BATCH_SIZE = 256
# SpooledTemporaryFile 的内存上限（字节），超出后自动切换到磁盘临时文件。
# 取较小值使大响应很快落盘，Python 堆占用不随输出条目数增长；小响应仍留内存。
SPOOL_MAX_MEMORY = 64 * 1024

_STRATA_SQL = (
    "SELECT group_id, stratum_no FROM strata "
    "WHERE group_id IS NOT NULL "
    "ORDER BY group_id, stratum_no"
)
_EDGES_SQL = (
    "SELECT ge.group_id, ge.before_no, ge.after_no, es.record_no "
    "FROM graph_edges ge "
    "LEFT JOIN edge_support es "
    "ON ge.before_no = es.before_no AND ge.after_no = es.after_no "
    # 外层边按 (组, 端点) 走覆盖索引有序；每条边的依据记录由
    # idx_edge_support_lookup 按 record_no 有序产出，故末列无需再排序，
    # 整体输出顺序与显式 ORDER BY ... es.record_no 完全一致且免临时 B-TREE
    "ORDER BY ge.group_id, ge.before_no, ge.after_no"
)


class _BatchCursor:
    """对 sqlite3 游标的定长分批读取包装。

    每次只持有至多 BATCH_SIZE 行，peek/advance 接口供两路有序流归并使用。
    """

    def __init__(self, conn: sqlite3.Connection, sql: str):
        self._cursor = conn.execute(sql)
        self._batch: Deque[sqlite3.Row] = deque()
        self._exhausted = False
        self._refill()

    def _refill(self) -> None:
        if self._exhausted:
            return
        rows = self._cursor.fetchmany(BATCH_SIZE)
        if not rows:
            self._exhausted = True
            self._cursor.close()
            return
        self._batch = deque(rows)

    def peek(self) -> sqlite3.Row | None:
        if self._batch:
            return self._batch[0]
        return None

    def advance(self) -> sqlite3.Row | None:
        row = self.peek()
        if row is not None:
            self._batch.popleft()
            if not self._batch:
                self._refill()
        return row

    def close(self) -> None:
        if not self._exhausted:
            self._cursor.close()
            self._exhausted = True
        self._batch = []


def _dump_json(value: Any) -> str:
    # 与 FastAPI 默认 JSONResponse 的序列化参数保持一致
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _iter_group_documents(
    strata_cursor: _BatchCursor, edges_cursor: _BatchCursor
) -> Iterator[str]:
    """两路有序流按 group_id 归并，逐组生成 JSON 文本片段。

    任意时刻 Python 堆中只保留：当前正在发射的一个层位号/一条边的记录号
    字符串、若干分隔用布尔状态，以及两个定长游标批次。组的层位列表与边的
    记录号列表都不物化，单组再大也不会让应用内存随条目数增长。
    """
    first_group = True
    yield "["

    while True:
        row = strata_cursor.peek()
        if row is None:
            break
        group_id = row["group_id"]

        if not first_group:
            yield ","
        first_group = False
        yield '{"group_id":' + str(int(group_id)) + ',"strata":['

        # ---- 组内层位：strata 流中属于本组的连续片段，边读边发射 ----
        first_stratum = True
        while True:
            srow = strata_cursor.peek()
            if srow is None or srow["group_id"] != group_id:
                break
            strata_cursor.advance()
            if not first_stratum:
                yield ","
            first_stratum = False
            yield _dump_json(srow["stratum_no"])
        yield '],"edges":['

        # ---- 组内连线：跳过不属于本组的边（理论上不会出现，防御性处理）----
        erow = edges_cursor.peek()
        while erow is not None and erow["group_id"] < group_id:
            edges_cursor.advance()
            erow = edges_cursor.peek()

        first_edge = True
        while True:
            erow = edges_cursor.peek()
            if erow is None or erow["group_id"] != group_id:
                break
            if not first_edge:
                yield ","
            first_edge = False
            before = erow["before_no"]
            after = erow["after_no"]
            yield '{"before":' + _dump_json(before) + ',"after":' + _dump_json(after)
            yield ',"record_numbers":['

            # 同一条边的依据记录号是边流中 (before, after) 相同的连续片段，
            # 逐条发射，不聚合为列表
            first_record = True
            while True:
                rec_row = edges_cursor.peek()
                if (
                    rec_row is None
                    or rec_row["group_id"] != group_id
                    or rec_row["before_no"] != before
                    or rec_row["after_no"] != after
                ):
                    break
                edges_cursor.advance()
                # LEFT JOIN 在无依据记录时 record_no 为 NULL，跳过
                if rec_row["record_no"] is None:
                    continue
                if not first_record:
                    yield ","
                first_record = False
                yield _dump_json(rec_row["record_no"])
            yield "]}"

        yield "]}"

    yield "]"


def export_groups_spool(db_path: str | None = None) -> BinaryIO:
    """在单个读事务内把 /groups 响应完整写入临时缓冲并返回。

    返回的可读二进制文件位于文件开头，由调用方负责 close()
    （StreamingResponse 消费结束或客户端断开时会触发）。

    本函数返回前数据库连接已关闭、读事务已结束，因此后续网络发送的
    快慢与数据库/WAL 完全解耦。任何取数或组装异常都会关闭全部资源后抛出。
    """
    conn = db.connect(db_path)
    strata_cursor: _BatchCursor | None = None
    edges_cursor: _BatchCursor | None = None
    spool: tempfile.SpooledTemporaryFile | None = None
    try:
        # 显式开启读事务，两条 SELECT 共享同一 WAL 快照
        conn.execute("BEGIN")
        strata_cursor = _BatchCursor(conn, _STRATA_SQL)
        edges_cursor = _BatchCursor(conn, _EDGES_SQL)
        spool = tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX_MEMORY, mode="w+b")
        for fragment in _iter_group_documents(strata_cursor, edges_cursor):
            spool.write(fragment.encode("utf-8"))
        # 全部取数与组装成功：先结束读事务、关闭连接，再交出缓冲
        conn.commit()
        spool.flush()
        spool.seek(0)
        return spool
    except BaseException:
        # 显式回滚读事务，尽快释放快照，避免依赖连接关闭的隐式行为
        conn.rollback()
        if spool is not None:
            spool.close()
        raise
    finally:
        if strata_cursor is not None:
            strata_cursor.close()
        if edges_cursor is not None:
            edges_cursor.close()
        conn.close()
