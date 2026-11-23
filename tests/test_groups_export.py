"""GET /groups 流式导出的非功能特性测试。

覆盖需求：
- 沿用原 JSON 字段与数组顺序（与旧实现逐字节一致）；
- SQL 查询次数不随输出条目数增长（固定 2 条 SELECT）；
- 应用额外内存不随输出条目数增长（tracemalloc 峰值对比）；
- 组内数据来自同一份已提交快照，扫描期间的写入不会混入；
- 客户端接收慢/暂停不延长读事务、不妨碍 WAL 回收；
- 客户端断开后本次占用的临时资源被释放；
- 取数/组装失败返回错误响应。
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import tracemalloc

import pytest
from fastapi.testclient import TestClient

from app import db as db_module
from app import export as export_module
from app.schemas import BatchRequest


# ---------------- 夹具与造数工具 ----------------


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("ARCH_DB_PATH", str(db_path))
    from app.main import app

    # raise_server_exceptions=False：服务器端异常经全局处理器返回 500，
    # 而不是被 TestClient 重新抛出，便于断言错误响应
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client


def _register(client, *numbers):
    resp = client.post("/strata", json={"stratum_numbers": list(numbers)})
    assert resp.status_code == 201, resp.text


def _add_chain(client, prefix, size):
    """造一个 size 个层位、size-1 条边、每边 3 份依据记录的大组。"""
    numbers = [f"{prefix}L{i}" for i in range(size)]
    _register(client, *numbers)
    operations = []
    for i in range(size - 1):
        for k in range(3):
            operations.append(
                {
                    "op": "add",
                    "record_no": f"{prefix}R{i}_{k}",
                    "before": numbers[i],
                    "after": numbers[i + 1],
                }
            )
    resp = client.post("/records/batch", json={"operations": operations})
    assert resp.status_code == 201, resp.text


def _groups_body(client) -> bytes:
    with client.stream("GET", "/groups") as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/json")
        return b"".join(resp.iter_bytes())


# ---------------- 功能与字节级兼容 ----------------


def test_groups_stream_matches_legacy_payload_and_order(client):
    _add_chain(client, "A", 6)
    _add_chain(client, "B", 4)

    body = _groups_body(client)
    streamed = json.loads(body)

    # 与旧的整表载入实现语义一致
    from app import service

    legacy = service.list_groups()
    assert streamed == legacy

    # 数组顺序：组按 group_id、层位/连线/记录号各自有序
    assert [g["group_id"] for g in streamed] == sorted(
        g["group_id"] for g in streamed
    )
    for group in streamed:
        assert group["strata"] == sorted(group["strata"])
        edge_keys = [(e["before"], e["after"]) for e in group["edges"]]
        assert edge_keys == sorted(edge_keys)
        for edge in group["edges"]:
            assert edge["record_numbers"] == sorted(edge["record_numbers"])

    # 字段名保持不变
    top = streamed[0]
    assert set(top) == {"group_id", "strata", "edges"}
    assert set(top["edges"][0]) == {"before", "after", "record_numbers"}


def test_empty_database_streams_empty_array(client):
    assert _groups_body(client) == b"[]"


def test_unicode_stratum_numbers_serialized_like_default_json(client):
    _register(client, "层位甲", "层位乙")
    resp = client.post(
        "/records/batch",
        json={
            "operations": [
                {"op": "add", "record_no": "R1",
                 "before": "层位甲", "after": "层位乙"}
            ]
        },
    )
    assert resp.status_code == 201
    body = _groups_body(client)
    # 与 FastAPI 默认 JSONResponse 一样不转义非 ASCII
    assert "层位甲".encode() in body
    json.loads(body)


# ---------------- 固定 SQL 查询次数 ----------------


class _CountingCursor:
    def __init__(self, real):
        self._real = real

    def execute(self, sql, params=()):
        return _CountingCursor(self._real.execute(sql, params))

    def __getattr__(self, name):
        return getattr(self._real, name)


class _CountingConn:
    def __init__(self, real, counter):
        self._real = real
        self._counter = counter

    def execute(self, sql, params=()):
        if sql.lstrip().upper().startswith("SELECT"):
            self._counter.append(sql)
        return _CountingCursor(self._real.execute(sql, params))

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_select_count_is_constant_regardless_of_output_size(client, monkeypatch):
    _add_chain(client, "S", 30)  # 30 层位、29 条边、87 份依据记录

    select_counter: list[str] = []
    real_connect = db_module.connect
    export_active = {"v": False}

    def counting_connect(path=None):
        conn = real_connect(path)
        if not export_active["v"]:
            return conn
        return _CountingConn(conn, select_counter)

    # export.py 通过 db 模块属性访问 connect，patch 模块属性即可生效
    monkeypatch.setattr(db_module, "connect", counting_connect)

    def export_once() -> bytes:
        export_active["v"] = True
        try:
            return _groups_body(client)
        finally:
            export_active["v"] = False

    body_small = export_once()
    small_queries = len(select_counter)
    assert small_queries == 2  # strata 扫描 + 边/依据 LEFT JOIN 扫描

    # 输出条目数扩大一个数量级以上，SELECT 数保持不变
    select_counter.clear()
    _add_chain(client, "L", 400)  # 造数连接不计数
    body_large = export_once()
    assert len(select_counter) == small_queries == 2

    assert len(body_large) > len(body_small) * 5
    assert len(json.loads(body_large)) == 2


# ---------------- 定长内存 ----------------


def test_peak_memory_does_not_grow_with_output_size(monkeypatch, tmp_path):
    """应用额外内存不随输出条目数增长。

    用两个独立数据库对比：数据量相差约 12 倍，两者都会填满定长游标批次并
    触发 spool 落盘（一次性固定成本），因此峰值差应远小于条目数差；
    再用第三个更大的库验证峰值进入平台期，而不是随输出线性增长。
    """
    from app import service

    def build(db_path, prefix, size):
        service.ensure_initialized(db_path)
        numbers = [f"{prefix}L{i}" for i in range(size)]
        service.register_strata(numbers, db_path=db_path)
        ops = []
        for i in range(size - 1):
            for k in range(3):
                ops.append(
                    {
                        "op": "add",
                        "record_no": f"{prefix}R{i}_{k}",
                        "before": numbers[i],
                        "after": numbers[i + 1],
                    }
                )
        service.apply_batch(BatchRequest(operations=ops), db_path=db_path)

    def measure_peak(db_path, size):
        build(db_path, "G", size)
        tracemalloc.start()
        spool = export_module.export_groups_spool(db_path)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        spool.close()
        return peak

    db_small = str(tmp_path / "a.db")
    db_large = str(tmp_path / "b.db")
    db_xl = str(tmp_path / "c.db")
    peak_small = measure_peak(db_small, 200)
    peak_large = measure_peak(db_large, 600)
    peak_xl = measure_peak(db_xl, 1500)

    # 条目数 3 倍 / 2.5 倍增长，峰值只允许 1.6 倍以内的常数级浮动
    assert peak_large < peak_small * 1.6, (peak_small, peak_large)
    assert peak_xl < peak_large * 1.6, (peak_large, peak_xl)
    # 200 与 1500 相差 7.5 倍条目，峰值差远小于条目数差
    assert peak_xl < peak_small * 2.5, (peak_small, peak_xl)


# ---------------- 单一快照一致性 ----------------


def test_concurrent_commits_during_export_do_not_mix_into_snapshot(
    client, monkeypatch
):
    _add_chain(client, "A", 20)

    snapshot = json.loads(_groups_body(client))
    assert len(snapshot) == 1
    original_group_ids = {g["group_id"] for g in snapshot}

    barrier = threading.Event()
    errors: list[Exception] = []

    real_iter = export_module._iter_group_documents

    def paused_iter(strata_cursor, edges_cursor):
        # 取数开始后、扫描中途放行写入线程，模拟导出期间的现场订正
        barrier.set()
        yield from real_iter(strata_cursor, edges_cursor)

    monkeypatch.setattr(export_module, "_iter_group_documents", paused_iter)

    def writer():
        try:
            barrier.wait(timeout=5)
            _add_chain(client, "B", 15)  # 全新的组，提交进库
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    body = _groups_body(client)
    thread.join(timeout=10)

    assert errors == []
    streamed = json.loads(body)
    # 本次响应只包含导出开始时已提交的那一份数据
    assert {g["group_id"] for g in streamed} == original_group_ids
    # 写入确实已落库：随后一次读取能看到新组
    assert len(json.loads(_groups_body(client))) == 2


# ---------------- 读事务时长与 WAL 回收 ----------------


def test_db_connection_closed_before_response_replay_starts(client, monkeypatch):
    _add_chain(client, "W", 40)

    opened: list = []
    real_connect = db_module.connect

    def tracking_connect(path=None):
        conn = real_connect(path)
        opened.append(conn)
        return conn

    monkeypatch.setattr(db_module, "connect", tracking_connect)

    with client.stream("GET", "/groups") as resp:
        # 响应头已返回意味着取数/组装阶段结束：连接必须已经关闭
        assert resp.status_code == 200
        assert opened, "导出过程中应当打开过数据库连接"
        for conn in opened:
            # 连接关闭后再执行 SQL 会抛 ProgrammingError
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        resp.read()


def test_wal_can_be_checkpointed_while_client_pauses(client, tmp_path):
    db_file = os.environ["ARCH_DB_PATH"]
    _add_chain(client, "C", 30)
    _register(client, "CL1", "CL2")

    # 制造新的 WAL 帧
    extra = client.post(
        "/records/batch",
        json={
            "operations": [
                {"op": "add", "record_no": "EXTRA1",
                 "before": "CL1", "after": "CL2"}
            ]
        },
    )
    assert extra.status_code == 201, extra.text

    # 导出已经把完整响应缓冲好；此时模拟客户端暂停（只取响应头不读体）
    with client.stream("GET", "/groups") as resp:
        assert resp.status_code == 200
        # 用一个独立连接执行 TRUNCATE checkpoint：导出连接已关闭，
        # 不存在长读事务占用旧快照，WAL 必须可以回收
        checker = sqlite3.connect(db_file)
        try:
            checker.execute("PRAGMA wal_autocheckpoint=0")
            busy, log_frames, checkpointed = checker.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            assert (busy, log_frames, checkpointed) == (0, 0, 0), (
                busy,
                log_frames,
                checkpointed,
            )
            wal_path = db_file + "-wal"
            assert not os.path.exists(wal_path) or os.path.getsize(wal_path) == 0
        finally:
            checker.close()
        body = b"".join(resp.iter_bytes())  # 客户端恢复接收，内容仍完整
    json.loads(body)


# ---------------- 断开与错误处理 ----------------


def test_spool_released_after_client_disconnect(client, monkeypatch):
    _add_chain(client, "D", 25)

    live_files: set = set()
    real_factory = tempfile.SpooledTemporaryFile

    def tracking_factory(*args, **kwargs):
        f = real_factory(*args, **kwargs)
        live_files.add(f)
        original_close = f.close

        def closing():
            live_files.discard(f)
            return original_close()

        f.close = closing
        return f

    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", tracking_factory)

    with client.stream("GET", "/groups") as resp:
        assert resp.status_code == 200
        # 只读一块就关闭，模拟客户端中途断开
        next(resp.iter_bytes())
    # 退出 stream 上下文即关闭响应，生成器 finally 必须关闭缓冲
    assert live_files == set()


def test_export_failure_returns_error_and_releases_resources(
    client, monkeypatch, tmp_path
):
    _add_chain(client, "E", 10)

    live_files: set = set()
    real_factory = tempfile.SpooledTemporaryFile

    def tracking_factory(*args, **kwargs):
        f = real_factory(*args, **kwargs)
        live_files.add(f)
        original_close = f.close

        def closing():
            live_files.discard(f)
            return original_close()

        f.close = closing
        return f

    monkeypatch.setattr(tempfile, "SpooledTemporaryFile", tracking_factory)

    def boom(*_args, **_kwargs):
        raise sqlite3.OperationalError("simulated read failure")

    monkeypatch.setattr(export_module._BatchCursor, "_refill", boom)

    resp = client.get("/groups")
    assert resp.status_code == 500
    assert json.loads(resp.content)["detail"]
    # 失败路径不残留临时缓冲
    assert live_files == set()


# ---------------- 大单组与落盘边界 ----------------


def test_very_large_single_group_streams_complete_and_ordered(client):
    """题目核心场景：单个层位组很大，输出必须完整且顺序正确。"""
    size = 800
    width = len(str(size - 1))  # 等宽编号，使字典序与数值序一致

    _add_chain(client, "BIG", size)
    # _add_chain 用非等宽编号，这里单独用等宽编号重建一个库验证顺序
    db_path = os.environ["ARCH_DB_PATH"]
    from app import service

    service.register_strata([f"ORD{i:0{width}d}" for i in range(size)],
                            db_path=db_path)
    ops = []
    for i in range(size - 1):
        for k in range(2):
            ops.append({
                "op": "add", "record_no": f"ORR{i:0{width}d}_{k}",
                "before": f"ORD{i:0{width}d}",
                "after": f"ORD{i + 1:0{width}d}",
            })
    service.apply_batch(BatchRequest(operations=ops), db_path=db_path)

    groups = json.loads(_groups_body(client))
    target = next(g for g in groups if g["strata"][0].startswith("ORD"))

    # 层位完整、有序（SQLite 文本 ORDER BY 下等宽编号即数值序）
    assert target["strata"] == [f"ORD{i:0{width}d}" for i in range(size)]

    # 每条连线都在，顺序为 (before, after) 升序，每条线挂 2 份依据记录号
    assert len(target["edges"]) == size - 1
    for i, edge in enumerate(target["edges"]):
        assert edge["before"] == f"ORD{i:0{width}d}"
        assert edge["after"] == f"ORD{i + 1:0{width}d}"
        assert edge["record_numbers"] == [
            f"ORR{i:0{width}d}_0", f"ORR{i:0{width}d}_1"
        ]

    # 非等宽编号的 BIG 组也必须完整无缺（仅校验计数，顺序按 SQLite 文本序）
    big = next(g for g in groups if g["strata"][0].startswith("BIG"))
    assert len(big["strata"]) == size
    assert len(big["edges"]) == size - 1
    assert sum(len(e["record_numbers"]) for e in big["edges"]) == (size - 1) * 3


def test_many_small_groups_all_present_in_order(client):
    """组数量很多时，每个组都不丢且按 group_id 升序。"""
    for g in range(12):
        _add_chain(client, f"G{g}_", 4)

    groups = json.loads(_groups_body(client))
    assert len(groups) == 12
    ids = [g["group_id"] for g in groups]
    assert ids == sorted(ids)
    for group in groups:
        assert len(group["strata"]) == 4
        assert len(group["edges"]) == 3


def test_large_response_rolls_over_to_disk_then_replays(monkeypatch, tmp_path):
    """超过内存阈值的响应必须落盘，且重放内容与流式生成结果一致。"""
    from app import service

    db_path = str(tmp_path / "roll.db")
    service.ensure_initialized(db_path)
    # 足够大的层位号让输出确定超过 64KiB 内存阈值
    size = 2000
    numbers = [f"XL-LAYER-{i:05d}" for i in range(size)]
    service.register_strata(numbers, db_path=db_path)
    ops = []
    for i in range(size - 1):
        ops.append({
            "op": "add", "record_no": f"XR-RECORD-{i:05d}",
            "before": numbers[i], "after": numbers[i + 1],
        })
    service.apply_batch(BatchRequest(operations=ops), db_path=db_path)

    spool = export_module.export_groups_spool(db_path)
    # 输出远超 64KiB 内存阈值，必然已 rollover 到真实磁盘文件
    assert getattr(spool, "_rolled", False) is True
    payload = spool.read()
    spool.close()

    assert len(payload) > export_module.SPOOL_MAX_MEMORY
    parsed = json.loads(payload)
    assert len(parsed[0]["strata"]) == size
    assert len(parsed[0]["edges"]) == size - 1


def test_small_response_stays_in_memory(monkeypatch, tmp_path):
    """小响应不触发落盘，保持内存路径的低延迟。"""
    from app import service

    db_path = str(tmp_path / "small.db")
    service.ensure_initialized(db_path)
    service.register_strata(["S1", "S2"], db_path=db_path)
    service.apply_batch(BatchRequest(operations=[
        {"op": "add", "record_no": "SR1", "before": "S1", "after": "S2"}
    ]), db_path=db_path)

    spool = export_module.export_groups_spool(db_path)
    assert getattr(spool, "_rolled", False) is False
    spool.close()


def test_response_headers_mark_json_and_no_store(client):
    _add_chain(client, "H", 5)
    with client.stream("GET", "/groups") as resp:
        assert resp.headers["content-type"] == "application/json"
        assert resp.headers["cache-control"] == "no-store"
        resp.read()


def test_connection_closed_when_cursor_construction_fails(monkeypatch, tmp_path):
    """BEGIN 之后、取数早期失败：连接必须回滚并关闭，不泄漏。"""
    from app import service

    db_path = str(tmp_path / "fail.db")
    service.ensure_initialized(db_path)
    service.register_strata(["F1", "F2"], db_path=db_path)
    service.apply_batch(BatchRequest(operations=[
        {"op": "add", "record_no": "FR1", "before": "F1", "after": "F2"}
    ]), db_path=db_path)

    state = {"opened": 0, "closed": 0}
    real_connect = db_module.connect

    class _FailingConn:
        def __init__(self, inner):
            self._inner = inner
            state["opened"] += 1

        def execute(self, sql, params=()):
            if sql.lstrip().upper().startswith("SELECT") and "FROM strata" in sql:
                raise sqlite3.OperationalError("simulated disk I/O error")
            return self._inner.execute(sql, params)

        def commit(self):
            return self._inner.commit()

        def rollback(self):
            return self._inner.rollback()

        def close(self):
            state["closed"] += 1
            return self._inner.close()

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(
        db_module, "connect", lambda path=None: _FailingConn(real_connect(path))
    )

    with pytest.raises(sqlite3.OperationalError):
        export_module.export_groups_spool(db_path)
    assert state == {"opened": 1, "closed": 1}, state
