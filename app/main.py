"""FastAPI 入口：考古层位先后关系图供数接口。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from . import service
from .responses import SpooledJSONResponse
from .schemas import (
    BatchRequest,
    BatchResponse,
    GroupOut,
    StrataRegisterRequest,
    StrataRegisterResponse,
)

logger = logging.getLogger("archaeology")

# 网络重放块大小：与取数/组装阶段无关，仅控制每次 send() 的字节数
_REPLAY_CHUNK_SIZE = 64 * 1024


@asynccontextmanager
async def lifespan(_app: FastAPI):
    service.ensure_initialized()
    yield


app = FastAPI(
    title="考古层位关系图服务",
    description=(
        "登记层位、批量补录/更正/撤回先后关系记录，"
        "并输出只保留不可间接推出连线的层位关系图。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(service.ValidationFailure)
def _validation_handler(_request: Request, exc: service.ValidationFailure):
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.exception_handler(service.CycleConflict)
def _cycle_handler(_request: Request, exc: service.CycleConflict):
    return JSONResponse(
        status_code=409,
        content={
            "detail": "本批改动会形成闭环，已整体驳回，原有记录与关系图保持不变",
            "cycle": exc.cycle,
            "edges": exc.edges,
        },
    )


@app.exception_handler(Exception)
def _unexpected_handler(request: Request, exc: Exception) -> JSONResponse:
    """取数或组装失败（响应尚未开始发送时）统一返回 JSON 错误。"""
    logger.exception("处理 %s 时发生未预期错误: %s", request.url.path, exc)
    return JSONResponse(status_code=500, content={"detail": "内部错误，导出失败"})


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/strata", response_model=StrataRegisterResponse, status_code=201)
def register_strata(payload: StrataRegisterRequest) -> StrataRegisterResponse:
    result = service.register_strata(payload.stratum_numbers)
    return StrataRegisterResponse(**result)


@app.get("/strata")
def get_strata() -> list[dict]:
    return service.list_strata()


@app.post("/records/batch", response_model=BatchResponse, status_code=201)
def apply_batch(payload: BatchRequest) -> BatchResponse:
    result = service.apply_batch(payload)
    return BatchResponse(**result)


@app.get("/records")
def get_records(
    active_only: bool = Query(default=False, description="仅返回未撤回的记录"),
) -> list[dict]:
    return service.list_records(active_only=active_only)


@app.get("/groups")
async def get_groups() -> SpooledJSONResponse:
    """全库层位组最简层位图。

    取数与 JSON 组装在线程池中完成（SQLite 调用为同步阻塞），期间使用
    定长内存与固定 SQL 数；整个读取处于单读事务的同一快照内。组装完成、
    数据库连接关闭后才返回响应重放磁盘缓冲，因此客户端接收慢或暂停不会
    延长读事务，也不会妨碍 WAL 回收。取数/组装失败时尚未发送任何响应
    字节，异常向上抛出由全局异常处理器转为 500；下载结束或客户端断开时
    由 SpooledJSONResponse 确定性地关闭缓冲、释放临时文件。
    """
    spool = await run_in_threadpool(service.open_groups_export)
    return SpooledJSONResponse(spool, chunk_size=_REPLAY_CHUNK_SIZE)


@app.get("/groups/by-stratum/{stratum_no}", response_model=GroupOut)
def get_group_by_stratum(stratum_no: str) -> GroupOut:
    # 未与任何记录相连的孤立层位 group_id 为 None，仍可正常返回
    result = service.get_group_for_stratum(stratum_no)
    return GroupOut(**result)
