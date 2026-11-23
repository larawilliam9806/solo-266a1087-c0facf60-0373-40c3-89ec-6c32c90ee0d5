"""GET /groups 使用的资源自释放流式响应。

标准 StreamingResponse 不会在客户端断开时确定性地关闭异步生成器
（依赖事件循环的异步生成器终结钩子/GC）。本类参照 Starlette FileResponse
的做法，让 __call__ 的调用栈直接持有缓冲文件：

- 正常下载结束：循环读完后 finally 关闭缓冲；
- 客户端断开：ASGI 任务组取消本任务，CancelledError 展开时 finally 同样执行；
- 因此无论成功、失败还是断开，本次导出占用的临时文件都会立即释放。

注意 finally 中关闭的是本地临时文件（纯本地系统调用，不涉及网络/SQLite），
直接同步调用即可；若在取消作用域内再 await run_in_threadpool，关闭协程会
被短路而不执行。
"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import BinaryIO

from starlette.concurrency import run_in_threadpool
from starlette.responses import StreamingResponse


class SpooledJSONResponse(StreamingResponse):
    def __init__(
        self,
        spool: BinaryIO,
        chunk_size: int,
        media_type: str = "application/json",
    ) -> None:
        self._spool = spool
        self._chunk_size = chunk_size

        async def body() -> AsyncIterator[bytes]:
            while True:
                chunk = await run_in_threadpool(spool.read, chunk_size)
                if not chunk:
                    break
                yield chunk

        super().__init__(
            content=body(),
            media_type=media_type,
            headers={"Cache-Control": "no-store"},
        )

    async def __call__(self, scope, receive, send) -> None:  # type: ignore[override]
        try:
            await super().__call__(scope, receive, send)
        finally:
            # 本地临时文件，同步关闭；取消场景下也能确定性执行
            self._spool.close()
