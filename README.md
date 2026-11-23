# 考古层位先后关系图服务

管理考古现场记录的层位（stratum）先后关系，并输出只保留**无法经其他层位间接推出**
的连线（有向图传递归约 / transitive reduction）的关系图。

- 后端：FastAPI + Uvicorn
- 存储：SQLite（WAL 模式，标准库 `sqlite3`，零额外 ORM 依赖）
- 部署：Docker Compose 一键启动，数据库落在具名卷 `/data`

## 业务规则与设计对应

| 需求 | 实现方式 |
| --- | --- |
| 先登记层位，再补录记录 | `POST /strata` 先入库；记录批次校验端点层位均已登记，否则 422 |
| 每条记录有唯一编号 + 先/后层位号，同一关系可有多份记录 | `records.record_no` 主键；同 `(before, after)` 的多份记录聚合到同一条图边，响应中列出全部支持记录号 |
| 成批补录、更正、撤回 | `POST /records/batch`，一个请求内混合 `add` / `correct` / `retract`，**整体原子生效** |
| 按层位号查询相连的整组层位 | 忽略边方向求弱连通分量，`GET /groups/by-stratum/{层位号}` 返回整组 |
| 图中只保留不可间接推出的先后线 | DAG 传递归约（`app/graph.py::transitive_reduction`）：逐条边验证"删掉它后两端是否仍可达"，仍可达则省略 |
| 每条线附全部支持记录号 | `edge_support` 表保存归约边 → 记录号映射 |
| 间接路径消失后直接线补回 | 每批次都以当前生效记录全集重新归约，撤回/更正使路径消失时，仍有直接记录的边自动回到图中 |
| 只重算改动前后涉及的层位组 | 受影响顶点 = 批次触碰记录的旧端点 ∪ 新端点；仅拆除并重算这些顶点所在的旧组，其他组数据原样保留（响应 `untouched_groups`） |
| 查询直接读取已生效的图 | 归约结果物化在 `graph_edges` / `edge_support` 表，查询不做临时图计算；WAL 允许写事务进行时并发读 |
| 本批记录和受影响的图（含拆分/合并后的各组）一并生效 | 单事务 `BEGIN IMMEDIATE` 内完成记录改写、旧组拆除、新组（合并或拆分）写入，一起 commit |
| 成环返回环上的层位与记录号 | 内存模拟本批后做 DFS 环检测，409 返回 `cycle`（首尾相同的层位序列）与环上每条边的支持记录号（以本批生效时为准） |
| 成环或保存失败时保留原记录和关系图 | 任何异常都 `ROLLBACK`；成环时记录写入虽先执行，但随回滚全部撤销 |

## `GET /groups` 全库导出的流式设计

归档时该接口一次取齐全库最简层位图。即使单个层位组很大，也要保证应用内存与
SQL 查询次数不随输出条目数增长，且慢客户端不拖累数据库。实现见 `app/export.py`
与 `app/responses.py`：

| 约束 | 实现方式 |
| --- | --- |
| 沿用原 JSON 字段与数组顺序 | 两条 `ORDER BY` 与旧 `_load_group` 完全一致，手写发射的 JSON 与 `json.dumps(..., ensure_ascii=False, separators=(",",":"))` 逐字节相同（有测试做新旧实现字节级对比） |
| SQL 查询次数不随条目数增长 | 固定 **2 条** SELECT：①`strata` 按 `(group_id, stratum_no)` 有序扫描；②`graph_edges LEFT JOIN edge_support` 按 `(group_id, before_no, after_no, record_no)` 有序扫描，无 N+1 |
| 组内层位/连线/依据记录号来自同一份已提交数据 | 两条游标在**同一连接、同一 `BEGIN` 读事务**内交错归并，共享同一 WAL 快照；扫描期间其他连接提交的现场订正不会混入本次响应，订正可继续正常提交 |
| 应用额外内存不随条目数增长 | 游标按批 `fetchmany`（每批 256 行，`deque` O(1) 弹出）；层位号与记录号逐条发射、不物化列表；JSON 边生成边写入 `SpooledTemporaryFile`，超过 64KiB 自动落盘，任意时刻堆中只有定长批次与当前行 |
| 慢/暂停客户端不延长读事务、不妨碍 WAL 回收 | 取数与 JSON 组装**全部完成并 `commit`/关闭连接后**才开始网络发送；客户端只影响磁盘缓冲的重放速度，数据库读事务早已结束，WAL 可随时 checkpoint 回收 |
| 取数/组装失败返回错误 | 失败发生在首个响应字节之前，异常向上抛出由全局处理器返回 HTTP 500 JSON；缓冲与连接在 `except/finally` 中关闭 |
| 下载结束或断开后释放资源 | 自定义 `SpooledJSONResponse` 在 `__call__` 的 `finally` 中同步关闭临时文件，正常结束、对端断开导致任务取消都会确定性触发（不依赖 GC） |

## 目录结构

```
app/
  graph.py      纯图算法：成环检测、弱连通分量、传递归约
  db.py         SQLite 连接、WAL/外键配置、建表脚本
  schemas.py    Pydantic 请求/响应模型
  service.py    事务编排：校验、批次模拟、局部重算、持久化、查询
  export.py     /groups 定长内存、固定 SQL、单快照的流式导出器
  responses.py  资源自释放的流式响应（结束/断开确定性关闭临时文件）
  main.py       FastAPI 路由与异常映射
tests/          pytest 单元测试 + API 端到端测试（37 个用例）
Dockerfile
docker-compose.yml
```

## 快速启动

### Docker Compose

```bash
docker compose up -d --build
curl http://localhost:8000/health
# {"status":"ok"}
```

SQLite 数据库保存在具名卷 `strata-data` 的 `/data/archaeology.db`，
可用 `ARCH_DB_PATH` 环境变量覆盖路径。

### 本地运行

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
ARCH_DB_PATH=./data/archaeology.db .venv/bin/uvicorn app.main:app --reload
```

### 测试

```bash
.venv/bin/pip install pytest httpx
.venv/bin/pytest
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/strata` | 登记层位（重复登记幂等，分别返回 `registered` / `already_existed`） |
| GET | `/strata` | 列出全部层位及其所属组 |
| POST | `/records/batch` | 批量补录/更正/撤回，原子生效 |
| GET | `/records?active_only=true` | 列记录（含已撤回，撤回记录 `active=false`） |
| GET | `/groups` | 全部层位组及其归约图 |
| GET | `/groups/by-stratum/{stratum_no}` | 按层位号查相连整组（核心查询） |

交互式文档：启动后访问 `http://localhost:8000/docs`。

## 使用示例

### 1. 登记层位

```bash
curl -X POST http://localhost:8000/strata \
  -H 'Content-Type: application/json' \
  -d '{"stratum_numbers": ["H1", "H2", "H3", "H4"]}'
```

### 2. 成批补录记录

`R3` 支撑的 `H1→H3` 与 `R1+R2` 的间接路径重复，归约后不会出现在图中，
但记录本身仍保存在 `records` 表。

```bash
curl -X POST http://localhost:8000/records/batch \
  -H 'Content-Type: application/json' \
  -d '{"operations": [
    {"op": "add", "record_no": "R1", "before": "H1", "after": "H2"},
    {"op": "add", "record_no": "R2", "before": "H2", "after": "H3"},
    {"op": "add", "record_no": "R3", "before": "H1", "after": "H3"}
  ]}'
```

响应（图中只剩不可间接推出的两条边）：

```json
{
  "applied_operations": 3,
  "affected_groups": [{
    "group_id": 1,
    "strata": ["H1", "H2", "H3"],
    "edges": [
      {"before": "H1", "after": "H2", "record_numbers": ["R1"]},
      {"before": "H2", "after": "H3", "record_numbers": ["R2"]}
    ]
  }],
  "untouched_groups": []
}
```

### 3. 撤回中间记录，直线自动补回

撤回 `R2` 后 `H1→H3` 不再有间接路径，`R3` 支撑的边重新进入图：

```bash
curl -X POST http://localhost:8000/records/batch \
  -H 'Content-Type: application/json' \
  -d '{"operations": [{"op": "retract", "record_no": "R2"}]}'
```

### 4. 更正记录

```bash
curl -X POST http://localhost:8000/records/batch \
  -H 'Content-Type: application/json' \
  -d '{"operations": [
    {"op": "correct", "record_no": "R1", "before": "H2", "after": "H4"}
  ]}'
```

### 5. 按层位号查整组图

```bash
curl http://localhost:8000/groups/by-stratum/H1
```

尚未参与任何关系的孤立层位返回 `{"group_id": null, "strata": ["H1"], "edges": []}`。

### 6. 成环被驳回（HTTP 409，整批回滚）

```bash
curl -X POST http://localhost:8000/records/batch \
  -H 'Content-Type: application/json' \
  -d '{"operations": [
    {"op": "add", "record_no": "R9", "before": "H3", "after": "H1"}
  ]}'
```

```json
{
  "detail": "本批改动会形成闭环，已整体驳回，原有记录与关系图保持不变",
  "cycle": ["H3", "H1", "H2", "H3"],
  "edges": [
    {"before": "H3", "after": "H1", "record_numbers": ["R9"]},
    {"before": "H1", "after": "H2", "record_numbers": ["R1"]},
    {"before": "H2", "after": "H3", "record_numbers": ["R2"]}
  ]
}
```

## 并发与一致性说明

- 每个批次先在**读阶段**完成前置校验与受影响顶点收集，再用 `BEGIN IMMEDIATE`
  取得写锁后做重算；SQLite 单写多读 + WAL 保证：写事务期间，其他连接对
  **已生效图**的查询不受阻塞。
- 针对互不相交层位组的两个批次，重算数据互不重叠；写锁仅在最后写入阶段短暂
  持有（`busy_timeout=30s`），其他组的订正可正常排队完成。
- 批次内所有操作同生共死：任何一步失败或检测到环，事务回滚，库内状态与
  批次开始前完全一致。
- `GET /groups` 的读事务只覆盖"取数 + 组装"阶段，且在该阶段结束时立即
  `commit` 并关闭连接；之后向客户端重放磁盘缓冲不再持有任何数据库快照，
  因此客户端接收慢或暂停既不会延长读事务，也不会妨碍 SQLite 对 WAL 做
  checkpoint 回收。导出期间现场订正仍可继续提交，只是本次响应保持导出
  开始时的那份快照。
- 大响应的临时缓冲落在 `TMPDIR`（compose 中挂为 512MiB tmpfs），下载结束
  或客户端断开即删除，不写镜像层、不残留宿主磁盘。
