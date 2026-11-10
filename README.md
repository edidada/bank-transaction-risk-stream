# bank-transaction-risk-stream

银行实时交易风控链路的**可运行模拟工程**（Java / Flink）。

原体系是 T+1 批处理，核心数据延迟超过 24 小时，无法支撑实时风控与监控。本工程用代码完整复现改造后的链路：

```
上游 MySQL 业务库
   │  Binlog（ROW / FULL）
   ▼
Flink CDC 采集（cdc-ingestion）        ← 全量快照 + 增量无缝衔接
   │  统一 JSON，Key = transaction_id
   ▼
Kafka（KRaft，解耦 / 削峰 / 多消费方）
   │
   ▼
Flink ETL（stream-etl）                ← 清洗、脱敏、按用户窗口风控计算
   ├──────────────► Doris（Unique Key）  实时风控查询，秒级可见
   ├──────────────► Hudi 等价历史存储    原始变更日志全量留存，可审计回放
   └──────────────► Kafka DLQ Topic      解析失败的坏记录，不阻断主流
```

> 这是**教学 / 演示用途的模拟实现**：可编译、可测试、可一键拉起整套物理拓扑，但账号口令、单副本、本地文件系统存储均为演示配置，不可直接用于生产。

---

## 一、技术栈与版本

| 组件 | 版本 | 说明 |
| :--- | :--- | :--- |
| JDK | 11（`maven.compiler.release=11`，JDK 17 可编译运行） | Flink 1.20 支持 11/17 |
| Maven | 3.9+ | 父 POM 统一管理依赖 |
| Apache Flink | 1.20.1 | DataStream API + Checkpoint |
| flink-connector-mysql-cdc | 3.6.0-1.20 | 官方适配 Flink 1.20 的 CDC Connector |
| flink-connector-kafka | 3.4.0-1.20 | KafkaSource / KafkaSink |
| Jackson | 2.17.2 | JSON 契约 |
| MySQL | 8.0.39 | `binlog_format=ROW`、`binlog_row_image=FULL` |
| Kafka | 3.8.0（KRaft，无 ZooKeeper） | 6 分区 CDC Topic |
| Apache Doris | 2.1.8.1（1 FE + 1 BE） | Unique Key 模型 + Stream Load |
| JUnit | 5.10.2 | 单元测试 |

**为什么 Doris / Hudi 没有用官方 Connector？**
官方 `flink-doris-connector-1.20:25.0.0` 的编译基准实际是 Flink 1.18（`flink.version=1.18.0`），与 1.20 运行时不兼容；`hudi-flink1.20-bundle` 体积大且与 CDC Connector 存在 Scala / Parquet 依赖冲突风险。因此本工程：

- Doris 走**原生 Stream Load HTTP 协议**（`DorisStreamLoadClient`），用 label 幂等 + 攒批实现 at-least-once；
- Hudi 用**按天分区的 NDJSON 变更日志**等价实现（保留 `op / before / after`，等价于 MOR + `changelog.enabled`）。

生产环境在版本矩阵匹配后，可直接替换这两个 Sink 为官方 Connector，业务代码无需改动。详见 [概要设计文档](docs/high-level-design.md#五关键设计决策)。

---

## 二、模块结构：1 个父工程 + 3 个 Maven Module

不按数据流阶段拆成多个独立仓库，而是**单仓库多模块**，统一锁版本、一次构建。

| Module | 职责 | 主要产出 |
| :--- | :--- | :--- |
| `cdc-ingestion` | Flink CDC 作业：读 MySQL 全量 + Binlog，转成统一 JSON 写 Kafka | 可提交的 Fat Jar |
| `stream-etl` | Flink ETL 作业：消费 Kafka → 清洗/脱敏 → 按用户窗口风控计算 → 双写 Doris 与历史存储；坏记录进 DLQ | 可提交的 Fat Jar |
| `infra-sim` | 基础设施模拟，不含 Flink 业务代码：Docker Compose、建表 DDL、Topic 初始化、运维脚本、Java 交易数据生成器 | compose / SQL / 可执行 Jar |

`cdc-ingestion` 与 `stream-etl` 分开，是因为两者连接器依赖差异大（CDC Source vs Kafka/Doris Sink），独立打包避免 Fat Jar 臃肿、便于分别调试；`infra-sim` 独立则把「环境搭建」与「业务逻辑」解耦。

```
bank-transaction-risk-stream/
├── pom.xml                        # 父 POM：依赖版本管理 + 插件
├── cdc-ingestion/                 # 作业一（Java）
├── stream-etl/                    # 作业二（Java）
├── infra-sim/                     # 环境编排、DDL、数据生成器、脚本
├── deploy/k3s/                    # k3s 部署清单与镜像 Dockerfile
├── docs/                          # 架构 / 概要设计 / 部署 / 运维 SOP / 测试
└── cdc.md                         # Flink CDC 原理补充材料
```

---

## 三、快速开始（本地 Docker Compose）

### 前置条件

- JDK 11/17、Maven 3.9+、Docker Compose v2
- Linux 宿主机建议先执行 `sudo sysctl -w vm.max_map_count=2000000`（Doris BE 需要）
- Windows / macOS 的 Docker Desktop 建议分配 ≥ 8 GB 内存
- 镜像 tag（尤其 `flink:1.20.1-scala_2.12-java11`、`apache/doris:{fe,be}-2.1.8.1`）以 Docker Hub 实际可用版本为准；若拉取失败，替换为同版本的可用 tag 并保持 Flink 为 1.20.1

### 1. 构建

```bash
mvn clean package
```

会执行全部单元测试并生成三个可执行 Jar：

```
cdc-ingestion/target/cdc-ingestion-1.0.0-SNAPSHOT.jar
stream-etl/target/stream-etl-1.0.0-SNAPSHOT.jar
infra-sim/target/infra-sim-1.0.0-SNAPSHOT.jar
```

### 2. 拉起物理架构

```bash
cd infra-sim
docker compose up -d
docker compose ps
```

服务清单：

| 服务 | 容器内主机名 | 宿主端口 | 作用 |
| :--- | :--- | :--- | :--- |
| `mysql` | `mysql` | 3306 | 业务库，开启 ROW Binlog，自动执行 `sql/mysql/01-schema.sql` |
| `kafka` | `kafka` | 9092 | KRaft 单节点，自动创建 `bank-cdc-events`(6) / `bank-cdc-dlq`(3) |
| `flink-jobmanager` | `flink-jobmanager` | 8081 | Flink Web UI / 作业提交入口 |
| `flink-taskmanager` | `flink-taskmanager` | — | 2 个作业共 4 个 slot |
| `doris-fe` | `doris-fe` | 8030 / 9030 | Stream Load HTTP / MySQL 协议查询 |
| `doris-be` | `doris-be` | 8040 | 存储与计算节点 |
| `doris-init` | — | — | 一次性建库建表 + 注册 BE |

### 3. 提交两个 Flink 作业

```bash
cd infra-sim
bash scripts/submit-jobs.sh
```

打开 <http://localhost:8081> 确认两个作业处于 `RUNNING`。

### 4. 制造实时交易流

```bash
mvn -pl infra-sim exec:java -Dexec.args="--jdbc-url jdbc:mysql://localhost:3306/bank --mysql-user root --mysql-password root --tps 30"
```

生成器会持续 INSERT，并穿插 UPDATE（状态流转）与 DELETE，制造完整的 CDC 变更流。

### 5. 端到端验证

```bash
cd infra-sim
bash scripts/smoke-test.sh
```

或直接查 Doris（兼容 MySQL 协议）：

```sql
SELECT level, COUNT(*) FROM risk.risk_event_detail GROUP BY level;
SELECT transaction_id, user_id, score, reasons
FROM risk.risk_event_detail WHERE level = 'HIGH'
ORDER BY event_time DESC LIMIT 20;
```

历史变更日志落在 `infra-sim/data/hudi/bank_transaction_changelog/dt=yyyy-MM-dd/*.ndjson`。

### 6. 停止

```bash
cd infra-sim && docker compose down -v
```

---

## 四、配置说明

三个作业/工具统一使用 `--key value` 或 `--key=value` 传参，优先级：**命令行 > 环境变量 > 默认值**（环境变量名如 `MYSQL_HOST`、`KAFKA_TOPIC`、`JDBC_URL`、`TPS`）。

### cdc-ingestion

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--mysql-host` / `--mysql-port` | `mysql` / `3306` | MySQL 地址 |
| `--mysql-user` / `--mysql-password` | `cdc` / `cdc123456` | CDC 账号，需 `REPLICATION SLAVE/CLIENT` |
| `--mysql-database` / `--mysql-table` | `bank` / `bank.transaction` | 捕获范围 |
| `--mysql-server-id` | `5401-5404` | 不可与 MySQL 及其他 CDC 作业重复 |
| `--cdc-startup-mode` | `initial` | `initial` / `latest` / `earliest` / `snapshot` |
| `--kafka-bootstrap` / `--kafka-topic` | `kafka:9092` / `bank-cdc-events` | 输出 |
| `--parallelism` / `--checkpoint-interval-ms` | `1` / `30000` | 并行度与 Checkpoint 周期 |
| `--job-name` | `cdc-ingestion` | 作业名 |

### stream-etl

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--kafka-bootstrap` / `--kafka-topic` / `--kafka-group` | `kafka:9092` / `bank-cdc-events` / `bank-risk-etl` | 输入 |
| `--kafka-startup-mode` | `earliest` | `earliest` / `latest` / `committed` |
| `--dlq-topic` | `bank-cdc-dlq` | 解析失败记录 |
| `--doris-fe-host` / `--doris-http-port` | `doris-fe` / `8030` | Stream Load 目标 |
| `--doris-database` / `--doris-table` | `risk` / `risk_event_detail` | Unique Key 表 |
| `--doris-user` / `--doris-password` | `root` / 空 | FE 账号 |
| `--doris-batch-size` / `--doris-flush-interval-ms` | `200` / `5000` | 攒批策略 |
| `--hudi-base-path` | `file:///data/hudi/bank_transaction_changelog` | 历史变更日志根目录 |
| `--hudi-flush-every` | `500` | 每 N 条 flush 一次 |
| `--risk-window-ms` | `600000` | 用户滑动窗口（10 分钟） |
| `--risk-high-score` / `--risk-medium-score` | `60` / `30` | 风险等级阈值 |
| `--state-ttl-hours` | `2` | 用户窗口状态 TTL |
| `--parallelism` / `--checkpoint-interval-ms` / `--job-name` | `2` / `30000` / `stream-etl` | 作业级配置 |

### infra-sim 数据生成器

| 参数 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `--jdbc-url` | `jdbc:mysql://localhost:3306/bank?...` | 目标库 |
| `--mysql-user` / `--mysql-password` | `root` / `root` | 写入账号 |
| `--tps` | `20` | 每秒写入笔数（1..10000） |
| `--duration-seconds` | `0` | 0 表示持续运行 |
| `--seed` | `20261110` | 随机种子，保证可复现 |
| `--update-ratio` / `--delete-ratio` | `0.15` / `0.02` | 制造 UPDATE / DELETE 变更的比例 |
| `--init-schema` | `true` | 自动建库建表 |

---

## 五、数据契约与风控规则

### Kafka 消息（`bank-cdc-events`）

Key = `transaction_id`（同一笔交易的所有变更进同一分区，保证有序）；Value 为统一 JSON：

```json
{
  "op": "c|u|d|r",
  "db": "bank",
  "table": "transaction",
  "ts_ms": 1730000000000,
  "before": { "...": "..." },
  "after":  { "...": "..." }
}
```

`r` = 全量快照读取；删除事件取 `before` 镜像。DECIMAL 以字符串传输避免精度丢失。

### 风控规则（`RiskRules`）

| 规则 | 条件 | 分值 |
| :--- | :--- | ---: |
| `LARGE_AMOUNT` | 单笔金额 ≥ 50000 | 40 |
| `HIGH_FREQUENCY` | 窗口内交易笔数 ≥ 5 | 25 |
| `WINDOW_AMOUNT_SPIKE` | 窗口内累计金额 ≥ 100000 | 20 |
| `ODD_HOUR` | 01:00–05:00（Asia/Shanghai） | 10 |
| `CITY_HOPPING` | 1 小时内跨城市交易 | 15 |
| `BLACKLIST_MERCHANT` | 命中 `M-BLACK-001/002` | 30 |

等级：`score ≥ 60` → `HIGH`，`≥ 30` → `MEDIUM`，否则 `LOW`。卡号在 ETL 阶段脱敏（保留前 6 后 4）。

详见 [概要设计文档](docs/high-level-design.md)。

---

## 六、测试

```bash
mvn clean package          # 全模块单元测试
mvn -pl cdc-ingestion test # 仅测 CDC 模块
mvn -pl stream-etl test    # 仅测 ETL 模块
```

测试覆盖：CDC 反序列化契约、配置解析、JSON 清洗与脱敏、风控规则与滑动窗口状态、Doris Stream Load 协议（内嵌 HTTP Server 验证 307 重定向与幂等 label）、Hudi 分区路径、Doris 列名序列化、数据生成器可复现性。

完整端到端测试用例与验收标准见 [测试文档](docs/testing.md)。

---

## 七、文档索引

| 文档 | 内容 |
| :--- | :--- |
| [架构文档](docs/architecture.md) | 分层架构、组件职责、部署视图、数据流、容错与扩展点 |
| [概要设计文档](docs/high-level-design.md) | 模块/类设计、数据模型、接口与参数、规则与状态设计、关键决策 |
| [部署文档（k3s）](docs/deployment-k3s.md) | k3s 集群部署、镜像构建导入、清单说明、验证与回滚 |
| [运维 SOP 文档](docs/operations-sop.md) | 巡检、作业启停与 Savepoint、故障处置、扩缩容、备份恢复 |
| [测试文档](docs/testing.md) | 测试策略、用例清单、端到端验证、压测与验收标准 |
| [Flink CDC 原理补充](cdc.md) | 增量快照算法、Exactly-Once、Schema Evolution 事件模型（位于仓库根目录 `cdc.md`） |

---

## 八、已知边界与生产化建议

1. **语义**：Doris Sink 目前是「攒批 + label 幂等」的 at-least-once；端到端 Exactly-Once 需把 label 绑定 Checkpoint ID 并在 `notifyCheckpointComplete` 提交（官方 Connector 2PC）。
2. **存储**：历史变更日志落本地文件系统；生产应替换为对象存储 `s3a://` + 官方 Hudi Connector（MOR 表）。
3. **高可用**：compose / k3s 清单均为单副本；生产需 3 FE + 多 BE、Kafka 多 broker、Flink HA（ZooKeeper/K8s）、MySQL 主从。
4. **Schema 演进**：当前 `includeSchemaChanges(false)`，上游 DDL 变更需人工同步；生产可开启 Flink CDC 3.x Pipeline 的 Schema Evolution。
5. **安全**：仓库内口令均为演示值；生产使用 Secret / Vault，开启 TLS 与鉴权，交易数据按监管要求加密与脱敏。
6. **状态后端**：默认 `hashmap`；生产建议 RocksDB + 增量 Checkpoint，并配置对象存储作为 Checkpoint 目录。
