# README
xx银行是一个比较典型的案例。其原有体系是 T+1 批处理，核心数据延迟超过 24 小时，无法支撑实时风控和监控。改造后的链路是：上游 MySQL 等业务库 → Flink CDC 采集 → Kafka 解耦 → Flink ETL 清洗 → Doris（主分析引擎）+ Hudi（历史存储） 。我的问题，我要写代码模拟这个场景，新建几个代码仓库，maven module如何设计，物理架构？

针对你描述的银行实时链路改造场景，要写代码模拟并新建仓库，我的建议是**不要按数据流阶段拆成多个独立仓库**，而是采用 **“1 个父工程 + 3 个 Maven Module”** 的单仓库多模块结构。物理部署上，需要模拟出 Flink 集群（JobManager + TaskManager）、Kafka 集群、MySQL、Doris、Hudi（依托 HDFS/对象存储）的分离部署形态。

### Maven Module 设计

统一管理 Flink（建议 1.20.x，与 CDC 3.x 兼容）、Flink CDC、Doris Connector、Hudi 的版本，避免依赖冲突。

三个子模块划分如下：

| Module 名称 | 职责 | 关键依赖 / 产出 |
| :--- | :--- | :--- |
| **`cdc-ingestion`** | 模拟上游 MySQL 变更捕获，将 Binlog 写入 Kafka 解耦。 | `flink-connector-mysql-cdc`、`flink-connector-kafka`。产出：可提交的 Flink CDC 作业 Jar（或 YAML Pipeline 描述文件）。 |
| **`stream-etl`** | 核心清洗逻辑。消费 Kafka，做数据清洗、维度补全、风控指标预计算，然后双写。 | `flink-connector-kafka`、`flink-doris-connector`、`flink-connector-hudi`。产出：包含 ETL 逻辑的 Flink 作业 Jar。 |
| **`infra-sim`** | 基础设施模拟。**不是** Flink 业务代码，而是 Docker Compose 编排、MySQL 初始化 DDL、Doris 建表语句、Hudi 表 DDL 以及模拟数据生成器。 | `docker-compose.yml`、`init.sql`、数据生成脚本（如 Python/Java 工具）。 |

**为什么这样分？**
`cdc-ingestion` 和 `stream-etl` 虽然都是 Flink 作业，但依赖的连接器差异较大（前者侧重 CDC Source，后者侧重 Doris/Hudi Sink），分开可以避免打出的 Fat Jar 过于臃肿，也方便独立调试。`infra-sim` 独立则是为了把“环境搭建”和“业务逻辑”解耦，让你在本地一键拉起整套物理架构进行联调。

### 物理架构与部署模拟

在本地模拟时，你需要用 Docker Compose 定义以下服务，并注意它们之间的网络连通性。物理拓扑如下：

**1. 数据源层**
- **MySQL**：开启 `binlog_format=ROW` 和 `binlog_row_image=FULL`，并设置唯一的 `server-id`，供 Flink CDC 读取全量+增量数据。
- **模拟数据**：通过脚本持续向 MySQL 的业务表（如 `transaction`、`user_account`）插入/更新数据，制造“实时风控”所需的变更流。

**2. 缓冲与解耦层**
- **Kafka (KRaft 模式)**：无需 ZooKeeper，降低本地资源消耗。创建一个多分区 Topic（如 `bank-cdc-events`），用于接收 Flink CDC 写入的变更事件。这样做的目的是模拟银行场景中“上游业务库变更”与“下游多个实时消费方”之间的解耦。

**3. 计算层**
- **Flink 集群**：需要模拟出 **JobManager** 和至少一个 **TaskManager**。
  - `cdc-ingestion` 作业提交后，从 MySQL 读取快照+Binlog，序列化为 JSON（或 Debezium 格式）写入 Kafka。
  - `stream-etl` 作业从 Kafka 消费，在这里你可以写一个简单的 `KeyedProcessFunction` 或 Flink SQL 做清洗（例如过滤异常金额、按用户 ID 聚合最近 N 笔交易），然后分别写入 Doris 和 Hudi。

**4. 存储与分析层**
- **Doris**：作为主分析引擎，用于实时风控查询。你需要预先在 Doris 中建立 **Unique Key 模型**的表（例如 `risk_event_detail`），利用 Flink Doris Connector 的 Stream Load 实现微批写入，保证低延迟。
- **Hudi**：作为历史存储，模拟长期留存与审计。Flink 写入 Hudi 时，建议表类型设为 **MERGE_ON_READ**，并开启 `changelog.enabled`（如果你需要保留中间变更状态用于回溯）。Hudi 的数据文件实际上落在本地文件系统（或 MinIO，如果 Compose 里加了对象存储服务），在本地模拟时可以直接挂载本地目录作为 `file://` 路径。

**网络与资源要点**：
在 `docker-compose.yml` 中，确保 Flink 容器能解析 `mysql`、`kafka`、`doris` 的主机名。Flink TaskManager 的内存建议给到 2GB 以上，因为同时跑 CDC 和 ETL 双写作业时，如果数据量稍大，内存不足会导致频繁 GC 甚至 OOM。另外，Doris 的 BE 节点在 Docker 中需要足够的 `vm.max_map_count`，如果本地是 macOS 或 Windows，可能需要调整 Docker Desktop 的资源分配或改用 Linux 环境跑这套模拟。

### 代码编写建议

在 `stream-etl` 模块中，建议用 **Flink DataStream API** 而不是纯 SQL 来实现核心 ETL。原因在于：你需要清晰地控制“清洗后写入 Doris”和“原始变更写入 Hudi”这两条分支。DataStream 里的 `side output` 或者 `ProcessFunction` 里的多路输出，能更直观地模拟银行场景中“实时风控（Doris 低延迟查询）”与“合规审计（Hudi 全量留存）”的分流逻辑。

如果你希望进一步降低代码量，也可以利用 Flink CDC 3.x 的 **YAML Pipeline** 特性，在 `cdc-ingestion` 模块里只写一个 YAML 文件来定义 MySQL → Kafka 的同步，把精力集中在 `stream-etl` 的清洗逻辑上。
