# cdc

Flink CDC 的核心原理可以概括为：将数据库的全量快照读取与增量日志捕获（如 Binlog）无缝衔接在同一个 Flink 流式作业中，并通过 Flink 的 Checkpoint 机制实现端到端的 Exactly-Once 语义。它并非简单地把 Debezium 包装一下，而是在事件模型、快照算法与容错机制上做了专门设计。

### 事件模型：数据变更与模式变更分离

Flink CDC 在内部把捕获到的变更封装成两类事件，而不是直接把原始日志丢给下游：

*   DataChangeEvent（数据变更事件）：描述行级数据变化，包含变更前镜像（Before）、变更后镜像（After）以及操作类型（Insert/Update/Delete）。
*   SchemaChangeEvent（模式变更事件）：描述表结构变化，如加列、删列、改类型等。

这种设计的关键在于事件顺序约束：数据变更事件本身不携带表结构信息（为了减小体积、提升序列化效率）。因此框架强制要求，在发送某张表的任何 DataChangeEvent 之前，必须先发送该表的 CreateTableEvent 或对应的 SchemaChangeEvent。这保证了算子处理数据时，始终知道当前表结构是什么。

### 全量与增量的无缝衔接：增量快照算法

这是 Flink CDC 最核心的机制，主要解决“快照期间数据仍在变化”的问题。以 MySQL 为例，其做法基于 DBLog 论文的增量快照思路：

1.  分片并行快照：将大表按主键范围切分成多个 Chunk，每个 Chunk 由一个 Reader 并行读取，提升全量阶段的吞吐。
2.  用位点圈定一致性窗口：对于每个 Chunk，在开始读取前记录一个低位点（LOW），读取结束后记录一个高位点（HIGH）。快照读到的数据，配合回放 `(LOW, HIGH]` 区间内的 Binlog，就能还原出该 Chunk 在某个逻辑时间点的一致性视图。
3.  切换条件：所有快照 Chunk 读完后，必须等一次完整的 Checkpoint 成功，才会启动唯一的 Binlog Reader 进入纯增量阶段。这样能保证即使全量阶段出问题需要恢复，也不会出现数据重复或丢失。

### Exactly-Once 的保证机制

Flink CDC 的 Exactly-Once 依赖两件事：算法上的不重不漏与 Flink Checkpoint 的状态快照。

*   算法层面：上述增量快照算法确保了全量与增量交接时，边界数据不会被重复处理或遗漏。不过需要注意，如果某些配置（如跳过回填）导致高位点直接取低位点，语义可能降级为至少一次。
*   容错层面：Source 算子参与 Flink Checkpoint。作业失败重启后，会从最近一次成功的 Checkpoint 恢复快照进度和 Binlog 消费位点，继续处理。下游 Sink 如果支持事务或幂等写入，配合 Checkpoint 即可实现端到端精确一次。

### 架构角色：Source 与 Sink 的对称设计

从 API 层面看，Flink CDC 的源端与目标端是对称的：

*   源端（Data Source）：由 `EventSource`（负责读变更、转事件）和 `MetadataAccessor`（负责读取表结构元数据）组成。
*   目标端（Data Sink）：由 `EventSink`（负责写入数据变更）和 `MetadataApplier`（负责将模式变更应用到下游系统）组成。

这种拆分让 Flink CDC 不仅能同步数据，还能在开启 Schema Evolution 时，自动将上游的 DDL 同步到下游（例如自动给下游表加列）。

目前公开的 Flink CDC 官方文档中，并没有直接嵌入一张完整的、可直接展示的架构图。不过，根据社区解析和官方 API 文档，可以为你梳理出两个层面的架构视图：一个是宏观的 Pipeline 作业拓扑，另一个是微观的数据流事件模型。

### 宏观架构：Pipeline 作业拓扑

从 Flink CDC 3.x 的运行时角度看，一个同步作业在 Flink 集群内部署后，其拓扑结构大致如下：

```text
┌─────────────────────────────────────────────────────────────┐
│                     Flink CDC Pipeline                      │
│                                                             │
│  ┌──────────┐   ┌─────────┐   ┌───────────┐   ┌──────────┐  │
│  │  Source  │──▶│ Router  │──▶│ Transform │──▶│   Sink   │  │
│  │ Connector│   │ (路由)  │   │  (转换)   │   │ Connector│  │
│  └────┬─────┘   └─────────┘   └───────────┘   └────┬─────┘  │
│       │                                            │        │
│       ▼                                            ▼        │
│  ┌──────────┐                                ┌──────────┐   │
│  │ Metadata │                                │ Metadata │   │
│  │ Accessor │                                │ Applier  │   │
│  └──────────┘                                └──────────┘   │
│       │                                            │        │
└───────┼────────────────────────────────────────────┼────────┘
        │                                            │
        ▼                                            ▼
   ┌─────────┐                                 ┌─────────┐
   │ 外部源   │                                 │ 外部目标 │
   │ (MySQL) │                                 │ (Doris) │
   └─────────┘                                 └─────────┘
```

各组件职责：
*   Source Connector：负责从外部系统捕获变更，转换为内部事件。它内部依赖 `MetadataAccessor` 来读取源端的表结构元数据。
*   Router / Transform：Router 决定事件的路由去向（例如分库分表合并），Transform 负责对数据进行投影或计算。
*   Sink Connector：接收事件并写入目标端。它依赖 `MetadataApplier` 将源端的 Schema 变更（DDL）应用到下游。

### 微观数据流：算子内部事件传递

在 Flink 作业的算子层面，数据以 Event 的形式流动。其核心设计是将数据变更与结构变更分离，并通过严格的顺序来保证正确性。

```text
Source 算子                    SchemaOperator                Sink 算子
    │                              │                           │
    │── CreateTableEvent ─────────▶│                           │
    │   (先发送表结构)              │                           │
    │                              │                           │
    │── DataChangeEvent ──────────▶│── DataChangeEvent ──────▶│
    │   (再发送数据)                │   (透传或转换)             │
    │                              │                           │
    │── SchemaChangeEvent ────────▶│                           │
    │   (如加列)                    │── FlushEvent ───────────▶│
    │                              │   (通知 Sink 刷写旧数据)   │
    │                              │                           │
    │                              │◀── FlushSuccessEvent ─────│
    │                              │   (Sink 确认刷写完成)      │
    │                              │                           │
    │                              │── SchemaChangeEvent ─────▶│
    │                              │   (应用 DDL 到目标端)      │
    │                              │                           │
    │── DataChangeEvent ──────────▶│── DataChangeEvent ──────▶│
    │   (新结构下的数据)            │                           │
```

关键机制：
*   顺序约束：对于新表，必须先发送 `CreateTableEvent`，再发送 `DataChangeEvent`。对于结构变更，必须在变更前后的数据流之间插入 `SchemaChangeEvent`。
*   Flush 屏障：当发生 Schema 变更时，为了防止下游混用新旧结构处理数据，会引入 `FlushEvent`。Sink 收到后必须先将缓存中的旧数据全部刷写落盘，并回复确认，之后才会继续处理新结构的数据和 DDL。

如果你需要查看官方源码仓库中的原始图片，可以访问 Flink CDC 的 GitHub 仓库，在 `docs/static/fig/` 目录下可以找到 `architecture.png` 和 `design.png` 文件。

