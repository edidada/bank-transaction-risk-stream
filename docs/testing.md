# 测试文档

| 项目 | 内容 |
| :--- | :--- |
| 系统名称 | bank-transaction-risk-stream |
| 文档版本 | v1.0 |
| 适用版本 | 代码 `1.0.0-SNAPSHOT` |
| 关联文档 | [架构文档](architecture.md) · [概要设计](high-level-design.md) · [部署（k3s）](deployment-k3s.md) · [运维 SOP](operations-sop.md) |

---

## 一、文档说明

本文定义测试策略、用例清单、执行方法与验收标准。当前基线：**55 个单元测试，全部通过**（`mvn clean package` 输出 `BUILD SUCCESS`）。涉及真实中间件的端到端用例需先按 [README 快速开始](../README.md#三快速开始本地-docker-compose) 或 [部署文档](deployment-k3s.md) 拉起环境。

**原则**

- 业务逻辑（解析、规则、窗口、配置）做成不依赖 Flink 运行时的纯 Java 类，单测无需起集群；
- 外部协议（Doris Stream Load）用 JDK 内置 `HttpServer` 做真实 HTTP 交互验证，不 mock 掉协议细节；
- 不连接真实 MySQL / Doris 的测试必须可在任意机器上离线运行，保证 CI 可用。

---

## 二、测试策略

```mermaid
pyramid
    /\
   /  \   端到端 E2E（Docker Compose / k3s，人工或半自动）
  /----\
 /      \  协议/集成测试（内嵌 HttpServer、Debezium SourceRecord 构造）
/--------\
/          \ 单元测试（JUnit 5，纯 Java 逻辑，55 个）
------------
```

| 层次 | 覆盖对象 | 是否依赖外部服务 | 执行频率 |
| :--- | :--- | :--- | :--- |
| 单元测试 | 配置解析、JSON 契约、清洗脱敏、风控规则、窗口状态、路径/序列化 | 否 | 每次提交 |
| 协议测试 | CDC `SourceRecord` 反序列化、Doris Stream Load HTTP（含 307 / label 幂等） | 否（内嵌服务器） | 每次提交 |
| 端到端测试 | 全链路数据可见性、Savepoint 恢复、DLQ、落盘 | 是（compose / k3s） | 发版前、环境变更后 |

**不测什么**（明确边界）：Flink 框架本身的 Checkpoint 机制、Kafka/Doris 服务端实现、Debezium 的 Binlog 解析正确性——这些由上游组件保证，本工程只验证自身代码与契约。

---

## 三、测试环境与运行方式

### 3.1 环境要求

| 层次 | 要求 |
| :--- | :--- |
| 单元测试 | JDK 11/17、Maven 3.9+，联网以下载 Maven 依赖（首次） |
| 端到端 | Docker Compose v2，≥ 8 GB 内存；或 k3s 集群 |
| Doris BE | Linux 宿主机 `vm.max_map_count ≥ 2000000` |

### 3.2 运行命令

```bash
# 全量：编译 + 全部单元测试 + 打 Fat Jar
mvn clean package

# 单模块
mvn -pl cdc-ingestion test
mvn -pl stream-etl test
mvn -pl infra-sim test

# 只跑某一个测试类
mvn -pl stream-etl test -Dtest=RiskRulesTest

# 跳过测试打包（仅排查构建问题时使用，禁止用于发版）
mvn clean package -DskipTests

# 查看测试报告
ls cdc-ingestion/target/surefire-reports/
ls stream-etl/target/surefire-reports/
```

**成功判据**：`BUILD SUCCESS`，且各模块 `Tests run: N, Failures: 0, Errors: 0, Skipped: 0`。

---

## 四、单元测试清单

### 4.1 `cdc-ingestion`（11 个）

| 测试类 | 用例 ID | 用例 | 断言要点 |
| :--- | :--- | :--- | :--- |
| `JobOptionsTest` | UT-CDC-01 | 默认值与校验 | 默认 host/port/tablename/topic，validate 通过 |
| | UT-CDC-02 | 命令行覆盖默认值 | 覆盖项生效，未覆盖项仍为默认 |
| | UT-CDC-03 | `--key=value` 写法 | 解析正确 |
| | UT-CDC-04 | 非法 `cdc-startup-mode` | 抛 `IllegalArgumentException` 且信息含参数名 |
| | UT-CDC-05 | 非法端口 | 抛 `NumberFormatException` |
| | UT-CDC-06 | 必填项空白 | 抛异常并指出 `kafka-bootstrap` |
| `CdcEventDeserializerTest` | UT-CDC-07 | INSERT 事件转统一 JSON | `op/db/table/ts_ms/after` 正确，`before` 为 null，DECIMAL 保留字符串 |
| | UT-CDC-08 | DELETE 事件保留 `before` | `after` 为 null；Kafka Key 取 `before.transaction_id` |
| | UT-CDC-09 | tombstone 记录 | 不产出任何消息 |
| | UT-CDC-10 | `getProducedType()` | 返回 `Types.STRING` |
| | UT-CDC-11 | UPDATE 事件 Kafka Key | Key = `transaction_id`（同分区有序） |

> 测试通过 `SchemaBuilder` 手工构造 Debezium 信封（`Struct` + `SourceRecord`），无需真实 MySQL。

### 4.2 `stream-etl`（35 个）

| 测试类 | 数量 | 覆盖内容 |
| :--- | ---: | :--- |
| `EtlOptionsTest` | 4 | 默认值、参数覆盖、分数阈值倒置、非法 Kafka 启动模式 |
| `TransactionEventParserTest` | 9 | INSERT/DELETE 镜像选择、金额 BigDecimal、卡号脱敏、`event_time` 回退 `ts_ms`、无镜像返回 null、缺字段/负数/非数字金额、非法 JSON、脱敏边界 |
| `RiskRulesTest` | 8 | LOW 基线、大额、多规则叠加升级 HIGH、凌晨时段、黑名单商户、跨城命中/不命中、阈值倒置 |
| `UserRiskWindowTest` | 5 | 窗口内累加、过期驱逐、乱序不回退、跨城连续事件、非法窗口长度 |
| `DorisStreamLoadClientTest` | 5 | Stream Load 头与 NDJSON body、307 跟随到 BE、`Status=Fail` 抛异常、`LABEL_ALREADY_EXISTS` 视为成功、库表名注入校验 |
| `HudiChangelogSinkTest` | 3 | 按天分区路径、`file://` 与普通路径、Windows 盘符路径 |
| `RiskResultSerializationTest` | 1 | JSON 列为下划线命名（与 Doris 列名一致）、无 camelCase 字段、DECIMAL 数值正确 |

**重点用例说明**

- `DorisStreamLoadClientTest` 使用 `com.sun.net.httpserver.HttpServer` 在随机端口启动真实 HTTP 服务：一个端点直接返回 `Status: Success` 用于校验请求头与 NDJSON body；另一个端点返回 `307 + Location` 用于校验重定向时 POST body 与 `label` 头被完整保留。
- `RiskResultSerializationTest` 直接决定 Doris Stream Load 能否按列名匹配，因此单独验证 `@JsonProperty` 全部为下划线命名。

### 4.3 `infra-sim`（9 个）

| 测试类 | 数量 | 覆盖内容 |
| :--- | ---: | :--- |
| `GeneratorOptionsTest` | 6 | 默认值、参数覆盖、比例非法/求和越界、tps 非法、布尔开关、JDBC URL 去库名 |
| `TransactionFactoryTest` | 3 | 同 seed 输出确定、字段合法且流水号唯一、能注入大额与黑名单样本 |

---

## 五、端到端测试用例

> 前置：`mvn clean package` 通过；`cd infra-sim && docker compose up -d` 全部就绪；两个作业已由 `scripts/submit-jobs.sh` 提交并处于 `RUNNING`；生成器以 `--tps 30` 运行。

| ID | 场景 | 步骤 | 期望结果 |
| :--- | :--- | :--- | :--- |
| E2E-01 | 全量快照 | 先在 MySQL 造 100 条数据，再以 `cdc-startup-mode=initial` 启动 CDC 作业 | Kafka `bank-cdc-events` 出现 `op=r` 快照消息，Doris 行数 ≥ 100 |
| E2E-02 | 增量 INSERT | 生成器写入新交易 | 5s 内 Doris 出现对应 `transaction_id`，`op=c` |
| E2E-03 | UPDATE 事件 | 生成器把 `INIT` 置为 `SUCCESS` | Kafka 中出现 `op=u`，`before`/`after` 均有值；Doris 行按 Unique Key 更新 |
| E2E-04 | DELETE 事件 | 生成器删除较早记录 | Kafka 中出现 `op=d` 且只有 `before`；历史日志保留该行；Doris 不新增风控记录 |
| E2E-05 | 坏记录进 DLQ | 手工向 `bank-cdc-events` 发送 `{"op":"c","after":{"amount":"abc"}}` | 作业不重启；DLQ Topic 出现含 `error` 与 `raw` 的消息；主流继续处理正常数据 |
| E2E-06 | 大额风控 | 写入单笔 ≥ 50000 的交易 | Doris 中该笔 `score ≥ 40`，`reasons` 含 `LARGE_AMOUNT`，`level` 为 MEDIUM 或 HIGH |
| E2E-07 | 高频风控 | 同一 `user_id` 在 10 分钟内写入 ≥ 5 笔 | `window_count ≥ 5`，`reasons` 含 `HIGH_FREQUENCY` |
| E2E-08 | 卡号脱敏 | 查询历史日志与 Doris 结果 | 历史日志保留原始 CDC JSON（源端合规域），Doris/内部事件不存明文；解析后的 `card_no` 形如 `622202******1234` |
| E2E-09 | 分区有序 | 对同一 `transaction_id` 连续 INSERT→UPDATE→DELETE | 三事件落同一 Kafka 分区且顺序为 c→u→d |
| E2E-10 | 历史日志落盘 | 运行 1 分钟后检查目录 | 出现 `dt=yyyy-MM-dd/part-*.ndjson`，每行均为合法 JSON 且含 `op` |
| E2E-11 | Doris 幂等 | 观察 `SHOW LOAD` 中 `LABEL_ALREADY_EXISTS` | 重复提交不产生重复行（`transaction_id` 唯一） |
| E2E-12 | Savepoint 恢复 | 触发 Savepoint → 取消 `stream-etl` → 从 Savepoint 恢复 | 恢复后 `window_count` 等状态延续，Doris 行数连续增长，无全量重放 |
| E2E-13 | k3s 部署验证 | 按 [部署文档](deployment-k3s.md#九部署检查清单) 执行 | 全部检查项通过 |

### 冒烟脚本

```bash
cd infra-sim
bash scripts/smoke-test.sh
```

脚本检查：MySQL 源表计数、Kafka CDC 消息样本、DLQ offset、Doris 风险等级分布与 HIGH 样例、历史日志文件存在性。任一失败返回非零退出码。

### 常用验证 SQL / 命令

```sql
-- Doris（9030）
SELECT level, COUNT(*) FROM risk.risk_event_detail GROUP BY level;
SELECT transaction_id, user_id, score, window_count, window_amount, reasons
FROM risk.risk_event_detail WHERE level='HIGH' ORDER BY event_time DESC LIMIT 20;
SHOW LOAD ORDER BY CreateTime DESC LIMIT 10\G
SHOW BACKENDS\G
```

```bash
# Kafka 消息样本
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic bank-cdc-events --max-messages 3 --from-beginning

# 消费积压
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group bank-risk-etl
```

---

## 六、协议 / 集成测试

| 项 | 方式 | 说明 |
| :--- | :--- | :--- |
| Debezium 事件模型 | `SchemaBuilder` 构造 `Struct` + `SourceRecord` | 覆盖 c/u/d、tombstone、DECIMAL 字符串、嵌套结构 |
| Doris Stream Load | 内嵌 `HttpServer` | 覆盖 200 成功、307 重定向、Fail、label 幂等、Basic Auth、`read_json_by_line` 头 |
| 文件系统写入 | 纯路径计算单测 | 按天分区、`file://`、Windows 盘符 |

**未覆盖**（需端到端或人工验证）：Flink 运行时序列化/状态兼容、真实 Kafka 分区分配、真实 Doris BE 存储、Checkpoint 恢复、容器编排网络。

---

## 七、性能与压测

| 项 | 方法 | 关注指标 |
| :--- | :--- | :--- |
| 基准 | `--tps 20` 运行 10 分钟 | 端到端延迟（源表写入 → Doris 可见）应 ≤ 10s（受 `doris-flush-interval-ms=5000` 主导） |
| 梯度 | 依次 `--tps 50 / 100 / 200`，每档 5 分钟 | Kafka `LAG`、Checkpoint 时长、TaskManager CPU/GC、Doris Stream Load 耗时 |
| 容量 | 逐步提高 `tps` 直到 `LAG` 不收敛 | 得出单 TaskManager 的吞吐上限 |
| 状态 | 构造大量不同 `user_id` | Checkpoint 大小、RocksDB/堆内存、TTL 清理效果 |
| 恢复 | 压测中 kill TaskManager | 作业自动重启、从最近 Checkpoint 恢复、数据不丢（允许 at-least-once 重复） |

记录模板：

| tps | 并行度 | LAG | Checkpoint p95 | Doris 行数/分钟 | 端到端 p95 | 备注 |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| | | | | | | |

---

## 八、验收标准

| # | 标准 | 判据 |
| :-- | :--- | :--- |
| 1 | 构建 | `mvn clean package` → `BUILD SUCCESS`，无编译警告导致失败 |
| 2 | 单元测试 | 55 个用例全部通过，`Failures=0, Errors=0, Skipped=0` |
| 3 | 制品 | 三个 Fat Jar 存在且 `META-INF/MANIFEST.MF` 含正确 `Main-Class` |
| 4 | 作业提交 | 两个作业可提交且 `RUNNING`，Checkpoint 连续成功 ≥ 3 次 |
| 5 | 数据链路 | E2E-01 ~ E2E-04、E2E-06、E2E-10 全部通过 |
| 6 | 容错 | E2E-05（DLQ）、E2E-12（Savepoint 恢复）通过 |
| 7 | 契约 | Kafka JSON 与 Doris 列名符合 [概要设计](high-level-design.md#六数据模型) |
| 8 | 安全 | 卡号脱敏生效（E2E-08） |
| 9 | 文档 | README 与 docs 下 5 份文档与代码版本一致 |
| 10 | k3s | E2E-13 部署检查清单全部勾选 |

---

## 九、CI 建议

最小流水线（示例，不创建文件，按需加入仓库）：

```yaml
name: build
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
          cache: maven
      - run: mvn -B clean package
      - name: 归档测试报告
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: surefire-reports
          path: '**/target/surefire-reports/*.txt'
```

建议补充：

- PR 阶段只跑单元测试（离线、快速）；
- 每日夜间任务用 `docker compose up` 跑 E2E 冒烟脚本；
- 发版前执行 k3s 部署检查清单并留存 Savepoint；
- 依赖版本升级时，强制重新验证「构建 + 作业提交 + Stream Load + Checkpoint 恢复」四项。

---

## 十、缺陷记录模板

| 字段 | 说明 |
| :--- | :--- |
| ID / 发现时间 | 唯一编号与时间戳 |
| 环境 | Compose / k3s、版本 tag、Flink / Doris 版本 |
| 复现步骤 | 最小可复现命令与输入数据（含 Kafka 消息样本） |
| 期望 / 实际 | 对照本文用例 ID |
| 日志 | 作业异常栈、Checkpoint 信息、`SHOW LOAD` 错误 URL |
| 影响面 | 是否丢数 / 重复 / 延迟 / 仅展示 |
| 修复与回归 | 对应单测或 E2E 用例编号 |

> 修复任何生产缺陷时，**必须先补一个能复现该缺陷的单元测试或 E2E 用例**，再改代码。
