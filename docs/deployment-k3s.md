# 部署文档（k3s）

| 项目 | 内容 |
| :--- | :--- |
| 系统名称 | bank-transaction-risk-stream |
| 文档版本 | v1.0 |
| 目标环境 | 单节点 / 多节点 k3s 集群（模拟、联调、演示） |
| 关联文档 | [架构文档](architecture.md) · [概要设计](high-level-design.md) · [运维 SOP](operations-sop.md) · [测试](testing.md) |

> 本地开发联调请用 `infra-sim/docker-compose.yml`（见 [README](../README.md#三快速开始本地-docker-compose)）。本文描述 **k3s** 部署。清单位于 `deploy/k3s/`，均为演示级单副本配置，生产化差距见 [架构文档第 10 节](architecture.md#10-现状与生产化差距)。

---

## 一、前置条件

| 项 | 要求 | 检查命令 |
| :--- | :--- | :--- |
| k3s | v1.28+（自带 Traefik、local-path  provisioner） | `kubectl get nodes` |
| 节点资源 | ≥ 8 vCPU / 16 GB 内存 / 60 GB 磁盘 | `free -h`、`df -h` |
| 内核参数 | `vm.max_map_count ≥ 2000000`（Doris BE） | `sysctl vm.max_map_count` |
| 工具 | JDK 11/17、Maven 3.9+、Docker、`kubectl` | `mvn -v`、`docker -v` |
| 网络 | 节点可拉取 Docker Hub 镜像 | `docker pull hello-world` |
| 本机域名 | Flink / Doris UI | 配置 `flink.bank-risk.local`、`doris.bank-risk.local` |

Linux 节点设置内核参数（**必须在启动 Doris 之前**）：

```bash
sudo sysctl -w vm.max_map_count=2000000
echo 'vm.max_map_count=2000000' | sudo tee -a /etc/sysctl.conf
```

### k3s 安装（如尚未安装）

按 k3s 官方文档安装单节点集群，例如：

```bash
curl -sfL https://get.k3s.io | sh -
sudo systemctl status k3s
sudo k3s kubectl get nodes
mkdir -p ~/.kube && sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config && sudo chown $(id -u):$(id -g) ~/.kube/config
```

确认默认 StorageClass 存在：

```bash
kubectl get storageclass     # 应看到 local-path (default)
```

---

## 二、清单文件说明

`deploy/k3s/` 目录：

| 文件 | 资源 | 作用 |
| :--- | :--- | :--- |
| `00-namespace.yaml` | Namespace `bank-risk` | 资源隔离 |
| `01-config.yaml` | ConfigMap ×3 + Secret | `flink-conf.yaml`、MySQL 初始化 SQL、Doris 建表 SQL、演示账号 |
| `02-mysql.yaml` | PVC + Deployment + Service | MySQL 8.0.39，ROW Binlog |
| `03-kafka.yaml` | Headless Service + Service + StatefulSet + Job | Kafka 3.8.0 KRaft 单节点 + Topic 初始化 |
| `04-flink.yaml` | PVC ×2 + Deployment ×2 + Service | JobManager ×1、TaskManager ×2 |
| `05-doris.yaml` | Service ×3 + StatefulSet ×2 + Job | Doris FE/BE + 建表/注册 BE |
| `06-submit-jobs.yaml` | Job | 一次性提交两个 Flink 作业（**不在 kustomization 中，需单独 apply**） |
| `07-ingress.yaml` | Ingress | Traefik 暴露 Flink UI / Doris FE HTTP |
| `08-data-generator.yaml` | Deployment | 持续造数 |
| `Dockerfile.job-runner` | 镜像构建 | 把两个 Fat Jar 打进 Flink 客户端镜像 |
| `Dockerfile.generator` | 镜像构建 | 打包 `infra-sim` Fat Jar |
| `kustomization.yaml` | Kustomization | 一键 apply 除 `06` 外的全部资源 |

**设计要点**

- 配置以 ConfigMap `flink-config` 为唯一来源，通过 `FLINK_PROPERTIES` 环境变量注入官方 Flink 镜像的 entrypoint，避免把 ConfigMap 挂载成只读 `flink-conf.yaml` 时与 entrypoint 生成配置冲突。
- 有状态组件用 `local-path` StorageClass 的 PVC；`flink-checkpoints` 与 `hudi-data` 两个 RWO 卷被 JobManager 与 TaskManager 同时挂载，**单节点 k3s 可行**；多节点集群必须换 RWX（Longhorn / NFS）或对象存储。
- 服务发现统一使用 `<svc>.bank-risk.svc.cluster.local`；StatefulSet 用 `<pod>.<headless>.bank-risk.svc.cluster.local` 稳定 DNS。
- 自建镜像设为 `imagePullPolicy: IfNotPresent`，通过 `k3s ctr images import` 导入，无需私有仓库。

---

## 三、部署步骤

### 步骤 1：构建制品

在仓库根目录执行：

```bash
mvn clean package
```

确认三个 Fat Jar 存在：

```bash
ls -lh cdc-ingestion/target/cdc-ingestion-1.0.0-SNAPSHOT.jar \
       stream-etl/target/stream-etl-1.0.0-SNAPSHOT.jar \
       infra-sim/target/infra-sim-1.0.0-SNAPSHOT.jar
```

### 步骤 2：构建并导入自建镜像

k3s 使用 containerd，Docker 本地镜像不会自动可见，必须导出后导入：

```bash
docker build -t bank-risk-job-runner:1.0.0 -f deploy/k3s/Dockerfile.job-runner .
docker build -t bank-risk-data-generator:1.0.0 -f deploy/k3s/Dockerfile.generator .

docker save bank-risk-job-runner:1.0.0     | sudo k3s ctr images import -
docker save bank-risk-data-generator:1.0.0 | sudo k3s ctr images import -

sudo k3s ctr images ls | grep bank-risk
```

> 多节点集群需要把镜像导入**每个节点**，或推送到私有仓库并把清单中的镜像名改为仓库地址。

### 步骤 3：应用基础资源

```bash
kubectl apply -k deploy/k3s
```

等待全部工作负载就绪：

```bash
kubectl -n bank-risk get pods -w
kubectl -n bank-risk get pvc
kubectl -n bank-risk get svc
```

预期状态：

| 工作负载 | 期望 |
| :--- | :--- |
| `mysql-*` | `Running` 1/1（readinessProbe 通过） |
| `kafka-0` | `Running` 1/1 |
| `flink-jobmanager-*` | `Running` 1/1 |
| `flink-taskmanager-*` ×2 | `Running` 1/1 |
| `doris-fe-0` / `doris-be-0` | `Running` 1/1 |
| `data-generator-*` | `Running` 1/1 |
| `kafka-init-topics-*` / `doris-init-*` | `Completed` |

检查初始化 Job 日志：

```bash
kubectl -n bank-risk logs job/kafka-init-topics
kubectl -n bank-risk logs job/doris-init
```

`doris-init` 成功日志应包含 `SHOW BACKENDS` 输出且 `Alive: true`。

### 步骤 4：提交 Flink 作业

```bash
kubectl -n bank-risk apply -f deploy/k3s/06-submit-jobs.yaml
kubectl -n bank-risk logs -f job/submit-flink-jobs
```

日志末尾应列出两个 `RUNNING` 作业（`cdc-ingestion`、`stream-etl`）。

> **重要**：该 Job 不具备幂等性。重复 apply 会重复提交作业，导致同一 Consumer Group 内重复实例。重新提交前先按 [运维 SOP](operations-sop.md#三作业启停与-savepoint) 取消已有作业，并删除旧 Job：`kubectl -n bank-risk delete job submit-flink-jobs`。

### 步骤 5：验证部署

```bash
# Flink UI（Ingress）
curl -H 'Host: flink.bank-risk.local' http://<节点IP>/overview

# Doris FE HTTP
curl -H 'Host: doris.bank-risk.local' http://<节点IP>/api/bootstrap

# 风控结果
kubectl -n bank-risk run doris-client --rm -it --restart=Never \
  --image=mysql:8.0.39 -- \
  mysql -h doris-fe.bank-risk.svc.cluster.local -P 9030 -uroot \
  -e "SELECT level, COUNT(*) FROM risk.risk_event_detail GROUP BY level;"

# 历史变更日志落盘（在挂载 PVC 的节点上）
kubectl -n bank-risk exec deploy/flink-taskmanager -- \
  sh -c 'find /data/hudi -name "*.ndjson" | head'
```

完整端到端用例见 [测试文档](testing.md#五端到端测试用例)。

### 步骤 6：配置本机 hosts（访问 UI）

```
<节点IP>  flink.bank-risk.local
<节点IP>  doris.bank-risk.local
```

浏览器访问 `http://flink.bank-risk.local` 与 `http://doris.bank-risk.local`。

---

## 四、配置说明

### 4.1 修改配置的位置

| 想改什么 | 改哪里 |
| :--- | :--- |
| Flink 并行度 / Checkpoint / 状态路径 | `deploy/k3s/01-config.yaml` → ConfigMap `flink-config` |
| MySQL / Doris 账号口令 | `deploy/k3s/01-config.yaml` → Secret `bank-risk-credentials`（同时改 `01-config.yaml` 中 MySQL 初始化 SQL 的 `cdc` 口令） |
| MySQL 表结构 | `deploy/k3s/01-config.yaml` → ConfigMap `mysql-init-sql` |
| Doris 表结构 | `deploy/k3s/01-config.yaml` → ConfigMap `doris-init-sql` |
| 作业运行参数 | `deploy/k3s/06-submit-jobs.yaml` 的 `args` |
| 造数速率 | `deploy/k3s/08-data-generator.yaml` 的 `TPS` 环境变量 |
| 镜像版本 | 各清单的 `image` 字段 + `Dockerfile.*` 的 FROM |

修改 ConfigMap 后需重启对应 Deployment 才会生效：

```bash
kubectl -n bank-risk rollout restart deploy/flink-jobmanager deploy/flink-taskmanager
```

### 4.2 作业参数（k3s 版与 compose 版的差异）

| 参数 | compose | k3s |
| :--- | :--- | :--- |
| `--mysql-host` | `mysql` | `mysql.bank-risk.svc.cluster.local` |
| `--kafka-bootstrap` | `kafka:9092` | `kafka.bank-risk.svc.cluster.local:9092` |
| `--doris-fe-host` | `doris-fe` | `doris-fe.bank-risk.svc.cluster.local` |
| `--hudi-base-path` | `file:///data/hudi/...` | 相同（PVC 挂载到 `/data/hudi`） |
| 提交方式 | `docker compose exec ... flink run -d` | `flink run -d -m flink-jobmanager:8081` |

全部参数含义见 [概要设计文档](high-level-design.md#四模块详细设计)。

### 4.3 资源配额基线

| 工作负载 | requests | limits |
| :--- | :--- | :--- |
| mysql | 500m / 1Gi | 2 / 4Gi |
| kafka | 500m / 1Gi | 2 / 3Gi |
| flink-jobmanager | 500m / 1Gi | 2 / 2Gi |
| flink-taskmanager ×2 | 1 / 2Gi | 4 / 4Gi |
| doris-fe | 1 / 2Gi | 2 / 8Gi |
| doris-be | 1 / 2Gi | 4 / 8Gi |
| data-generator | 200m / 512Mi | 1 / 1Gi |

合计约 5.7 vCPU / 12.5 GB requests，需保证节点有足够余量。

---

## 五、与 Docker Compose 的对照

| 维度 | Docker Compose | k3s |
| :--- | :--- | :--- |
| 入口 | `infra-sim/docker-compose.yml` | `deploy/k3s/kustomization.yaml` |
| 有状态存储 | 命名卷 + `./data` 绑定目录 | PVC（`local-path`） |
| 配置注入 | `FLINK_PROPERTIES` 环境变量 | ConfigMap → `FLINK_PROPERTIES` 环境变量 |
| 初始化 | `kafka-init` / `doris-init` 一次性 service | 同名 Kubernetes Job |
| 作业 Jar | 挂载 `infra-sim/jobs/` | 打进 `bank-risk-job-runner` 镜像 |
| 作业提交 | `scripts/submit-jobs.sh` | `Job: submit-flink-jobs` |
| 造数 | 宿主机 `mvn -pl infra-sim exec:java` | `Deployment: data-generator` |
| 外部访问 | 宿主端口 8081 / 8030 / 9030 / 3306 / 9092 | Traefik Ingress（仅 UI/HTTP） |
| 适用场景 | 本地开发、快速验证 | 团队共享联调、演示、准生产演练 |

---

## 六、扩缩容

```bash
# 扩 TaskManager（同时确认 slot 与 Kafka 分区数匹配）
kubectl -n bank-risk scale deploy/flink-taskmanager --replicas=3

# 提高造数速率
kubectl -n bank-risk set env deploy/data-generator TPS=100
```

**扩容顺序建议**：先扩 TaskManager slot → 再扩 Kafka 分区（分区数只能增不能减）→ 最后提高 `tps` / `doris-batch-size`。注意两个 Flink 作业的 `--parallelism` 是提交时固定的，扩容后需按 SOP 用新并行度重新提交。

缩容前先做 Savepoint，避免状态迁移过程丢数据。

---

## 七、升级与回滚

### 7.1 升级作业代码

```bash
# 1) 取消作业并保存 Savepoint（见运维 SOP）
kubectl -n bank-risk exec deploy/flink-jobmanager -- \
  /opt/flink/bin/flink cancel -s /data/flink/savepoints <jobId>

# 2) 重新构建 Jar 与镜像、导入、滚动更新
mvn clean package
docker build -t bank-risk-job-runner:1.0.1 -f deploy/k3s/Dockerfile.job-runner .
docker save bank-risk-job-runner:1.0.1 | sudo k3s ctr images import -
# 修改 06-submit-jobs.yaml 的 image 与 jar 版本

# 3) 用 Savepoint 恢复
kubectl -n bank-risk delete job submit-flink-jobs --ignore-not-found
kubectl -n bank-risk apply -f deploy/k3s/06-submit-jobs.yaml
```

### 7.2 回滚

1. 取消新作业；
2. 把 `06-submit-jobs.yaml` 的镜像 tag 改回上一版本；
3. 用升级前保存的 Savepoint 重新提交；
4. 若只是配置回滚：`kubectl -n bank-risk rollout undo deploy/<name>`。

### 7.3 卸载

```bash
kubectl -n bank-risk delete job submit-flink-jobs --ignore-not-found
kubectl delete -k deploy/k3s
kubectl -n bank-risk get pvc     # 确认数据卷是否需保留
```

> `kubectl delete -k` 不会自动删除 PVC（取决于 provisioner 回收策略），如需彻底清理请手动 `kubectl -n bank-risk delete pvc --all`。

---

## 八、常见问题

| 现象 | 原因 | 处理 |
| :--- | :--- | :--- |
| 远端镜像 `ImagePullBackOff` / `manifest unknown` | 清单中的镜像 tag 不存在或已下线（Docker Hub 标签会调整） | 到 Docker Hub 核对可用 tag 后统一修改清单；**Flink 必须保持 1.20.1**，否则与作业 Jar 不兼容 |
| Pod `ImagePullBackOff`（自建镜像） | 镜像未导入 k3s containerd | `sudo k3s ctr images import -`，确认 `imagePullPolicy: IfNotPresent` |
| Pod `Pending`，事件 `waiting for a volume to be created` | `local-path` provisioner 异常 / 磁盘不足 | `kubectl -n bank-risk describe pvc`、检查节点磁盘 |
| Doris BE 反复重启 | `vm.max_map_count` 过小 | 在**节点**执行 `sysctl -w vm.max_map_count=2000000` |
| `doris-init` 一直 `CrashLoopBackOff` | FE/BE 未就绪 | 属正常重试（backoffLimit=10）；查看 `kubectl logs job/doris-init` |
| Flink 作业 `RUNNING` 但无数据 | Kafka 服务名/Topic 不对、CDC 账号权限不足 | 检查 `06-submit-jobs.yaml` 参数与 `mysql-init-sql` 的 GRANT |
| `sinkTo`/`addSink` 相关 ClassCast / NoSuchMethod | 作业 Jar 与集群 Flink 版本不一致 | 统一为 Flink 1.20.1（见 [概要设计](high-level-design.md#十依赖版本与兼容性)） |
| 多节点下 TaskManager 挂载 PVC 失败 | RWO 卷不能跨节点多 Pod 挂载 | 换 RWX 存储或把状态/日志目录改为 `s3a://` 对象存储 |
| Ingress 404 | 未配置 hosts / Host 头 | `curl -H 'Host: flink.bank-risk.local' http://<节点IP>/overview` |
| 重复提交导致双份消费 | `submit-flink-jobs` 被重复 apply | 取消旧作业并删除 Job 后再 apply |

---

## 九、部署检查清单

- [ ] `mvn clean package` 全部通过（含单元测试）
- [ ] 两个自建镜像已导入到所有 k3s 节点
- [ ] `vm.max_map_count ≥ 2000000`
- [ ] `kubectl apply -k deploy/k3s` 成功，全部 Pod `Running`
- [ ] `kafka-init-topics` / `doris-init` Job `Completed`，BE `Alive: true`
- [ ] `submit-flink-jobs` Job `Completed`，Flink UI 两个作业 `RUNNING`
- [ ] Doris 中能查到 `risk.risk_event_detail` 的 HIGH/MEDIUM/LOW 分布
- [ ] `/data/hudi` 下出现 `dt=*/part-*.ndjson`
- [ ] Ingress 可访问 Flink UI 与 Doris FE
- [ ] 已记录本次部署的镜像 tag 与 Savepoint 路径，便于回滚
