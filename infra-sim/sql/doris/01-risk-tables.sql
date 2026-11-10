-- Doris 建表：实时风控明细（主分析引擎）
-- Unique Key 模型保证同一 transaction_id 的重复 Stream Load 幂等覆盖。

CREATE DATABASE IF NOT EXISTS risk;
USE risk;

CREATE TABLE IF NOT EXISTS risk_event_detail (
    transaction_id VARCHAR(64)   NOT NULL COMMENT '交易流水号',
    user_id        VARCHAR(32)   NOT NULL COMMENT '用户ID',
    event_time     BIGINT        NOT NULL COMMENT '交易发生时间(epoch millis)',
    processed_at   BIGINT        NOT NULL COMMENT 'ETL 处理时间(epoch millis)',
    op             VARCHAR(8)    NOT NULL COMMENT 'CDC 操作类型 c/u/d/r',
    amount         DECIMAL(18,2) NOT NULL COMMENT '本笔金额',
    city           VARCHAR(32)   NULL     COMMENT '交易城市',
    merchant_id    VARCHAR(32)   NULL     COMMENT '商户号',
    score          INT           NOT NULL COMMENT '风控分值',
    level          VARCHAR(8)    NOT NULL COMMENT 'HIGH/MEDIUM/LOW',
    window_count   INT           NOT NULL COMMENT '窗口内交易笔数',
    window_amount  DECIMAL(18,2) NOT NULL COMMENT '窗口内累计金额',
    reasons        VARCHAR(512)  NULL     COMMENT '命中规则，| 分隔'
)
UNIQUE KEY(transaction_id)
DISTRIBUTED BY HASH(transaction_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- 常用实时风控查询示例
-- SELECT level, COUNT(*) FROM risk.risk_event_detail GROUP BY level;
-- SELECT * FROM risk.risk_event_detail WHERE level = 'HIGH' ORDER BY event_time DESC LIMIT 20;
