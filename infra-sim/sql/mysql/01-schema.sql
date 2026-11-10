-- MySQL 初始化脚本：业务表 + CDC 专用账号
-- 仅用于本地模拟，口令为演示值。

CREATE DATABASE IF NOT EXISTS bank DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE bank;

-- 交易流水表，Flink CDC 监听此表（bank.transaction）
-- event_time 使用 BIGINT(epoch millis)，避免 Debezium 时间类型在不同配置下的语义差异
CREATE TABLE IF NOT EXISTS `transaction` (
    transaction_id VARCHAR(64)  NOT NULL PRIMARY KEY,
    user_id        VARCHAR(32)  NOT NULL,
    amount         DECIMAL(18,2) NOT NULL,
    currency       VARCHAR(8)   NOT NULL DEFAULT 'CNY',
    status         VARCHAR(16)  NOT NULL DEFAULT 'INIT',
    channel        VARCHAR(16)  NOT NULL,
    merchant_id    VARCHAR(32)  NOT NULL,
    city           VARCHAR(32)  NOT NULL,
    card_no        VARCHAR(32)  NOT NULL,
    event_time     BIGINT       NOT NULL,
    updated_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_user_event (user_id, event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 维表示例：当前版本 CDC 只采集 transaction，维表用于演示后续维度补全扩展
CREATE TABLE IF NOT EXISTS `user_account` (
    user_id    VARCHAR(32) NOT NULL PRIMARY KEY,
    user_name  VARCHAR(64) NOT NULL,
    risk_level VARCHAR(16) NOT NULL DEFAULT 'LOW',
    created_at TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `user_account` (user_id, user_name, risk_level) VALUES
    ('U1001', '演示用户01', 'LOW'),
    ('U1002', '演示用户02', 'LOW'),
    ('U1003', '演示用户03', 'MEDIUM');

-- Flink CDC 账号：需要 REPLICATION SLAVE / CLIENT 才能读取 Binlog
-- mysql_native_password 兼容 Debezium 1.9.x（Flink CDC 3.6.0-1.20 内置版本）
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED WITH mysql_native_password BY 'cdc123456';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'cdc'@'%';
GRANT ALL PRIVILEGES ON bank.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
