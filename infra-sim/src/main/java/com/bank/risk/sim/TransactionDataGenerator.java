package com.bank.risk.sim;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Random;

/**
 * 模拟上游业务库：持续向 MySQL {@code bank.transaction} 写入交易，并穿插 UPDATE / DELETE，
 * 制造 Flink CDC 可捕获的全量快照与增量变更流。
 *
 * <p>运行示例：
 * <pre>{@code
 * mvn -pl infra-sim exec:java -Dexec.args="--jdbc-url jdbc:mysql://localhost:3306/bank --tps 30"
 * }</pre>
 */
public final class TransactionDataGenerator {

    private static final String INSERT_SQL =
            "INSERT INTO `transaction` (transaction_id, user_id, amount, currency, status, "
                    + "channel, merchant_id, city, card_no, event_time) VALUES (?,?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_SQL =
            "UPDATE `transaction` SET status = ? WHERE transaction_id = ?";
    private static final String DELETE_SQL =
            "DELETE FROM `transaction` WHERE transaction_id = ?";

    private TransactionDataGenerator() {
    }

    public static void main(String[] args) throws Exception {
        GeneratorOptions options = GeneratorOptions.from(args);
        options.validate();

        int tps = options.getInt("tps");
        long durationSeconds = options.getLong("duration-seconds");
        double updateRatio = options.getDouble("update-ratio");
        double deleteRatio = options.getDouble("delete-ratio");
        long seed = options.getLong("seed");

        Connection connection = connect(options);
        if (options.getBoolean("init-schema")) {
            ensureSchema(connection);
        }

        TransactionFactory factory = new TransactionFactory(seed);
        Random opRandom = new Random(seed + 1);
        ArrayDeque<String> recentIds = new ArrayDeque<>();
        long total = 0L;
        long deadline = durationSeconds > 0
                ? System.currentTimeMillis() + durationSeconds * 1000L
                : Long.MAX_VALUE;

        System.out.printf("启动交易数据生成器: tps=%d, duration=%s, seed=%d%n",
                tps, durationSeconds == 0 ? "无限" : durationSeconds + "s", seed);

        try (PreparedStatement insert = connection.prepareStatement(INSERT_SQL);
             PreparedStatement update = connection.prepareStatement(UPDATE_SQL);
             PreparedStatement delete = connection.prepareStatement(DELETE_SQL)) {

            while (System.currentTimeMillis() < deadline) {
                long secondStart = System.currentTimeMillis();
                for (int i = 0; i < tps; i++) {
                    Transaction tx = factory.next(System.currentTimeMillis());
                    insert.setString(1, tx.getTransactionId());
                    insert.setString(2, tx.getUserId());
                    insert.setBigDecimal(3, tx.getAmount());
                    insert.setString(4, tx.getCurrency());
                    insert.setString(5, tx.getStatus());
                    insert.setString(6, tx.getChannel());
                    insert.setString(7, tx.getMerchantId());
                    insert.setString(8, tx.getCity());
                    insert.setString(9, tx.getCardNo());
                    insert.setLong(10, tx.getEventTime());
                    insert.executeUpdate();
                    recentIds.addLast(tx.getTransactionId());
                    if (recentIds.size() > 5000) {
                        recentIds.removeFirst();
                    }
                    total++;

                    double dice = opRandom.nextDouble();
                    if (dice < deleteRatio && recentIds.size() > 100) {
                        // 删除较早的记录，产生 CDC delete 事件（写入 Hudi 变更日志）
                        delete.setString(1, nth(recentIds, opRandom.nextInt(50)));
                        delete.executeUpdate();
                    } else if (dice < deleteRatio + updateRatio && recentIds.size() > 1) {
                        // 把 INIT 交易推进到 SUCCESS，产生 CDC update before/after 镜像
                        update.setString(1, "SUCCESS");
                        update.setString(2, recentIds.getLast());
                        update.executeUpdate();
                    }
                }
                if (total % Math.max(tps, 1000) < tps) {
                    System.out.printf("已写入 %d 条交易，当前窗口内缓存 %d 个流水号%n",
                            total, recentIds.size());
                }
                long elapsed = System.currentTimeMillis() - secondStart;
                if (elapsed < 1000) {
                    Thread.sleep(1000 - elapsed);
                }
            }
        } finally {
            connection.close();
        }
        System.out.printf("生成器结束，共写入 %d 条交易%n", total);
    }

    private static Connection connect(GeneratorOptions options) throws SQLException {
        String url = options.get("jdbc-url");
        String user = options.get("mysql-user");
        String password = options.get("mysql-password");
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException first) {
            if (!options.getBoolean("init-schema")) {
                throw first;
            }
            // 目标库可能尚不存在，先连到实例级创建数据库
            String adminUrl = stripDatabase(url);
            try (Connection admin = DriverManager.getConnection(adminUrl, user, password);
                 Statement statement = admin.createStatement()) {
                statement.executeUpdate(
                        "CREATE DATABASE IF NOT EXISTS bank DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
            }
            return DriverManager.getConnection(url, user, password);
        }
    }

    /** 去掉 JDBC URL 中的数据库名，保留 scheme/host/port 与查询参数。 */
    static String stripDatabase(String url) {
        int scheme = url.indexOf("://");
        if (scheme < 0) {
            return url;
        }
        int pathStart = url.indexOf('/', scheme + 3);
        if (pathStart < 0) {
            return url + "/";
        }
        int query = url.indexOf('?', pathStart);
        return url.substring(0, pathStart + 1) + (query >= 0 ? url.substring(query) : "");
    }

    private static void ensureSchema(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS `transaction` ("
                    + "transaction_id VARCHAR(64) NOT NULL PRIMARY KEY,"
                    + "user_id VARCHAR(32) NOT NULL,"
                    + "amount DECIMAL(18,2) NOT NULL,"
                    + "currency VARCHAR(8) NOT NULL DEFAULT 'CNY',"
                    + "status VARCHAR(16) NOT NULL DEFAULT 'INIT',"
                    + "channel VARCHAR(16) NOT NULL,"
                    + "merchant_id VARCHAR(32) NOT NULL,"
                    + "city VARCHAR(32) NOT NULL,"
                    + "card_no VARCHAR(32) NOT NULL,"
                    + "event_time BIGINT NOT NULL,"
                    + "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "KEY idx_user_event (user_id, event_time)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS `user_account` ("
                    + "user_id VARCHAR(32) NOT NULL PRIMARY KEY,"
                    + "user_name VARCHAR(64) NOT NULL,"
                    + "risk_level VARCHAR(16) NOT NULL DEFAULT 'LOW',"
                    + "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
        }
    }

    private static String nth(ArrayDeque<String> deque, int index) {
        int i = 0;
        for (String value : deque) {
            if (i++ == index) {
                return value;
            }
        }
        return deque.peekFirst();
    }
}
