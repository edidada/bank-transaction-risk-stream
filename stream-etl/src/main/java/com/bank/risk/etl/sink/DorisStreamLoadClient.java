package com.bank.risk.etl.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Doris Stream Load HTTP 客户端。
 *
 * <p>为什么不用 {@code flink-doris-connector}：该 Connector 25.0.0 的编译基准是 Flink 1.18，
 * 与本工程的 Flink 1.20 运行时不兼容；且模拟工程只需 Stream Load 微批写入，
 * 直接走 HTTP 协议可以显著减小 Fat Jar 体积。生产环境可在版本匹配后替换为官方 Connector。
 *
 * <p>幂等：Doris 以 label 去重，同一 label 重复提交会返回 {@code LABEL_ALREADY_EXISTS}，
 * 这里按成功处理，配合 Sink 的「失败批次复用 label 重试」实现 at-least-once 语义下的不重复。
 *
 * <p>FE 返回 307 时会重定向到 BE，这里手工跟随（保留 POST body 与全部 Stream Load 头）。
 */
public class DorisStreamLoadClient implements AutoCloseable {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_REDIRECTS = 3;

    private final URI loadUri;
    private final String authorization;
    private final HttpClient httpClient;

    public DorisStreamLoadClient(String host, int port, String database, String table,
                                 String user, String password) {
        if (!IDENTIFIER.matcher(database).matches() || !IDENTIFIER.matcher(table).matches()) {
            throw new IllegalArgumentException("Doris 库名/表名只能包含字母、数字和下划线");
        }
        this.loadUri = URI.create("http://" + host + ":" + port
                + "/api/" + database + "/" + table + "/_stream_load");
        this.authorization = "Basic " + Base64.getEncoder().encodeToString(
                (user + ":" + (password == null ? "" : password)).getBytes(StandardCharsets.UTF_8));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
    }

    public URI loadUri() {
        return loadUri;
    }

    public void load(String label, List<String> jsonLines) throws IOException, InterruptedException {
        if (jsonLines == null || jsonLines.isEmpty()) {
            return;
        }
        StringBuilder body = new StringBuilder();
        for (String line : jsonLines) {
            body.append(line).append('\n');
        }
        HttpResponse<String> response = post(loadUri, label, body.toString(), 0);
        if (response.statusCode() != 200) {
            throw new IOException("Doris Stream Load HTTP " + response.statusCode()
                    + ", label=" + label + ", body=" + response.body());
        }
        String status = parseStatus(response.body());
        if (!"Success".equalsIgnoreCase(status)
                && !"Publish Timeout".equalsIgnoreCase(status)
                && !"LABEL_ALREADY_EXISTS".equalsIgnoreCase(status)) {
            throw new IOException("Doris Stream Load 失败, label=" + label
                    + ", Status=" + status + ", body=" + response.body());
        }
    }

    private static String parseStatus(String responseBody) throws IOException {
        JsonNode json = MAPPER.readTree(responseBody);
        String status = json.path("Status").asText("");
        if (!status.isEmpty()) {
            return status;
        }
        // 部分版本把错误码放在 msg/code 中
        return json.path("msg").asText("");
    }

    private HttpResponse<String> post(URI uri, String label, String body, int redirects)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", authorization)
                .header("Content-Type", "text/plain; charset=UTF-8")
                .header("format", "json")
                .header("read_json_by_line", "true")
                .header("strip_outer_array", "false")
                .header("label", label)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 307 && redirects < MAX_REDIRECTS) {
            String location = response.headers().firstValue("Location").orElse(null);
            if (location != null) {
                return post(URI.create(location), label, body, redirects + 1);
            }
        }
        return response;
    }

    @Override
    public void close() {
        // Java 11 HttpClient 无需显式关闭；保留 AutoCloseable 便于未来替换实现
    }
}
