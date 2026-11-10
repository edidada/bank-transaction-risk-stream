package com.bank.risk.etl.sink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DorisStreamLoadClientTest {

    @Test
    void shouldPostJsonLinesWithStreamLoadHeaders() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> label = new AtomicReference<>();
        AtomicReference<String> format = new AtomicReference<>();
        AtomicReference<String> readByLine = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/risk/risk_event_detail/_stream_load", exchange -> {
            method.set(exchange.getRequestMethod());
            label.set(exchange.getRequestHeaders().getFirst("label"));
            format.set(exchange.getRequestHeaders().getFirst("format"));
            readByLine.set(exchange.getRequestHeaders().getFirst("read_json_by_line"));
            authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 200, "{\"Status\":\"Success\",\"NumberTotalRows\":2}");
        });
        server.start();
        try {
            DorisStreamLoadClient client = new DorisStreamLoadClient(
                    "127.0.0.1", server.getAddress().getPort(),
                    "risk", "risk_event_detail", "root", "secret");

            client.load("risk_etl_label_1", Arrays.asList("{\"a\":1}", "{\"a\":2}"));

            assertEquals("POST", method.get());
            assertEquals("risk_etl_label_1", label.get());
            assertEquals("json", format.get());
            assertEquals("true", readByLine.get());
            assertNotNull(authorization.get());
            assertTrue(authorization.get().startsWith("Basic "));
            assertEquals("{\"a\":1}\n{\"a\":2}\n", body.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldFollowFeRedirectToBe() throws Exception {
        AtomicReference<String> redirectedBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        server.createContext("/api/redirect/x/_stream_load", exchange -> {
            exchange.getResponseHeaders().add("Location",
                    "http://127.0.0.1:" + port + "/api/risk/risk_event_detail/_stream_load");
            respond(exchange, 307, "");
        });
        server.createContext("/api/risk/risk_event_detail/_stream_load", exchange -> {
            redirectedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(exchange, 200, "{\"Status\":\"Success\"}");
        });
        server.start();
        try {
            DorisStreamLoadClient client = new DorisStreamLoadClient(
                    "127.0.0.1", port, "redirect", "x", "root", "");

            client.load("label_redirect", Arrays.asList("{\"a\":1}"));

            assertEquals("{\"a\":1}\n", redirectedBody.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldThrowWhenDorisReportsFailure() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/risk/risk_event_detail/_stream_load", exchange ->
                respond(exchange, 200, "{\"Status\":\"Fail\",\"Message\":\"too many filtered rows\"}"));
        server.start();
        try {
            DorisStreamLoadClient client = new DorisStreamLoadClient(
                    "127.0.0.1", server.getAddress().getPort(),
                    "risk", "risk_event_detail", "root", "");

            Exception error = assertThrows(IOException.class,
                    () -> client.load("label_fail", Arrays.asList("{\"a\":1}")));
            assertTrue(error.getMessage().contains("Fail"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldTreatLabelAlreadyExistsAsSuccess() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/risk/risk_event_detail/_stream_load", exchange ->
                respond(exchange, 200, "{\"Status\":\"LABEL_ALREADY_EXISTS\"}"));
        server.start();
        try {
            DorisStreamLoadClient client = new DorisStreamLoadClient(
                    "127.0.0.1", server.getAddress().getPort(),
                    "risk", "risk_event_detail", "root", "");
            client.load("label_dup", Arrays.asList("{\"a\":1}"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void shouldRejectUnsafeDatabaseOrTableName() {
        assertThrows(IllegalArgumentException.class,
                () -> new DorisStreamLoadClient("127.0.0.1", 8030, "../../admin", "t", "root", ""));
        assertThrows(IllegalArgumentException.class,
                () -> new DorisStreamLoadClient("127.0.0.1", 8030, "risk", "t;drop", "root", ""));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) {
            exchange.getResponseBody().write(bytes);
        }
        exchange.close();
    }
}
