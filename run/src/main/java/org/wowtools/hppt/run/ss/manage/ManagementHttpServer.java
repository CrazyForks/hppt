package org.wowtools.hppt.run.ss.manage;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.Constant;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;

@Slf4j
public final class ManagementHttpServer implements AutoCloseable {
    private static final String API_PREFIX = "/api/v1";

    private final SsRuntimeState runtimeState;
    private final SsConfig.ManagementConfig config;
    private HttpServer httpServer;

    public ManagementHttpServer(SsRuntimeState runtimeState, SsConfig.ManagementConfig config) {
        this.runtimeState = runtimeState;
        this.config = config;
    }

    public boolean enabled() {
        return config != null && config.port > 0;
    }

    public void start() throws IOException {
        if (!enabled()) {
            return;
        }
        String host = normalizeHost(config.host);
        String token = normalizeToken(config.token);
        if (!isLocalHost(host) && token.isEmpty()) {
            throw new IllegalArgumentException("management.token is required when management.host is not local: " + host);
        }

        InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(host), config.port);
        httpServer = HttpServer.create(address, 0);
        httpServer.createContext(API_PREFIX + "/health", this::handleHealth);
        httpServer.createContext(API_PREFIX + "/status", this::handleStatus);
        httpServer.createContext(API_PREFIX + "/sessions", this::handleSessions);
        httpServer.createContext(API_PREFIX + "/clients", this::handleClients);
        httpServer.createContext(API_PREFIX + "/restart", this::handleRestart);
        httpServer.createContext(API_PREFIX + "/stop", this::handleStop);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.start();
        log.info("SS management server started at http://{}:{}", host, config.port);
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!preHandle(exchange, "GET")) {
            return;
        }
        writeJson(exchange, runtimeState.healthHttpStatus(), runtimeState.health());
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!preHandle(exchange, "GET")) {
            return;
        }
        writeJson(exchange, 200, runtimeState.status());
    }

    private void handleSessions(HttpExchange exchange) throws IOException {
        if (!preHandle(exchange, "GET")) {
            return;
        }
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("sessions", runtimeState.sessions());
        writeJson(exchange, 200, res);
    }

    private void handleClients(HttpExchange exchange) throws IOException {
        if (!preHandle(exchange, "GET")) {
            return;
        }
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("clients", runtimeState.clients());
        writeJson(exchange, 200, res);
    }

    private void handleRestart(HttpExchange exchange) throws IOException {
        if (!preHandle(exchange, "POST")) {
            return;
        }
        boolean accepted = runtimeState.requestRestart();
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("accepted", accepted);
        res.put("message", accepted ? "restart requested" : "no running service");
        writeJson(exchange, accepted ? 202 : 409, res);
    }

    private void handleStop(HttpExchange exchange) throws IOException {
        if (!preHandle(exchange, "POST")) {
            return;
        }
        boolean force = "true".equalsIgnoreCase(parseQuery(exchange.getRequestURI()).get("force"));
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("accepted", true);
        res.put("force", force);
        res.put("message", force ? "force stop requested" : "stop requested");
        writeJson(exchange, 202, res);
        if (force) {
            Thread.startVirtualThread(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                }
                log.warn("force stop requested by management api, System.exit(0)");
                System.exit(0);
            });
        } else {
            runtimeState.requestStop();
        }
    }

    private boolean preHandle(HttpExchange exchange, String expectedMethod) throws IOException {
        if (!expectedMethod.equalsIgnoreCase(exchange.getRequestMethod())) {
            writeJson(exchange, 405, Map.of("error", "method not allowed"));
            return false;
        }
        String token = normalizeToken(config.token);
        if (!token.isEmpty()) {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (!("Bearer " + token).equals(auth)) {
                writeJson(exchange, 401, Map.of("error", "unauthorized"));
                return false;
            }
        }
        return true;
    }

    private static Map<String, String> parseQuery(URI uri) {
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return Map.of();
        }
        Map<String, String> res = new LinkedHashMap<>();
        String[] items = query.split("&");
        for (String item : items) {
            int idx = item.indexOf('=');
            if (idx < 0) {
                res.put(item, "");
            } else {
                res.put(item.substring(0, idx), item.substring(idx + 1));
            }
        }
        return res;
    }

    private static void writeJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = Constant.jsonObjectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json;charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static String normalizeHost(String host) {
        if (host == null || host.isBlank()) {
            return "127.0.0.1";
        }
        return host.trim();
    }

    private static String normalizeToken(String token) {
        return token == null ? "" : token.trim();
    }

    private static boolean isLocalHost(String host) {
        return "127.0.0.1".equals(host)
                || "::1".equals(host)
                || "localhost".equalsIgnoreCase(host);
    }

    @Override
    public void close() {
        if (httpServer != null) {
            log.info("SS management server stopped");
            httpServer.stop(0);
        }
    }
}
