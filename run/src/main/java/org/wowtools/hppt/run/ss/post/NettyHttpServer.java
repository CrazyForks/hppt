package org.wowtools.hppt.run.ss.post;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.pojo.BytesList;
import org.wowtools.hppt.common.util.BytesUtil;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
class NettyHttpServer {
    private final int port;
    private final PostServerSessionService postServerSessionService;
    private final SsConfig ssConfig;
    private HttpServer server;

    public NettyHttpServer(int port, PostServerSessionService postServerSessionService, SsConfig ssConfig) {
        this.port = port;
        this.ssConfig = ssConfig;
        this.postServerSessionService = postServerSessionService;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", new PostHandler(postServerSessionService, ssConfig));
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        log.info("服务端启动完成 端口 {}", port);
    }

    public void stop() {
        if (null != server) {
            server.stop(0);
        }
    }
}

@Slf4j
class PostHandler implements HttpHandler {
    private static final byte[] emptyBytes = new byte[0];
    private final PostServerSessionService postServerSessionService;
    private final long replyDelayTime;
    private final long waitResponseTime;

    public PostHandler(PostServerSessionService postServerSessionService, SsConfig ssConfig) {
        this.postServerSessionService = postServerSessionService;
        replyDelayTime = ssConfig.post.replyDelayTime;
        waitResponseTime = ssConfig.post.waitResponseTime;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String uri = exchange.getRequestURI().toString();
            String[] arr = uri.split("\\?", 2);
            String path = arr[0];
            String cookie = arr.length > 1 && arr[1].startsWith("c=") ? arr[1].substring(2) : null;

            if (cookie == null) {
                sendResponse(exchange, 400, emptyBytes);
                return;
            }

            if ("/s".equals(path) && "POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                handleSend(exchange, cookie);
            } else if ("/r".equals(path) && "POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                handleReply(exchange, cookie);
            } else {
                sendResponse(exchange, 404, emptyBytes);
            }
        } catch (IOException e) {
            log.debug("handle io err", e);
        } catch (Exception e) {
            log.warn("handle err", e);
            try {
                sendResponse(exchange, 500, emptyBytes);
            } catch (IOException ignored) {
            }
        }
    }

    private void handleSend(HttpExchange exchange, String cookie) throws IOException {
        PostCtx ctx = postServerSessionService.ctxMap.computeIfAbsent(cookie, (c) -> new PostCtx(cookie));
        receive(ctx, exchange);
        sendResponse(exchange, 200, emptyBytes);
    }

    private void receive(PostCtx ctx, HttpExchange exchange) throws IOException {
        byte[] bytes;
        try (InputStream is = exchange.getRequestBody()) {
            bytes = is.readAllBytes();
        }
        log.debug("收到请求body {}", bytes.length);

        BytesList bytesList = BytesUtil.pbBytes2BytesList(bytes);
        for (byte[] sub : bytesList.getBytes()) {
            postServerSessionService.receiveClientBytes(ctx, sub);
        }
    }

    private void handleReply(HttpExchange exchange, String cookie) throws IOException, InterruptedException {
        PostCtx ctx = postServerSessionService.ctxMap.get(cookie);
        if (ctx != null) {
            write(exchange, ctx);
        } else {
            sendResponse(exchange, 404, emptyBytes);
        }
    }

    private void write(HttpExchange exchange, PostCtx ctx) throws IOException, InterruptedException {
        if (replyDelayTime > 0) {
            Thread.sleep(replyDelayTime);
        }

        List<byte[]> bytesList = new LinkedList<>();
        byte[] rBytes = ctx.sendQueue.poll(waitResponseTime, TimeUnit.MILLISECONDS);
        if (null == rBytes) {
            sendResponse(exchange, 200, emptyBytes);
            return;
        }
        bytesList.add(rBytes);

        ctx.sendQueue.drainToList(bytesList);
        rBytes = BytesUtil.bytesCollection2PbBytes(bytesList);
        log.debug("向客户端发送字节 bytesList {} body {}", bytesList.size(), rBytes.length);
        sendResponse(exchange, 200, rBytes);
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Length", String.valueOf(body.length));
        exchange.sendResponseHeaders(code, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }
}
