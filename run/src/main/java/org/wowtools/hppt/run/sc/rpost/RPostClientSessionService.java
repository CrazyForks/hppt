package org.wowtools.hppt.run.sc.rpost;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.BytesUtil;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RPostClientSessionService extends ClientSessionService {

    private final BlockingQueue<byte[]> sendQueue = new LinkedBlockingQueue<>();
    private HttpServer server;

    public RPostClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) {
        try {
            int port = config.rpost.port;
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", new RPostHandler(config));
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.start();
            log.info("HTTP服务端启动完成，端口 {}", port);
            cb.end(null);
        } catch (Exception e) {
            log.warn("start err", e);
            exit();
            cb.end(e);
        }
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        sendQueue.add(bytes);
    }

    @Override
    protected void doClose() {
        if (null != server) {
            server.stop(0);
        }
    }

    private class RPostHandler implements HttpHandler {
        private final ScConfig config;

        RPostHandler(ScConfig config) {
            this.config = config;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String path = exchange.getRequestURI().getPath();
                if ("/s".equals(path)) {
                    sendResponse(exchange);
                } else if ("/r".equals(path)) {
                    receiveBytes(exchange);
                } else {
                    sendEmptyResponse(exchange, 404);
                }
            } catch (IOException e) {
                log.debug("handle io err", e);
            } catch (Exception e) {
                log.warn("handle err", e);
                try {
                    sendEmptyResponse(exchange, 500);
                } catch (IOException ignored) {
                }
            }
        }

        private void sendResponse(HttpExchange exchange) throws IOException, InterruptedException {
            byte[] rBytes = sendQueue.poll(config.rpost.waitResponseTime, TimeUnit.MILLISECONDS);
            if (null != rBytes) {
                List<byte[]> bytesList = new LinkedList<>();
                bytesList.add(rBytes);
                sendQueue.drainTo(bytesList);
                rBytes = BytesUtil.bytesCollection2PbBytes(bytesList);
                log.debug("向客户端发送字节 {}", rBytes.length);

                exchange.getResponseHeaders().set("Content-Length", String.valueOf(rBytes.length));
                exchange.sendResponseHeaders(200, rBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(rBytes);
                }
            } else {
                sendEmptyResponse(exchange, 200);
            }
        }

        private void receiveBytes(HttpExchange exchange) throws IOException {
            byte[] bytes;
            try (InputStream is = exchange.getRequestBody()) {
                bytes = is.readAllBytes();
            }
            Collection<byte[]> bytesList = BytesUtil.pbBytes2BytesList(bytes).getBytes();
            for (byte[] sub : bytesList) {
                try {
                    receiveServerBytes(sub);
                } catch (Exception e) {
                    log.warn("接收字节异常", e);
                    exit();
                }
            }
            sendEmptyResponse(exchange, 200);
        }
    }

    private static void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.getResponseHeaders().set("Content-Length", "0");
        exchange.sendResponseHeaders(code, -1);
        exchange.getResponseBody().close();
    }
}
