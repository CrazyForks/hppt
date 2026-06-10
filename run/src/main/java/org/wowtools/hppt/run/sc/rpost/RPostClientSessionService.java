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
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RPostClientSessionService extends ClientSessionService {
    private static final byte[] WAKEUP_MARKER = new byte[0];

    private BlockingQueue<byte[]> sendQueue;
    private HttpServer server;
    private AtomicBoolean transportConnected;
    private volatile long lastTransportActivityTime;
    private volatile Cb connectionCb;
    private long inactivityTimeoutMillis;

    public RPostClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) throws Exception {
        try {
            ensureInitialized();
            connectionCb = cb;
            inactivityTimeoutMillis = Math.max(1000L, config.heartbeatPeriod > 0 ? config.heartbeatPeriod * 2L : 15000L);
            int port = config.rpost.port;
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", new RPostHandler(config));
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.start();
            log.info("HTTP服务端启动完成，端口 {}", port);
            startTransportWatchdog();
        } catch (Exception e) {
            log.warn("start err", e);
            exit("rpost listener start err:" + e.getMessage());
            throw e;
        }
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        ensureInitialized();
        sendQueue.add(bytes);
    }

    @Override
    protected void doClose() {
        if (null != server) {
            server.stop(0);
        }
    }

    @Override
    protected void onTransportDisconnected(String reason) {
        ensureInitialized();
        sendQueue.clear();
        sendQueue.offer(WAKEUP_MARKER);
    }

    @Override
    protected boolean handleHeartbeatTimeout() {
        ensureInitialized();
        if (transportConnected.compareAndSet(true, false)) {
            transportDisconnected("heartbeat timeout");
        }
        return true;
    }

    private class RPostHandler implements HttpHandler {
        private final ScConfig config;

        RPostHandler(ScConfig config) {
            this.config = config;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                touchTransport();
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
            ensureInitialized();
            long waitMillis = transportConnected.get()
                    ? config.rpost.waitResponseTime
                    : Math.min(1000L, config.rpost.waitResponseTime);
            byte[] rBytes = sendQueue.poll(waitMillis, TimeUnit.MILLISECONDS);
            if (rBytes == WAKEUP_MARKER) {
                sendEmptyResponse(exchange, 200);
                return;
            }
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
            ensureInitialized();
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
                    transportDisconnected("rpost receive err:" + e.getMessage());
                    throw new IOException(e);
                }
            }
            sendEmptyResponse(exchange, 200);
        }
    }

    private void touchTransport() {
        ensureInitialized();
        lastTransportActivityTime = System.currentTimeMillis();
        if (transportConnected.compareAndSet(false, true)) {
            markTransportReady("rpost");
            Cb cb = connectionCb;
            if (cb != null) {
                cb.end(null);
            }
        }
    }

    private void startTransportWatchdog() {
        Thread.startVirtualThread(() -> {
            while (running) {
                try {
                    Thread.sleep(Math.max(1000L, inactivityTimeoutMillis / 2));
                } catch (InterruptedException ignored) {
                }
                if (!running) {
                    return;
                }
                long last = lastTransportActivityTime;
                if (last == 0) {
                    continue;
                }
                if (transportConnected.get() && System.currentTimeMillis() - last > inactivityTimeoutMillis
                        && transportConnected.compareAndSet(true, false)) {
                    transportDisconnected("rpost inactivity timeout");
                }
            }
        });
    }

    private synchronized void ensureInitialized() {
        if (sendQueue == null) {
            sendQueue = new LinkedBlockingQueue<>();
        }
        if (transportConnected == null) {
            transportConnected = new AtomicBoolean();
        }
    }

    private static void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.getResponseHeaders().set("Content-Length", "0");
        exchange.sendResponseHeaders(code, -1);
        exchange.getResponseBody().close();
    }
}
