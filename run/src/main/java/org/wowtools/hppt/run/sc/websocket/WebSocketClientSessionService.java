package org.wowtools.hppt.run.sc.websocket;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.PingFrame;
import org.java_websocket.handshake.ServerHandshake;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.net.URI;
import java.nio.ByteBuffer;

/**
 * WebSocket协议客户端 - Java-WebSocket实现
 * SC主动连接SS，使用WebSocket二进制帧传输数据
 *
 * @author liuyu
 * @date 2024/3/12
 */
@Slf4j
public class WebSocketClientSessionService extends ClientSessionService {

    private WebSocketClient wsClient;
    private volatile boolean opened = false;

    public WebSocketClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) throws Exception {
        String url = config.websocket.serverUrl + "/s";
        wsClient = new WebSocketClient(URI.create(url)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                if (!opened) {
                    opened = true;
                    cb.end(null);
                }
            }

            @Override
            public void onMessage(ByteBuffer bytes) {
                try {
                    byte[] data = new byte[bytes.remaining()];
                    bytes.get(data);
                    receiveServerBytes(data);
                } catch (Exception e) {
                    log.warn("receiveServerBytes err", e);
                    exit();
                }
            }

            @Override
            public void onMessage(String message) {
                //只使用二进制消息，忽略文本消息
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("ws closed code={} reason={} remote={}", code, reason, remote);
                exit();
            }

            @Override
            public void onError(Exception ex) {
                log.warn("ws error", ex);
                if (!opened) {
                    opened = true;
                    cb.end(ex);
                } else {
                    exit();
                }
            }
        };
        wsClient.setConnectionLostTimeout(0);
        wsClient.connect();

        //启动ping线程
        long pingInterval = config.websocket.pingInterval;
        if (pingInterval > 0) {
            Thread.startVirtualThread(() -> {
                while (running) {
                    try {
                        Thread.sleep(pingInterval);
                    } catch (InterruptedException ignored) {
                        break;
                    }
                    try {
                        if (running && wsClient != null && wsClient.isOpen()) {
                            wsClient.sendFrame(new PingFrame());
                        }
                    } catch (Exception e) {
                        log.warn("ping err", e);
                        exit();
                    }
                }
            });
        }
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        try {
            if (wsClient != null && wsClient.isOpen()) {
                wsClient.send(bytes);
            }
        } catch (Exception e) {
            log.warn("sendBytesToServer err", e);
            exit();
        }
    }

    @Override
    protected void doClose() throws Exception {
        if (null != wsClient) {
            wsClient.close();
        }
    }
}
