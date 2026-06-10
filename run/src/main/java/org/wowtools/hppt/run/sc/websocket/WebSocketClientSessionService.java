package org.wowtools.hppt.run.sc.websocket;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.framing.PingFrame;
import org.java_websocket.handshake.ServerHandshake;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.common.util.ReconnectBackoff;
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

    public WebSocketClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) throws Exception {
        String url = config.websocket.serverUrl + "/s";
        ReconnectBackoff backoff = new ReconnectBackoff(config.transportReconnectBaseDelayMillis,
                config.transportReconnectMaxDelayMillis, config.transportReconnectJitterMillis);
        IoThreadUtil.startIoThread(() -> {
            while (running) {
                final String[] disconnectReason = {"websocket closed"};
                WebSocketClient currentClient = new WebSocketClient(URI.create(url)) {
                    @Override
                    public void onOpen(ServerHandshake handshake) {
                        log.info("ws opened {}", url);
                    }

                    @Override
                    public void onMessage(ByteBuffer bytes) {
                        try {
                            byte[] data = new byte[bytes.remaining()];
                            bytes.get(data);
                            receiveServerBytes(data);
                        } catch (Exception e) {
                            log.warn("receiveServerBytes err", e);
                            disconnectReason[0] = "receiveServerBytes err:" + e.getMessage();
                            close();
                        }
                    }

                    @Override
                    public void onMessage(String message) {
                        //只使用二进制消息，忽略文本消息
                    }

                    @Override
                    public void onClose(int code, String reason, boolean remote) {
                        disconnectReason[0] = "ws closed code=" + code + " reason=" + reason + " remote=" + remote;
                        log.info("{}", disconnectReason[0]);
                    }

                    @Override
                    public void onError(Exception ex) {
                        disconnectReason[0] = "ws error:" + ex.getMessage();
                        if (running) {
                            log.warn("ws error", ex);
                        }
                    }
                };
                currentClient.setConnectionLostTimeout(0);
                try {
                    if (!currentClient.connectBlocking()) {
                        disconnectReason[0] = "ws connectBlocking false";
                    } else {
                        wsClient = currentClient;
                        markTransportReady("websocket");
                        backoff.reset();
                        cb.end(null);
                        while (running && currentClient.isOpen()) {
                            try {
                                Thread.sleep(200);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                } catch (Exception e) {
                    disconnectReason[0] = "ws client err:" + e.getMessage();
                    if (running) {
                        log.warn("ws connect err", e);
                    }
                } finally {
                    closeClient(currentClient);
                }
                if (!running) {
                    return;
                }
                long delayMillis = backoff.nextDelayMillis();
                transportDisconnected(disconnectReason[0]);
                recordTransportReconnect("websocket", disconnectReason[0],
                        backoff.getConsecutiveFailures(), delayMillis);
                sleepForReconnect(delayMillis);
            }
        }, "hppt-io-ws-client");

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
                        WebSocketClient currentClient = wsClient;
                        if (running && currentClient != null && currentClient.isOpen()) {
                            currentClient.sendFrame(new PingFrame());
                        }
                    } catch (Exception e) {
                        log.warn("ping err", e);
                        closeClient(wsClient);
                    }
                }
            });
        }
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        try {
            WebSocketClient currentClient = wsClient;
            if (currentClient != null && currentClient.isOpen()) {
                currentClient.sendDirect(bytes);
            }
        } catch (Exception e) {
            log.warn("sendBytesToServer err", e);
            closeClient(wsClient);
        }
    }

    @Override
    protected void doClose() throws Exception {
        if (null != wsClient) {
            wsClient.close();
        }
    }

    private void closeClient(WebSocketClient currentClient) {
        if (currentClient == null) {
            return;
        }
        try {
            currentClient.close();
        } catch (Exception ignored) {
        }
        if (wsClient == currentClient) {
            wsClient = null;
        }
    }
}
