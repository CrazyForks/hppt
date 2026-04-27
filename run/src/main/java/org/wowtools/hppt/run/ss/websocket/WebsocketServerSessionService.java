package org.wowtools.hppt.run.ss.websocket;

import lombok.extern.slf4j.Slf4j;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * WebSocket协议服务端 - Java-WebSocket实现
 * SS监听端口，接受SC的WebSocket连接，使用二进制帧传输数据
 *
 * @author liuyu
 * @date 2024/2/7
 */
@Slf4j
public class WebsocketServerSessionService extends ServerSessionService<WebSocket> {

    private WebSocketServer server;

    public WebsocketServerSessionService(SsConfig ssConfig) throws Exception {
        super(ssConfig);
    }

    @Override
    protected void init(SsConfig ssConfig) throws Exception {
        server = new WebSocketServer(new InetSocketAddress(ssConfig.port)) {
            @Override
            public void onOpen(WebSocket conn, ClientHandshake handshake) {
                //连接建立
            }

            @Override
            public void onClose(WebSocket conn, int code, String reason, boolean remote) {
                removeCtx(conn);
            }

            @Override
            public void onMessage(WebSocket conn, ByteBuffer message) {
                try {
                    byte[] data = new byte[message.remaining()];
                    message.get(data);
                    receiveClientBytes(conn, data);
                } catch (Exception e) {
                    log.warn("onMessage err", e);
                    removeCtx(conn);
                }
            }

            @Override
            public void onMessage(WebSocket conn, String message) {
                //只使用二进制消息，忽略文本消息
            }

            @Override
            public void onError(WebSocket conn, Exception ex) {
                log.warn("ws server error", ex);
                if (conn != null) {
                    removeCtx(conn);
                }
            }

            @Override
            public void onStart() {
                log.info("ws server started on port {}", ssConfig.port);
            }
        };
        server.setConnectionLostTimeout(0);
        server.start();
    }

    @Override
    protected void sendBytesToClient(WebSocket conn, byte[] bytes) {
        try {
            conn.send(bytes);
        } catch (Exception e) {
            log.warn("sendBytesToClient err", e);
            removeCtx(conn);
        }
    }

    @Override
    protected void closeCtx(WebSocket conn) {
        try {
            conn.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onExit() {
        try {
            if (null != server) {
                server.stop();
            }
        } catch (Exception e) {
            log.warn("server.stop() err", e);
        }
    }
}
