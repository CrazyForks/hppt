package org.wowtools.hppt.common.client;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.pojo.SessionBytes;
import org.wowtools.hppt.common.util.DebugConfig;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClientSession管理器
 *
 * @author liuyu
 * @date 2023/11/18
 */
@Slf4j
public class ClientSessionManager implements AutoCloseable {
    private final Map<Integer, ClientSession> clientSessionMap = new ConcurrentHashMap<>();
    private final ClientSessionLifecycle lifecycle;
    private final ClientBytesSender clientBytesSender;
    private final int bufferSize;
    private final List<ServerSocket> serverSockets = new ArrayList<>();
    private volatile boolean running = true;

    ClientSessionManager(ClientSessionManagerBuilder builder) {
        lifecycle = builder.lifecycle;
        if (lifecycle == null) {
            throw new RuntimeException("lifecycle不能为空");
        }
        clientBytesSender = builder.clientBytesSender;
        if (clientBytesSender == null) {
            throw new RuntimeException("clientBytesSender不能为空");
        }
        bufferSize = builder.bufferSize;
    }

    /**
     * 绑定端口, 成功返回true
     *
     * @param localHost 本机绑定哪个ip，多网卡有冲突时填写，一般填null即可
     * @param port      端口
     * @return 绑定成功返回true
     */
    public boolean bindPort(String localHost, int port) {
        try {
            ServerSocket ss;
            if (localHost == null) {
                ss = new ServerSocket(port);
            } else {
                ss = new ServerSocket();
                ss.bind(new InetSocketAddress(localHost, port));
            }
            serverSockets.add(ss);
            Thread.startVirtualThread(() -> acceptLoop(ss, port));
            log.debug("bindPort {} success", port);
            return true;
        } catch (Exception e) {
            log.error("Failed to bind port {}.", port, e);
            return false;
        }
    }

    private void acceptLoop(ServerSocket ss, int port) {
        while (running) {
            try {
                Socket userSocket = ss.accept();
                userSocket.setTcpNoDelay(true);
                if (bufferSize > 0) {
                    userSocket.setSendBufferSize(bufferSize);
                    userSocket.setReceiveBufferSize(bufferSize);
                }
                // 通知 clientBytesSender 有新连接
                ClientBytesSender.SessionIdCallBack cb = new ClientBytesSender.SessionIdCallBack(userSocket) {
                    @Override
                    public void cb(int sessionId) {
                        try {
                            ClientSession clientSession = new ClientSession(sessionId, userSocket, lifecycle);
                            clientSessionMap.put(sessionId, clientSession);
                            lifecycle.created(clientSession);
                            // 启动读取线程
                            startReadThread(clientSession, port);
                        } catch (IOException e) {
                            log.warn("创建ClientSession异常", e);
                            try {
                                userSocket.close();
                            } catch (IOException ignored) {
                            }
                        }
                    }
                };
                clientBytesSender.connected(port, userSocket, cb);
            } catch (IOException e) {
                if (running) {
                    log.warn("accept异常", e);
                }
            }
        }
    }

    private void startReadThread(ClientSession clientSession, int port) {
        Thread.startVirtualThread(() -> {
            try {
                InputStream in = clientSession.getSocket().getInputStream();
                byte[] buf = new byte[bufferSize > 0 ? bufferSize : 10240];
                while (clientSession.isRunning()) {
                    int n = in.read(buf);
                    if (n == -1) {
                        break;
                    }
                    byte[] bytes = new byte[n];
                    System.arraycopy(buf, 0, bytes, 0, n);
                    if (log.isDebugEnabled()) {
                        log.debug("ClientSession {} 收到用户端字节 {}", clientSession.getSessionId(), bytes.length);
                    }
                    bytes = lifecycle.beforeSendToTarget(clientSession, bytes);
                    if (bytes == null) {
                        continue;
                    }
                    SessionBytes sessionBytes = new SessionBytes(clientSession.getSessionId(), bytes);
                    if (DebugConfig.OpenSerialNumber) {
                        log.debug("用户端发来字节 >sessionBytes-SerialNumber {}", sessionBytes.getSerialNumber());
                    }
                    clientBytesSender.sendToTarget(clientSession, sessionBytes);
                    lifecycle.afterSendToTarget(clientSession, bytes);
                }
            } catch (IOException e) {
                if (clientSession.isRunning()) {
                    log.debug("读取用户Socket异常 sessionId={}", clientSession.getSessionId(), e);
                }
            } finally {
                disposeClientSession(clientSession, "用户连接关闭");
            }
        });
    }

    public void disposeClientSession(ClientSession clientSession, String type) {
        clientSession.close();
        log.info("ClientSession {} close,type [{}]", clientSession.getSessionId(), type);
        if (clientSessionMap.remove(clientSession.getSessionId()) != null) {
            lifecycle.closed(clientSession);
        }
    }

    public ClientSession getClientSessionBySessionId(int sessionId) {
        return clientSessionMap.get(sessionId);
    }

    public int getSessionNum() {
        return clientSessionMap.size();
    }

    @Override
    public void close() {
        running = false;
        for (ServerSocket ss : serverSockets) {
            try {
                ss.close();
            } catch (IOException e) {
                log.debug("关闭ServerSocket异常", e);
            }
        }
        for (ClientSession session : clientSessionMap.values()) {
            session.close();
        }
    }
}
