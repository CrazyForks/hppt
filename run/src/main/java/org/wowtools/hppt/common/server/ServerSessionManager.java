package org.wowtools.hppt.common.server;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.Constant;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author liuyu
 * @date 2023/11/18
 */
@Slf4j
public class ServerSessionManager implements AutoCloseable {

    private volatile boolean running = true;
    private final AtomicInteger sessionIdBuilder = new AtomicInteger();

    //<sessionId,session>
    private final Map<Integer, ServerSession> serverSessionMap = new ConcurrentHashMap<>();
    //<clientId,Map<sessionId,session>>
    private final Map<String, Map<Integer, ServerSession>> clientIdServerSessionMap = new ConcurrentHashMap<>();

    private final ServerSessionLifecycle lifecycle;
    private final long sessionTimeout;

    private long lastHeartbeatTime = System.currentTimeMillis();

    ServerSessionManager(ServerSessionManagerBuilder builder) {
        lifecycle = builder.lifecycle;
        sessionTimeout = builder.sessionTimeout;
        //定期检查超时session
        Thread.startVirtualThread(() -> {
            while (running) {
                try {
                    Thread.sleep(sessionTimeout);
                } catch (InterruptedException e) {
                    continue;
                }
                if (!running) {
                    return;
                }
                log.info("check session: serverSessionMap {}", serverSessionMap.size());
                HashSet<ServerSession> needClosedSessions = new HashSet<>();
                serverSessionMap.forEach((id, session) -> {
                    if (session.isTimeOut()) {
                        needClosedSessions.add(session);
                    } else if (session.isNeedCheckActive()) {
                        session.getClient().addCommand(String.valueOf(Constant.ScCommands.CheckSessionActive) + session.getSessionId());
                    }
                });
                for (ServerSession session : needClosedSessions) {
                    disposeServerSession(session, "超时关闭");
                }
            }
        });
    }

    @Override
    public void close() {
        running = false;
    }

    //新建一个session并返回sessionId
    public int createServerSession(LoginClientService.Client client, String host, int port, long timeoutMillis) {
        int sessionId = sessionIdBuilder.addAndGet(1);
        Map<Integer, ServerSession> clientSessions = clientIdServerSessionMap.computeIfAbsent(client.clientId, (id) -> new ConcurrentHashMap<>());

        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(host, port), (int) timeoutMillis);
            socket.setTcpNoDelay(true);
            log.info("new ServerSession {} {}:{} from {}", sessionId, host, port, client.clientId);
            ServerSession serverSession = new ServerSession(sessionTimeout, sessionId, client, lifecycle, socket);
            serverSessionMap.put(sessionId, serverSession);
            clientSessions.put(sessionId, serverSession);
            lifecycle.created(serverSession);
        } catch (Exception e) {
            log.warn("连接目标端口异常 sessionId {} {}:{}", sessionId, host, port, e);
        }
        return sessionId;
    }

    public void disposeServerSession(ServerSession serverSession, String type) {
        try {
            serverSession.close();
        } catch (Exception e) {
            log.warn("close session error", e);
        }
        log.info("serverSession {} close,type [{}]", serverSession.getSessionId(), type);
        if (null != serverSessionMap.remove(serverSession.getSessionId())) {
            serverSession.getClient().addCommand(String.valueOf(Constant.ScCommands.CloseSession) + serverSession.getSessionId());
            lifecycle.closed(serverSession);
        }
        Map<Integer, ServerSession> clientSessions = clientIdServerSessionMap.get(serverSession.getClient().clientId);
        if (clientSessions != null) {
            clientSessions.remove(serverSession.getSessionId());
        }
    }

    public Map<Integer, ServerSession> getServerSessionMapByClientId(String clientId) {
        return clientIdServerSessionMap.get(clientId);
    }

    public ServerSession getServerSessionBySessionId(int sessionId) {
        return serverSessionMap.get(sessionId);
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    public void setLastHeartbeatTime(long lastHeartbeatTime) {
        this.lastHeartbeatTime = lastHeartbeatTime;
    }
}
