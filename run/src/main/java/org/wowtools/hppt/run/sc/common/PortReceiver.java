package org.wowtools.hppt.run.sc.common;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.client.ClientBytesSender;
import org.wowtools.hppt.common.client.ClientSession;
import org.wowtools.hppt.common.client.ClientSessionManager;
import org.wowtools.hppt.common.client.ClientTalker;
import org.wowtools.hppt.common.pojo.SessionBytes;
import org.wowtools.hppt.common.util.*;
import org.wowtools.hppt.run.sc.pojo.ScConfig;
import org.wowtools.hppt.run.sc.util.ScUtil;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liuyu
 * @date 2024/9/27
 */
@Slf4j
final class PortReceiver implements Receiver {
    private final ScConfig config;
    private final ClientSessionManager clientSessionManager;
    private final ClientSessionService clientSessionService;


    private final BufferPool<String> sendCommandQueue = new BufferPool<>(">PortReceiver-sendCommand");
    private final BufferPool<SessionBytes> sendBytesQueue = new BufferPool<>(">PortReceiver-sendBytesQueue");

    private final Map<Integer, ClientBytesSender.SessionIdCallBack> sessionIdCallBackMap = new ConcurrentHashMap<>();//<newSessionFlag,cb>
    private volatile AesCipherUtil aesCipherUtil;
    private final ReentrantLock dtLock = new ReentrantLock();

    private volatile long dt = 0;

    private boolean firstLoginErr = true;
    private volatile boolean noLogin = true;
    private final AtomicInteger transportGeneration = new AtomicInteger();

    private volatile boolean running = true;
    //服务端回复心跳包的时间
    private volatile long serverHeartbeatTime = System.currentTimeMillis();
    private final long heartbeatTimeoutMillis;

    private final ClientTalker.CommandCallBack commandCallBack = (type, param) -> {
        if (Constant.ScCommands.Heartbeat == type) {
            log.info("收到服务端心跳包");
            serverHeartbeatTime = System.currentTimeMillis();
        }
    };


    public PortReceiver(ScConfig config, ClientSessionService clientSessionService) throws Exception {
        this.config = config;
        this.clientSessionService = clientSessionService;
        heartbeatTimeoutMillis = config.heartbeatPeriod <= 0 ? Long.MAX_VALUE : config.heartbeatPeriod * 3;
        clientSessionManager = ScUtil.createClientSessionManager(config,
                clientSessionService.buildClientSessionLifecycle(), buildClientBytesSender());
        buildSendThread().start();
        checkSessionInit();

        //心跳检测
        if (config.heartbeatPeriod > 0) {
            Thread.startVirtualThread(() -> {
                try {
                    Thread.sleep(config.heartbeatPeriod / 3);
                } catch (InterruptedException ignored) {
                }
                while (running) {
                    if (noLogin) {
                        try {
                            Thread.sleep(config.heartbeatPeriod);
                        } catch (InterruptedException ignored) {
                        }
                        continue;
                    }
                    //发送心跳包
                    sendCommandQueue.add(Constant.SsCommands.Heartbeat + ":" + System.currentTimeMillis());
                    //检测服务端心跳回复
                    if (System.currentTimeMillis() - serverHeartbeatTime > heartbeatTimeoutMillis) {
                        log.warn("长期未收到服务端心跳，疑似故障，重启");
                        if (clientSessionService.handleHeartbeatTimeout()) {
                            serverHeartbeatTime = System.currentTimeMillis();
                            continue;
                        }
                        clientSessionService.exit("heartbeat timeout");
                        break;
                    }
                    log.info("心跳检测正常");
                    try {
                        Thread.sleep(config.heartbeatPeriod);
                    } catch (InterruptedException ignored) {
                    }
                }
            });

        }

        clientSessionService.connectToServer(config, (exceptionCb) -> {
            if (null != exceptionCb) {
                log.warn("建立连接异常", exceptionCb);
                clientSessionService.exit("connect err:" + exceptionCb.getMessage());
                return;
            }
            int generation = transportGeneration.incrementAndGet();
            log.info("连接建立完成 generation={}", generation);
            startHandshake(generation);
        });
    }

    @Override
    public void receiveServerBytes(byte[] bytes) throws Exception {
        if (noLogin) {
            try {
                bytes = GridAesCipherUtil.decrypt(bytes);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("无效的字节，舍弃, bytes {}", new String(bytes, StandardCharsets.UTF_8), e);
                }
                return;
            }
            String s = new String(bytes, StandardCharsets.UTF_8);
            String[] cmd = s.split(" ", 2);
            switch (cmd[0]) {
                case "dt":
                    dtLock.lock();
                    try {
                        if (dt == 0) {
                            long localTs = System.currentTimeMillis();
                            long serverTs = Long.parseLong(cmd[1]);
                            dt = serverTs - localTs;
                            log.info("dt {} ms", dt);
                        }
                    } finally {
                        dtLock.unlock();
                    }
                    break;
                case "login":
                    String code = cmd[1];
                    if ("0".equals(code)) {
                        noLogin = false;
                        serverHeartbeatTime = System.currentTimeMillis();
                        log.info("登录成功");
                    } else if (firstLoginErr) {
                        firstLoginErr = false;
                        log.warn("第一次登录失败 {} ，重试", code);
                        Thread.sleep(10000);
                        sendLoginCommand();
                    } else {
                        log.error("登录失败 {}", code);
                        clientSessionService.exit("login failed:" + code);
                    }
                    break;
                default:
                    log.warn("未知命令 {}", s);
            }
        } else {
            ClientTalker.receiveServerBytes(config, bytes, clientSessionManager, aesCipherUtil, sendCommandQueue, sessionIdCallBackMap, commandCallBack);
        }
    }

    @Override
    public void closeClientSession(ClientSession clientSession) {
        if (!noLogin) {
            sendCommandQueue.add(String.valueOf(Constant.SsCommands.CloseSession) + clientSession.getSessionId());
        }
    }

    @Override
    public void exit() {
        running = false;
        clientSessionManager.close();
    }

    @Override
    public void transportDisconnected(String reason) {
        int generation = transportGeneration.incrementAndGet();
        log.warn("传输连接断开，重置客户端状态 generation={} reason={}", generation, reason);
        resetTransportState(reason);
    }

    @Override
    public boolean notUsed() {
        return !noLogin && clientSessionManager.getSessionNum() == 0;
    }

    private void sendLoginCommand() {
        aesCipherUtil = new AesCipherUtil(config.clientPassword, System.currentTimeMillis() + dt);
        String loginCode = BytesUtil.bytes2base64(aesCipherUtil.encryptor.encrypt(config.clientPassword.getBytes(StandardCharsets.UTF_8)));
        clientSessionService.sendBytesToServer(GridAesCipherUtil.encrypt(("login " + config.clientUser + " " + loginCode).getBytes(StandardCharsets.UTF_8)));
    }

    private void sendDtCommand() {
        clientSessionService.sendBytesToServer(GridAesCipherUtil.encrypt("dt".getBytes(StandardCharsets.UTF_8)));
    }

    private void startHandshake(int generation) {
        Thread.startVirtualThread(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignored) {
            }
            if (!running || generation != transportGeneration.get()) {
                return;
            }
            sendDtCommand();
            int n = 0;
            while (dt == 0 && clientSessionService.running && generation == transportGeneration.get()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
                n++;
                if (n > 100) {
                    sendDtCommand();
                    n = 0;
                }
            }
            if (!running || generation != transportGeneration.get() || dt == 0) {
                return;
            }
            sendLoginCommand();
        });
    }

    private void resetTransportState(String reason) {
        serverHeartbeatTime = System.currentTimeMillis();
        dt = 0;
        aesCipherUtil = null;
        noLogin = true;
        firstLoginErr = true;
        sendCommandQueue.clear();
        sendBytesQueue.clear();
        sessionIdCallBackMap.forEach((key, value) -> {
            try {
                value.socket.close();
            } catch (Exception ignored) {
            }
        });
        sessionIdCallBackMap.clear();
        clientSessionManager.disposeAllSessions("传输连接重置:" + reason);
        sendCommandQueue.clear();
        sendBytesQueue.clear();
        ClientTalker.clearPendingSessionBytes();
    }

    //起一个线程定时检测是否有SessionIdCallBack长期未得到响应，若是则说明连接故障，重启ClientSessionService
    private void checkSessionInit() {
        Thread.startVirtualThread(() -> {
            while (running) {
                try {
                    Thread.sleep(30000);

                    List<Map.Entry<Integer, ClientBytesSender.SessionIdCallBack>> timeoutEntries = new LinkedList<>();
                    for (Map.Entry<Integer, ClientBytesSender.SessionIdCallBack> entry : sessionIdCallBackMap.entrySet()) {
                        if (RoughTimeUtil.getTimestamp() - entry.getValue().createTime > 30_000) {
                            timeoutEntries.add(entry);
                        }
                    }
                    for (Map.Entry<Integer, ClientBytesSender.SessionIdCallBack> entry : timeoutEntries) {
                        log.warn("session长期未连接成功，疑似连接故障，主动关闭");
                        sessionIdCallBackMap.remove(entry.getKey());
                        try {
                            entry.getValue().socket.close();
                        } catch (Exception ignored) {
                        }
                    }
                } catch (Exception e) {
                    log.warn("checkSessionInit", e);
                }

            }
        });
    }

    private Thread buildSendThread() {
        return new Thread(() -> {
            while (running) {
                try {
                    if (noLogin) {
                        // 登录完成前无法加密发送，等待
                        Thread.sleep(100);
                        continue;
                    }
                    byte[] sendBytes = ClientTalker.buildSendToServerBytes(config, config.maxSendBodySize, sendCommandQueue, sendBytesQueue, aesCipherUtil, true);
                    if (null != sendBytes) {
                        log.debug("sendBytesToServer {}", sendBytes.length);
                        clientSessionService.sendBytesToServer(sendBytes);
                    }
                } catch (Exception e) {
                    log.warn("发送消息异常", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            }

        });
    }

    private ClientBytesSender buildClientBytesSender() {
        return new ClientBytesSender() {
            private final AtomicInteger newSessionFlagIdx = new AtomicInteger();

            @Override
            public void connected(int port, Socket socket, SessionIdCallBack cb) {
                for (ScConfig.Forward forward : config.forwards) {
                    if (forward.localPort == port) {
                        int newSessionFlag = newSessionFlagIdx.addAndGet(1);
                        String cmd = Constant.SsCommands.CreateSession + forward.remoteHost + Constant.sessionIdJoinFlag + forward.remotePort + Constant.sessionIdJoinFlag + newSessionFlag;
                        sendCommandQueue.add(cmd);
                        log.debug("connected command: {}", cmd);
                        sessionIdCallBackMap.put(newSessionFlag, cb);
                        log.info("建立连接 {}: {}->{}:{}", socket.getPort(), forward.localPort, forward.remoteHost, forward.remotePort);
                        try {
                            clientSessionService.newConnected();
                        } catch (Exception e) {
                            log.warn("newConnected Exception", e);
                        }
                        return;
                    }
                }
                throw new RuntimeException("未知 localPort " + port);
            }

            @Override
            public void sendToTarget(ClientSession clientSession, SessionBytes sessionBytes) {
                sendBytesQueue.add(sessionBytes);
            }
        };
    }
}
