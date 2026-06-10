package org.wowtools.hppt.common.server;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.pojo.SendAbleSessionBytes;
import org.wowtools.hppt.common.pojo.SessionBytes;
import org.wowtools.hppt.common.util.DebugConfig;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.common.util.RoughTimeUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liuyu
 * @date 2023/11/17
 */
@Slf4j
public class ServerSession {

    private final LoginClientService.Client client;
    private final Socket socket;
    private final OutputStream out;
    private final int sessionId;
    private final long sessionTimeout;
    private final ServerSessionLifecycle lifecycle;
    private final ReentrantLock writeLock = new ReentrantLock();
    //上次活跃时间
    private volatile long activeTime;
    private volatile boolean running = true;
    private final AtomicLong bytesFromTarget = new AtomicLong();
    private final AtomicLong bytesToTarget = new AtomicLong();


    ServerSession(long sessionTimeout, int sessionId, LoginClientService.Client client,
                  ServerSessionLifecycle lifecycle, Socket socket) throws IOException {
        this.sessionId = sessionId;
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.sessionTimeout = sessionTimeout;
        this.lifecycle = lifecycle;
        this.client = client;
        activeSession();
        client.addSession(this);
        // 启动从目标端口读取数据的线程
        IoThreadUtil.startIoThread(this::readFromTarget, "hppt-io-target-" + sessionId);
    }

    private void readFromTarget() {
        try {
            InputStream in = socket.getInputStream();
            byte[] buf = new byte[65536];
            while (running) {
                int n = in.read(buf);
                if (n == -1) {
                    log.info("目标端口EOF sessionId={} bytesFromTarget={}", sessionId, bytesFromTarget.get());
                    break;
                }
                byte[] bytes;
                if (n == buf.length) {
                    bytes = buf;
                    buf = new byte[65536];
                } else {
                    bytes = Arrays.copyOf(buf, n);
                }
                bytesFromTarget.addAndGet(n);
                activeSession();
                log.debug("serverSession {} 收到目标端口字节 {}", sessionId, bytes.length);

                SessionBytes sessionBytes = new SessionBytes(sessionId, bytes);
                if (DebugConfig.OpenSerialNumber) {
                    log.debug("目标端发来字节 <sessionBytes-SerialNumber {}", sessionBytes.getSerialNumber());
                }
                lifecycle.sendToClientBuffer(sessionBytes, client, new SendAbleSessionBytes.CallBack() {
                    @Override
                    public void cb(boolean success) {
                        // 直接同步回调，不再阻塞等待
                    }
                });
                lifecycle.afterSendToTarget(this, bytes);
            }
        } catch (IOException e) {
            if (running) {
                log.warn("读取目标端口异常 sessionId={}", sessionId, e);
            }
        } finally {
            close();
        }
    }

    /**
     * 向目标端口发送字节（直接写入，无中间队列）
     */
    public void sendToTarget(SessionBytes sessionBytes) {
        sendToTarget(sessionBytes.getBytes());
    }

    /**
     * 向目标端口发送字节（直接写入，无中间队列）
     *
     * @param bytes bytes
     */
    public void sendToTarget(byte[] bytes) {
        writeLock.lock();
        try {
            activeSession();
            bytes = lifecycle.beforeSendToTarget(this, bytes);
            if (bytes != null) {
                try {
                    out.write(bytes);
                    out.flush();
                    bytesToTarget.addAndGet(bytes.length);
                    log.debug("向目标端口发送字节 {}", bytes.length);
                    lifecycle.afterSendToTarget(this, bytes);
                } catch (IOException e) {
                    log.warn("向目标端口发送字节异常", e);
                    close();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 保持会话活跃
     */
    public void activeSession() {
        activeTime = RoughTimeUtil.getTimestamp();
    }

    //是否需要向用户侧确认session是否存活
    public boolean isNeedCheckActive() {
        return activeTime + sessionTimeout <= RoughTimeUtil.getTimestamp();
    }

    //session是否超时
    public boolean isTimeOut() {
        return activeTime + (sessionTimeout * 2) <= RoughTimeUtil.getTimestamp();
    }

    public int getSessionId() {
        return sessionId;
    }

    void close() {
        if (!running) {
            return;
        }
        running = false;
        log.info("ServerSession关闭 sessionId={} bytesFromTarget={} bytesToTarget={}",
                sessionId, bytesFromTarget.get(), bytesToTarget.get());
        try {
            socket.close();
        } catch (IOException ignored) {
        }
        client.removeSession(this);
    }

    @Override
    public String toString() {
        return "("
                + client.clientId + " "
                + sessionId + (isTimeOut() ? " timeout)" : " active)");
    }

    public LoginClientService.Client getClient() {
        return client;
    }

    public long getActiveTime() {
        return activeTime;
    }

    public boolean isRunning() {
        return running;
    }

    public long getBytesFromTarget() {
        return bytesFromTarget.get();
    }

    public long getBytesToTarget() {
        return bytesToTarget.get();
    }

    public String getTargetAddress() {
        SocketAddress address = socket.getRemoteSocketAddress();
        return address == null ? "" : address.toString();
    }
}
