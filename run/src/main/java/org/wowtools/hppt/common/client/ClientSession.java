package org.wowtools.hppt.common.client;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 客户端会话
 *
 * @author liuyu
 * @date 2023/11/17
 */
@Slf4j
public class ClientSession {
    private final int sessionId;
    private final Socket socket;
    private final OutputStream out;
    private final ClientSessionLifecycle lifecycle;
    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile boolean running = true;

    ClientSession(int sessionId, Socket socket, ClientSessionLifecycle lifecycle) throws IOException {
        this.sessionId = sessionId;
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.lifecycle = lifecycle;
    }

    /**
     * 直接写入用户Socket
     *
     * @param bytes bytes
     */
    public void sendToUser(byte[] bytes) {
        writeLock.lock();
        try {
            bytes = lifecycle.beforeSendToUser(this, bytes);
            if (bytes != null) {
                try {
                    out.write(bytes);
                    out.flush();
                    lifecycle.afterSendToUser(this, bytes);
                } catch (IOException e) {
                    log.warn("向用户发送字节异常 sessionId={}", sessionId, e);
                    close();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    public int getSessionId() {
        return sessionId;
    }

    public Socket getSocket() {
        return socket;
    }

    void close() {
        if (!running) {
            return;
        }
        running = false;
        try {
            socket.close();
        } catch (IOException e) {
            log.debug("关闭socket异常", e);
        }
    }

    public boolean isRunning() {
        return running;
    }
}
