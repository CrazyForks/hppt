package org.wowtools.hppt.run.ss.rhppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.common.util.ReconnectBackoff;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rhppt协议服务端 - Socket + 虚拟线程实现
 * SS主动连接SC，使用长度前缀帧格式传输数据
 *
 * @author liuyu
 * @date 2024/4/15
 */
@Slf4j
public class RHpptServerSessionService extends ServerSessionService<Socket> {

    private volatile Socket socket;
    private volatile OutputStream out;
    private final ReentrantLock writeLock = new ReentrantLock();
    private int lengthFieldLength;

    public RHpptServerSessionService(SsConfig ssConfig) {
        super(ssConfig);
    }

    @Override
    protected void init(SsConfig ssConfig) throws Exception {
        lengthFieldLength = ssConfig.rhppt.lengthFieldLength;
        ReconnectBackoff backoff = new ReconnectBackoff(ssConfig.transportReconnectBaseDelayMillis,
                ssConfig.transportReconnectMaxDelayMillis, ssConfig.transportReconnectJitterMillis);
        IoThreadUtil.startIoThread(() -> {
            while (running) {
                Socket currentSocket = null;
                String disconnectReason = "rhppt connection closed";
                try {
                    currentSocket = new Socket(ssConfig.rhppt.host, ssConfig.rhppt.port);
                    currentSocket.setTcpNoDelay(true);
                    socket = currentSocket;
                    out = currentSocket.getOutputStream();
                    markReady("rhppt transport");
                    backoff.reset();
                    InputStream in = currentSocket.getInputStream();
                    while (running && socket == currentSocket) {
                        byte[] frame = FrameIo.readFrame(in, lengthFieldLength);
                        if (frame == null) {
                            disconnectReason = "rhppt peer closed";
                            break;
                        }
                        receiveClientBytes(currentSocket, frame);
                    }
                } catch (Exception e) {
                    disconnectReason = "rhppt readLoop err:" + e.getMessage();
                    if (running) {
                        log.warn("rhppt readLoop err", e);
                    }
                } finally {
                    if (currentSocket != null) {
                        removeCtx(currentSocket);
                        clearSocket(currentSocket);
                    }
                }
                if (!running) {
                    return;
                }
                long delayMillis = backoff.nextDelayMillis();
                transportDisconnected(disconnectReason);
                recordTransportReconnect("rhppt", disconnectReason, backoff.getConsecutiveFailures(), delayMillis);
                sleepForReconnect(delayMillis);
            }
        }, "hppt-io-server-connect");
    }

    @Override
    protected void sendBytesToClient(Socket socket, byte[] bytes) {
        try {
            writeLock.lock();
            try {
                FrameIo.writeFrame(out, bytes, lengthFieldLength);
            } finally {
                writeLock.unlock();
            }
        } catch (Exception e) {
            log.warn("sendBytesToClient err", e);
            clearSocket(socket);
        }
    }

    @Override
    protected void closeCtx(Socket socket) throws Exception {
        socket.close();
    }

    @Override
    public void onExit() {
        try {
            if (null != socket) {
                socket.close();
            }
        } catch (Exception e) {
            log.warn("socket.close() err", e);
        }
    }

    private void clearSocket(Socket currentSocket) {
        if (currentSocket == null) {
            return;
        }
        try {
            currentSocket.close();
        } catch (Exception ignored) {
        }
        if (socket == currentSocket) {
            socket = null;
            out = null;
        }
    }
}
