package org.wowtools.hppt.run.sc.hppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.common.util.ReconnectBackoff;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * hppt协议客户端 - Socket + 虚拟线程实现
 * SC主动连接SS，使用长度前缀帧格式传输数据
 *
 * @author liuyu
 * @date 2024/3/12
 */
@Slf4j
public class HpptClientSessionService extends ClientSessionService {

    private volatile Socket socket;
    private volatile OutputStream out;
    private final ReentrantLock writeLock = new ReentrantLock();
    private int lengthFieldLength;

    public HpptClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) {
        lengthFieldLength = config.hppt.lengthFieldLength;
        ReconnectBackoff backoff = new ReconnectBackoff(config.transportReconnectBaseDelayMillis,
                config.transportReconnectMaxDelayMillis, config.transportReconnectJitterMillis);
        IoThreadUtil.startIoThread(() -> {
            while (running) {
                Socket currentSocket = null;
                String disconnectReason = "hppt connection closed";
                try {
                    currentSocket = new Socket(config.hppt.host, config.hppt.port);
                    currentSocket.setTcpNoDelay(true);
                    socket = currentSocket;
                    out = currentSocket.getOutputStream();
                    markTransportReady("hppt");
                    backoff.reset();
                    cb.end(null);
                    InputStream in = currentSocket.getInputStream();
                    while (running && socket == currentSocket) {
                        byte[] frame = FrameIo.readFrame(in, lengthFieldLength);
                        if (frame == null) {
                            disconnectReason = "hppt peer closed";
                            break;
                        }
                        receiveServerBytes(frame);
                    }
                } catch (Exception e) {
                    disconnectReason = "hppt client err:" + e.getMessage();
                    if (running) {
                        log.warn("hppt client connection err", e);
                    }
                } finally {
                    clearSocket(currentSocket);
                }
                if (!running) {
                    return;
                }
                long delayMillis = backoff.nextDelayMillis();
                transportDisconnected(disconnectReason);
                recordTransportReconnect("hppt", disconnectReason, backoff.getConsecutiveFailures(), delayMillis);
                sleepForReconnect(delayMillis);
            }
        }, "hppt-io-client-read");
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        try {
            writeLock.lock();
            try {
                FrameIo.writeFrame(out, bytes, lengthFieldLength);
            } finally {
                writeLock.unlock();
            }
        } catch (Exception e) {
            log.warn("sendBytesToServer err", e);
            clearSocket(socket);
        }
    }

    @Override
    protected void doClose() throws Exception {
        if (null != socket) {
            socket.close();
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
