package org.wowtools.hppt.run.sc.rhppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * rhppt协议客户端 - Socket + 虚拟线程实现
 * SC监听端口，等待SS主动连接进来，使用长度前缀帧格式传输数据
 *
 * @author liuyu
 * @date 2024/4/9
 */
@Slf4j
public class RHpptClientSessionService extends ClientSessionService {

    private ServerSocket serverSocket;
    private volatile Socket socket;
    private volatile OutputStream out;
    private final ReentrantLock writeLock = new ReentrantLock();
    private int lengthFieldLength;

    public RHpptClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) throws Exception {
        lengthFieldLength = config.rhppt.lengthFieldLength;
        serverSocket = new ServerSocket(config.rhppt.port);
        IoThreadUtil.startIoThread(() -> {
            while (running) {
                Socket currentSocket = null;
                String disconnectReason = "rhppt connection closed";
                try {
                    currentSocket = serverSocket.accept();
                    currentSocket.setTcpNoDelay(true);
                    socket = currentSocket;
                    out = currentSocket.getOutputStream();
                    markTransportReady("rhppt");
                    cb.end(null);
                    InputStream in = currentSocket.getInputStream();
                    while (running && socket == currentSocket) {
                        byte[] frame = FrameIo.readFrame(in, lengthFieldLength);
                        if (frame == null) {
                            disconnectReason = "rhppt peer closed";
                            break;
                        }
                        receiveServerBytes(frame);
                    }
                } catch (Exception e) {
                    disconnectReason = "rhppt client err:" + e.getMessage();
                    if (running) {
                        log.warn("rhppt client connection err", e);
                    }
                } finally {
                    clearSocket(currentSocket);
                }
                if (!running) {
                    return;
                }
                transportDisconnected(disconnectReason);
                recordTransportReconnect("rhppt", disconnectReason, 1, 0);
            }
        }, "hppt-io-client-accept");
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
        if (null != serverSocket) {
            serverSocket.close();
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
