package org.wowtools.hppt.run.sc.rhppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
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
        Thread.startVirtualThread(() -> {
            try {
                socket = serverSocket.accept();
                socket.setTcpNoDelay(true);
                out = socket.getOutputStream();
                cb.end(null);
                // 读循环
                InputStream in = socket.getInputStream();
                while (running) {
                    byte[] frame = FrameIo.readFrame(in, lengthFieldLength);
                    if (frame == null) {
                        break;
                    }
                    receiveServerBytes(frame);
                }
            } catch (Exception e) {
                log.warn("rhppt client connection err", e);
            } finally {
                exit();
            }
        });
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
            exit();
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
}
