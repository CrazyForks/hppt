package org.wowtools.hppt.run.sc.hppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
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
        Thread.startVirtualThread(() -> {
            try {
                socket = new Socket(config.hppt.host, config.hppt.port);
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
                log.warn("hppt client connection err", e);
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
    }
}
