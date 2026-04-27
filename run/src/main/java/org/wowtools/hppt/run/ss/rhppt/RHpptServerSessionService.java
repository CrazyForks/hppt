package org.wowtools.hppt.run.ss.rhppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
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
    private final ReentrantLock writeLock = new ReentrantLock();
    private int lengthFieldLength;

    public RHpptServerSessionService(SsConfig ssConfig) {
        super(ssConfig);
    }

    @Override
    protected void init(SsConfig ssConfig) throws Exception {
        lengthFieldLength = ssConfig.rhppt.lengthFieldLength;
        socket = new Socket(ssConfig.rhppt.host, ssConfig.rhppt.port);
        socket.setTcpNoDelay(true);
        Thread.startVirtualThread(() -> {
            try {
                InputStream in = socket.getInputStream();
                while (running) {
                    byte[] frame = FrameIo.readFrame(in, lengthFieldLength);
                    if (frame == null) {
                        break;
                    }
                    receiveClientBytes(socket, frame);
                }
            } catch (Exception e) {
                if (running) {
                    log.warn("rhppt readLoop err", e);
                }
            } finally {
                exit("rhppt connection closed");
            }
        });
    }

    @Override
    protected void sendBytesToClient(Socket socket, byte[] bytes) {
        try {
            OutputStream out = socket.getOutputStream();
            writeLock.lock();
            try {
                FrameIo.writeFrame(out, bytes, lengthFieldLength);
            } finally {
                writeLock.unlock();
            }
        } catch (Exception e) {
            log.warn("sendBytesToClient err", e);
            exit("sendBytesToClient err:" + e.getMessage());
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
}
