package org.wowtools.hppt.run.ss.hppt;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.FrameIo;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;

/**
 * hppt协议服务端 - Socket + 虚拟线程实现
 * SS监听端口，接受SC连接，使用长度前缀帧格式传输数据
 *
 * @author liuyu
 * @date 2024/4/9
 */
@Slf4j
public class HpptServerSessionService extends ServerSessionService<Socket> {
    private volatile ServerSocket serverSocket;
    private final ReentrantLock writeLock = new ReentrantLock();
    private int lengthFieldLength;

    public HpptServerSessionService(SsConfig ssConfig) throws Exception {
        super(ssConfig);
    }

    @Override
    protected void init(SsConfig ssConfig) throws Exception {
        lengthFieldLength = ssConfig.hppt.lengthFieldLength;
        serverSocket = new ServerSocket(ssConfig.port);
        Thread.startVirtualThread(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    clientSocket.setTcpNoDelay(true);
                    Thread.startVirtualThread(() -> readLoop(clientSocket));
                } catch (Exception e) {
                    if (running) {
                        log.warn("accept err", e);
                    }
                }
            }
        });
    }

    private void readLoop(Socket socket) {
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
                log.debug("readLoop err", e);
            }
        } finally {
            removeCtx(socket);
        }
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
            removeCtx(socket);
        }
    }

    @Override
    protected void closeCtx(Socket socket) throws Exception {
        socket.close();
    }

    @Override
    public void onExit() {
        try {
            if (null != serverSocket) {
                serverSocket.close();
            }
        } catch (Exception e) {
            log.warn("serverSocket.close() err", e);
        }
    }
}
