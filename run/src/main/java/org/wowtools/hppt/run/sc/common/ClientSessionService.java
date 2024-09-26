package org.wowtools.hppt.run.sc.common;

import io.netty.util.internal.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.client.ClientSession;
import org.wowtools.hppt.common.client.ClientSessionLifecycle;
import org.wowtools.hppt.common.pojo.SessionBytes;
import org.wowtools.hppt.common.util.Constant;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author liuyu
 * @date 2024/3/12
 */
@Slf4j
public abstract class ClientSessionService {
    protected final ScConfig config;

    protected final BlockingQueue<String> sendCommandQueue = new LinkedBlockingQueue<>();
    protected final BlockingQueue<SessionBytes> sendBytesQueue = new LinkedBlockingQueue<>();


    protected final Receiver receiver;
    protected volatile boolean running = true;

    /**
     * 当一个事件结束时发起的回调
     */
    @FunctionalInterface
    protected interface Cb {
        void end();
    }

    public ClientSessionService(ScConfig config) throws Exception {
        this.config = config;
        if (!config.isRelay) {
            receiver = new PortReceiver(config, this);
        } else {
            throw new RuntimeException("中继模式暂未实现");
        }
    }


    /**
     * 与服务端建立连接
     *
     * @param config 配置文件
     * @param cb     请在连接完成后主动调用cb.end()
     */
    protected abstract void connectToServer(ScConfig config, Cb cb) throws Exception;

    /**
     * 发送字节到服务端的具体方法
     *
     * @param bytes bytes
     */
    protected abstract void sendBytesToServer(byte[] bytes);

    /**
     * 收到服务端传过来的字节时，主动调用此方法进行接收操作
     *
     * @param bytes
     * @throws Exception
     */
    protected void receiveServerBytes(byte[] bytes) throws Exception {
        receiver.receiveServerBytes(bytes);
    }

    /**
     * 当发生难以修复的异常等情况时，主动调用此方法结束当前服务，以便后续自动重启等操作
     */
    public void exit() {
        running = false;
        receiver.exit();
        try {
            doClose();
        } catch (Exception e) {
            log.warn("doClose error ", e);
        }
        synchronized (this) {
            this.notify();
        }
    }

    /**
     * 当服务关闭时，若有一些资源需要释放、关闭等，重写此方法
     */
    protected void doClose() throws Exception {

    }

    /**
     * 阻塞直到exit方法被调用
     */
    public void sync() {
        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("sync interrupted");
        }
    }


    protected ClientSessionLifecycle buildClientSessionLifecycle() {
        ClientSessionLifecycle common = new ClientSessionLifecycle() {
            @Override
            public void closed(ClientSession clientSession) {
                sendCommandQueue.add(String.valueOf(Constant.SsCommands.CloseSession) + clientSession.getSessionId());
            }
        };
        if (StringUtil.isNullOrEmpty(config.lifecycle)) {
            return common;
        } else {
            try {
                Class<? extends ClientSessionLifecycle> clazz = (Class<? extends ClientSessionLifecycle>) Class.forName(config.lifecycle);
                ClientSessionLifecycle custom = clazz.getDeclaredConstructor().newInstance();
                return new ClientSessionLifecycle() {
                    @Override
                    public void closed(ClientSession clientSession) {
                        common.closed(clientSession);
                        custom.closed(clientSession);
                    }
                };
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


    protected void newConnected() {

    }

    /**
     * 是否未被用户被使用
     * @return
     */
    public boolean notUsed() {
        return receiver.notUsed();
    }
}
