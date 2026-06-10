package org.wowtools.hppt.run.sc.common;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.client.ClientSession;
import org.wowtools.hppt.common.client.ClientSessionLifecycle;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author liuyu
 * @date 2024/3/12
 */
@Slf4j
public abstract class ClientSessionService implements AutoCloseable {
    protected final ScConfig config;


    public final Receiver receiver;
    protected volatile boolean running = true;
    private volatile boolean exited = false;
    private volatile String lastExitReason = "running";
    private final AtomicLong lastReadyTime = new AtomicLong(-1L);
    private final AtomicLong transportReconnectCount = new AtomicLong();
    private final ReentrantLock syncLock = new ReentrantLock();
    private final Condition syncCondition = syncLock.newCondition();

    private final BlockingQueue<byte[]> receiveServerBytesQueue;

    /**
     * 当一个事件结束时发起的回调
     */
    @FunctionalInterface
    public interface Cb {
        /**
         * 通知框架事件结束
         *
         * @param e 如遇到异常传入异常，否则传入null
         */
        void end(Throwable e);
    }

    public ClientSessionService(ScConfig config) throws Exception {
        this.config = config;
        receiveServerBytesQueue = new ArrayBlockingQueue<>(config.receiveQueueSize);
        receiver = new PortReceiver(config, this);
        log.info("--- 普通模式 receiveQueueSize={}", config.receiveQueueSize);
        IoThreadUtil.startIoThread(() -> {
            while (running) {
                byte[] bytes;
                try {
                    bytes = receiveServerBytesQueue.poll(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    bytes = null;
                }
                if (null == bytes) {
                    continue;
                }
                try {
                    receiver.receiveServerBytes(bytes);
                } catch (Exception e) {
                    log.warn("receiver.receiveServerBytes err", e);
                    exit("receiver.receiveServerBytes err:" + e.getMessage());
                }
            }

        }, "hppt-io-sc-recv");
    }


    /**
     * 与服务端建立连接
     *
     * @param config 配置文件
     * @param cb     请在连接完成后主动调用cb.end(null)，或在发生异常后主动调用cb.end(exception)
     */
    public abstract void connectToServer(ScConfig config, Cb cb) throws Exception;

    /**
     * 发送字节到服务端的具体方法
     *
     * @param bytes bytes
     */
    public abstract void sendBytesToServer(byte[] bytes);

    /**
     * 收到服务端传过来的字节时，主动调用此方法进行接收操作
     *
     * @param bytes
     * @throws Exception
     */
    public void receiveServerBytes(byte[] bytes) throws Exception {
        int queueSize = receiveServerBytesQueue.size();
        int capacity = config.receiveQueueSize;
        if (queueSize > capacity / 2 && queueSize % 50 == 0) {
            log.warn("receiveServerBytesQueue 高水位 {}/{} bytes={}", queueSize, capacity, bytes.length);
        }
        if (!receiveServerBytesQueue.offer(bytes, 30, TimeUnit.SECONDS)) {
            log.warn("缓冲区堆积过多数据 {}/{}, 强制退出", queueSize, capacity);
            exit("receiveServerBytesQueue overflow");
        }
    }

    /**
     * 当发生难以修复的异常等情况时，主动调用此方法结束当前服务，以便后续自动重启等操作
     */
    public void exit() {
        exit("manual exit");
    }

    public void exit(String reason) {
        if (exited) {
            return;
        }
        exited = true;
        lastExitReason = reason;
        log.warn("ClientSessionService exit reason=[{}] service={} lastReadyAt={}", reason, this,
                formatTs(lastReadyTime.get()));
        running = false;
        Receiver currentReceiver = receiver;
        if (currentReceiver != null) {
            currentReceiver.exit();
        }
        try {
            doClose();
        } catch (Exception e) {
            log.warn("doClose error ", e);
        }
        syncLock.lock();
        try {
            syncCondition.signalAll();
        } finally {
            syncLock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        exit("close()");
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
        syncLock.lock();
        try {
            syncCondition.await();
        } catch (InterruptedException e) {
            log.info("sync interrupted");
        } finally {
            syncLock.unlock();
        }
    }


    protected ClientSessionLifecycle buildClientSessionLifecycle() {
        ClientSessionLifecycle common = new ClientSessionLifecycle() {
            @Override
            public void closed(ClientSession clientSession) {
                receiver.closeClientSession(clientSession);
            }
        };
        if (config.lifecycle == null || config.lifecycle.isEmpty()) {
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

    protected void markTransportReady(String type) {
        long now = System.currentTimeMillis();
        lastReadyTime.set(now);
        log.info("传输层已就绪 type={} readyAt={}", type, formatTs(now));
    }

    protected void transportDisconnected(String reason) {
        onTransportDisconnected(reason);
        receiver.transportDisconnected(reason);
    }

    protected void recordTransportReconnect(String type, String reason, int attempt, long delayMillis) {
        transportReconnectCount.incrementAndGet();
        log.warn("传输层断开 type={} attempt={} nextDelay={}ms reason={}",
                type, attempt, delayMillis, reason);
    }

    protected void onTransportDisconnected(String reason) {

    }

    protected void sleepForReconnect(long delayMillis) {
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException ignored) {
        }
    }

    protected boolean handleHeartbeatTimeout() {
        return false;
    }

    public String getLastExitReason() {
        return lastExitReason;
    }

    public long getLastReadyTime() {
        return lastReadyTime.get();
    }

    public long getTransportReconnectCount() {
        return transportReconnectCount.get();
    }

    private static String formatTs(long ts) {
        if (ts <= 0) {
            return "never";
        }
        return new Date(ts).toString();
    }

    /**
     * 是否未被用户被使用
     *
     * @return
     */
    public boolean notUsed() {
        return receiver.notUsed();
    }
}
