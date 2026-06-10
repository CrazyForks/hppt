package org.wowtools.hppt.run.ss.common;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ServerSessionService抽象类
 * 注意，编写实现类时，不要在构造方法里做会阻塞的事情(比如起一个端口)，丢到init方法里做
 *
 * @param <CTX> 实际和客户端连接的上下文，如ChannelHandlerContext等
 */
@Slf4j
public abstract class ServerSessionService<CTX> {

    protected final SsConfig ssConfig;

    private final Receiver<CTX> receiver;


    protected volatile boolean running = true;
    private volatile boolean exited = false;
    private volatile String lastExitReason = "running";
    private volatile Thread syncThread;
    private final AtomicLong lastReadyTime = new AtomicLong(-1L);
    private final AtomicLong transportReconnectCount = new AtomicLong();

    /**
     * @param ssConfig 配置信息
     */
    public ServerSessionService(SsConfig ssConfig) {
        this.ssConfig = ssConfig;
        receiver = new PortReceiver<>(ssConfig, this);
        log.info("--- 普通模式");
    }

    /**
     * 启动服务 同步阻塞直到发生异常退出
     *
     * @param ssConfig 配置信息
     */
    public void syncStart(SsConfig ssConfig) {
        log.info("syncStart {}", this);
        syncThread = Thread.currentThread();
        try {
            try {
                init(ssConfig);
                markReady("service init");
            } catch (Exception e) {
                String reason = "init err:" + e.getMessage();
                log.error("初始化失败，服务不可用 {}", this, e);
                exit(reason);
                throw new IllegalStateException(reason, e);
            }
            //起一个线程，定期检查服务心跳
            if (ssConfig.heartbeatTimeout > 0) {
                Thread.startVirtualThread(() -> {
                    try {
                        Thread.sleep(ssConfig.heartbeatTimeout);
                    } catch (InterruptedException ignored) {
                    }
                    while (running) {
                        if (!receiver.hasActiveClient()) {
                            log.info("当前无活跃客户端，跳过心跳超时检查");
                            try {
                                Thread.sleep(ssConfig.heartbeatTimeout);
                            } catch (InterruptedException ignored) {
                            }
                            continue;
                        }
                        long lt = receiver.getLastHeartbeatTime();
                        if (lt < 0 || System.currentTimeMillis() - lt < ssConfig.heartbeatTimeout * 1.5) {
                            log.info("心跳检测正常");
                            try {
                                Thread.sleep(ssConfig.heartbeatTimeout);
                            } catch (InterruptedException ignored) {
                            }
                        } else {
                            log.warn("服务端心跳监测超时，执行重启");
                            exit("服务端心跳监测失败");
                            return;
                        }
                    }
                });
            }
            log.info("-------syncStart end {}", this);
            while (running) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    if (running) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    break;
                }
            }
            exit("running stop");
        } finally {
            if (syncThread == Thread.currentThread()) {
                syncThread = null;
            }
        }
    }

    /**
     * 初始化操作，允许阻塞/挂起方法
     *
     * @param ssConfig 配置信息
     */
    protected abstract void init(SsConfig ssConfig) throws Exception;

    /**
     * 发送字节到客户端的具体方法
     *
     * @param ctx   实际和客户端连接的上下文
     * @param bytes bytes
     */
    protected abstract void sendBytesToClient(CTX ctx, byte[] bytes);

    /**
     * 收到客户端传过来的字节时，主动调用此方法进行接收操作。
     * 此方法内部只是一个快速放入缓冲池的过程，所以速度很快，外部建议无需另起多线程来调此方法，不然外部的先后顺序不好控制
     *
     * @param ctx   实际和客户端连接的上下文
     * @param bytes bytes
     */
    public void receiveClientBytes(CTX ctx, byte[] bytes) {
        if (null == bytes || bytes.length == 0) {
            return;
        }
        log.debug("收到客户端字节数 {} , {}", bytes.length, ctx);
        try {
            receiver.receiveClientBytes(ctx, bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 关闭上下文
     *
     * @param ctx ctx
     */
    protected abstract void closeCtx(CTX ctx) throws Exception;

    /**
     * 退出当前服务时需要做的事情
     */
    protected abstract void onExit() throws Exception;

    /**
     * 当发生难以修复的异常等情况时，主动调用此方法结束当前服务，以便后续自动重启等操作
     */
    public void exit(String type) {
        if (exited) {
            return;
        }
        exited = true;
        lastExitReason = type;
        log.warn("ServerSessionService exit,type [{}] service {}", type, this);
        running = false;
        Thread thread = syncThread;
        if (thread != null && thread != Thread.currentThread()) {
            thread.interrupt();
        }
        receiver.exit();
        try {
            onExit();
        } catch (Exception e) {
            log.warn("doClose error ", e);
        }
    }


    /**
     * 移除无用的上下文，在有异常、上下文关闭等情况下主动调用
     *
     * @param ctx
     */
    protected void removeCtx(CTX ctx) {
        receiver.removeCtx(ctx);
        try {
            closeCtx(ctx);
        } catch (Exception e) {
            log.warn("closeCtx error", e);
        }
    }

    public Receiver<CTX> getReceiver() {
        return receiver;
    }

    protected void markReady(String type) {
        long now = System.currentTimeMillis();
        lastReadyTime.set(now);
        log.info("服务已就绪 type={} readyAt={}", type, formatTs(now));
    }

    protected void transportDisconnected(String reason) {
        onTransportDisconnected(reason);
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

    public String getLastExitReason() {
        return lastExitReason;
    }

    public long getLastReadyTime() {
        return lastReadyTime.get();
    }

    public long getTransportReconnectCount() {
        return transportReconnectCount.get();
    }

    public boolean isRunning() {
        return running;
    }

    public long getLastHeartbeatTime() {
        return receiver.getLastHeartbeatTime();
    }

    public boolean hasActiveClient() {
        return receiver.hasActiveClient();
    }

    public int getActiveClientCount() {
        return receiver.getActiveClientCount();
    }

    public int getActiveSessionCount() {
        return receiver.getActiveSessionCount();
    }

    public List<Map<String, Object>> snapshotClients() {
        return receiver.snapshotClients();
    }

    public List<Map<String, Object>> snapshotSessions() {
        return receiver.snapshotSessions();
    }

    private static String formatTs(long ts) {
        if (ts <= 0) {
            return "never";
        }
        return new Date(ts).toString();
    }
}
