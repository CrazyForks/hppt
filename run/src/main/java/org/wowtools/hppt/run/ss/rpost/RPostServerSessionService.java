package org.wowtools.hppt.run.ss.rpost;

import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.wowtools.hppt.common.util.BytesUtil;
import org.wowtools.hppt.common.util.HttpUtil;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.common.util.ReconnectBackoff;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liuyu
 * @date 2024/4/16
 */
@Slf4j
public class RPostServerSessionService extends ServerSessionService<RPostCtx> {

    private final BlockingQueue<byte[]> sendQueue = new LinkedBlockingQueue<>();
    private final String sendUrl;
    private final String receiveUrl;
    private final ReconnectBackoff backoff;
    private final AtomicBoolean transportConnected = new AtomicBoolean();


    private volatile boolean actived = true;
    private volatile RPostCtx currentCtx;


    public RPostServerSessionService(SsConfig ssConfig) {
        super(ssConfig);
        sendUrl = ssConfig.rpost.serverUrl + "/s";
        receiveUrl = ssConfig.rpost.serverUrl + "/r";
        backoff = new ReconnectBackoff(ssConfig.transportReconnectBaseDelayMillis,
                ssConfig.transportReconnectMaxDelayMillis, ssConfig.transportReconnectJitterMillis);
    }

    @Override
    protected void init(SsConfig ssConfig) throws Exception {
        startSendThread();
        startReceiveThread();
    }

    private void startSendThread() {
        IoThreadUtil.startIoThread(() -> {
            while (actived) {
                try {
                    byte[] sendBytes = sendQueue.poll(10000, TimeUnit.MINUTES);
                    if (null == sendBytes || !actived) {
                        continue;
                    }
                    List<byte[]> bytesList = new LinkedList<>();
                    bytesList.add(sendBytes);
                    sendBytes = BytesUtil.bytesCollection2PbBytes(bytesList);
                    try (Response response = HttpUtil.doPost(receiveUrl, sendBytes)) {
                        assertSuccessResponse(response, "rpost send");
                        markRequestSuccess();
                    }
                } catch (Exception e) {
                    if (!actived || !running) {
                        return;
                    }
                    log.warn("发送线程执行异常", e);
                    handleTransportFailure("rpost send err:" + e.getMessage());
                }
            }
        }, "hppt-io-rpost-send");
    }

    private void startReceiveThread() {
        IoThreadUtil.startIoThread(() -> {
            while (actived) {
                try {
                    byte[] responseBytes;
                    try (Response response = HttpUtil.doPost(sendUrl, null)) {
                        assertSuccessResponse(response, "rpost receive");
                        ResponseBody body = response.body();
                        responseBytes = null == body ? null : body.bytes();
                    }
                    markRequestSuccess();
                    if (null != responseBytes && responseBytes.length > 0) {
                        log.debug("收到服务端响应字节数 {}", responseBytes.length);
                        Collection<byte[]> bytesList = BytesUtil.pbBytes2BytesList(responseBytes).getBytes();
                        for (byte[] bytes : bytesList) {
                            receiveClientBytes(getOrCreateCtx(), bytes);
                        }
                    }
                } catch (Exception e) {
                    if (!actived || !running) {
                        return;
                    }
                    log.warn("接收线程执行异常", e);
                    handleTransportFailure("rpost receive err:" + e.getMessage());
                }
            }
        }, "hppt-io-rpost-recv");
    }

    @Override
    protected void sendBytesToClient(RPostCtx rPostCtx, byte[] bytes) {
        sendQueue.add(bytes);
    }

    @Override
    protected void closeCtx(RPostCtx rPostCtx) throws Exception {
        // 无需额外资源释放
    }

    @Override
    protected void onExit() throws Exception {
        actived = false;
    }

    @Override
    protected void onTransportDisconnected(String reason) {
        sendQueue.clear();
        RPostCtx ctx = currentCtx;
        currentCtx = null;
        if (ctx != null) {
            removeCtx(ctx);
        }
    }

    private void markRequestSuccess() {
        if (transportConnected.compareAndSet(false, true)) {
            backoff.reset();
            markReady("rpost transport");
        }
    }

    private void handleTransportFailure(String reason) {
        boolean firstDisconnect = transportConnected.getAndSet(false);
        if (firstDisconnect) {
            transportDisconnected(reason);
        }
        long delayMillis = backoff.nextDelayMillis();
        recordTransportReconnect("rpost", reason, backoff.getConsecutiveFailures(), delayMillis);
        sleepForReconnect(delayMillis);
    }

    private RPostCtx getOrCreateCtx() {
        RPostCtx ctx = currentCtx;
        if (ctx != null) {
            return ctx;
        }
        synchronized (this) {
            if (currentCtx == null) {
                currentCtx = new RPostCtx();
            }
            return currentCtx;
        }
    }

    private static void assertSuccessResponse(Response response, String type) {
        if (!response.isSuccessful()) {
            throw new RuntimeException(type + " http code " + response.code());
        }
    }
}
