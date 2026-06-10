package org.wowtools.hppt.run.sc.post;

import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.wowtools.hppt.common.util.BytesUtil;
import org.wowtools.hppt.common.util.HttpUtil;
import org.wowtools.hppt.common.util.IoThreadUtil;
import org.wowtools.hppt.common.util.ReconnectBackoff;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liuyu
 * @date 2024/1/31
 */
@Slf4j
public class PostClientSessionService extends ClientSessionService {

    private BlockingQueue<byte[]> sendQueue;
    private ReconnectBackoff backoff;
    private AtomicBoolean transportConnected;
    private volatile String cookie;
    private volatile Cb connectionCb;


    public PostClientSessionService(ScConfig config) throws Exception {
        super(config);
    }


    @Override
    public void connectToServer(ScConfig config, Cb cb) {
        ensureInitialized();
        connectionCb = cb;
        startSendThread();
        startReplyThread();
    }

    private void startSendThread() {
        IoThreadUtil.startIoThread(() -> {
            final long sendSleepTime = config.post.sendSleepTime;
            while (running) {
                try {
                    byte[] sendBytes;
                    List<byte[]> bytesList = new LinkedList<>();
                    sendBytes = sendQueue.take();
                    bytesList.add(sendBytes);
                    if (sendSleepTime > 0) {
                        try {
                            Thread.sleep(sendSleepTime);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    sendQueue.drainTo(bytesList);
                    sendBytes = BytesUtil.bytesCollection2PbBytes(bytesList);
                    String sendUrl = currentSendUrl();
                    if (log.isDebugEnabled()) {
                        long t = System.currentTimeMillis();
                        try (Response r = HttpUtil.doPost(sendUrl, sendBytes)) {
                            assertSuccessResponse(r, "post send");
                            assert r.body() != null;
                            byte[] rBytes = r.body().bytes();
                            if (rBytes.length == 0) {
                                markRequestSuccess();
                                log.debug("SendThread 发送完成,cost {}", System.currentTimeMillis() - t);
                            } else {
                                throw new RuntimeException("异常的响应值" + new String(rBytes, StandardCharsets.UTF_8));
                            }
                        }
                    } else {
                        try (Response r = HttpUtil.doPost(sendUrl, sendBytes)) {
                            assertSuccessResponse(r, "post send");
                            assert r.body() != null;
                            byte[] rBytes = r.body().bytes();
                            if (rBytes.length == 0) {
                                markRequestSuccess();
                                log.debug("SendThread 发送完成");
                            } else {
                                throw new RuntimeException("异常的响应值" + new String(rBytes, StandardCharsets.UTF_8));
                            }
                        }
                    }

                } catch (Exception e) {
                    if (!running) {
                        return;
                    }
                    log.warn("SendThread异常", e);
                    handleTransportFailure("post send err:" + e.getMessage());
                }
            }
        }, "hppt-io-post-send");
    }

    private void startReplyThread() {
        IoThreadUtil.startIoThread(() -> {
            final long sendSleepTime = config.post.sendSleepTime;
            while (running) {
                //发一个接收请求接数据
                try {
                    byte[] responseBytes;
                    String replyUrl = currentReplyUrl();
                    try (Response response = HttpUtil.doPost(replyUrl, null)) {
                        assertSuccessResponse(response, "post reply");
                        ResponseBody body = response.body();
                        responseBytes = null == body ? null : body.bytes();
                        markRequestSuccess();
                    }
                    if (null != responseBytes && responseBytes.length > 0) {
                        Collection<byte[]> bytesList;
                        try {
                            bytesList = BytesUtil.pbBytes2BytesList(responseBytes).getBytes();
                        } catch (Exception e) {
                            log.warn("解析字节异常 {}", new String(responseBytes, StandardCharsets.UTF_8));
                            throw e;
                        }
                        for (byte[] bytes : bytesList) {
                            receiveServerBytes(bytes);
                        }
                    }
                } catch (Exception e) {
                    if (!running) {
                        return;
                    }
                    log.warn("ReplyThread异常", e);
                    handleTransportFailure("post reply err:" + e.getMessage());
                }
                //按需做等待
                if (sendSleepTime > 0) {
                    try {
                        Thread.sleep(sendSleepTime);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }, "hppt-io-post-reply");
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        ensureInitialized();
        sendQueue.add(bytes);
    }

    @Override
    public void exit() {
        exit("manual exit");
    }

    @Override
    protected void onTransportDisconnected(String reason) {
        ensureInitialized();
        sendQueue.clear();
        cookie = newCookie();
    }

    private void markRequestSuccess() {
        ensureInitialized();
        if (transportConnected.compareAndSet(false, true)) {
            backoff.reset();
            markTransportReady("post");
            Cb cb = connectionCb;
            if (cb != null) {
                cb.end(null);
            }
        }
    }

    private void handleTransportFailure(String reason) {
        ensureInitialized();
        boolean firstDisconnect = transportConnected.getAndSet(false);
        if (firstDisconnect) {
            transportDisconnected(reason);
        }
        long delayMillis = backoff.nextDelayMillis();
        recordTransportReconnect("post", reason, backoff.getConsecutiveFailures(), delayMillis);
        sleepForReconnect(delayMillis);
    }

    private String currentSendUrl() {
        return config.post.serverUrl + "/s?c=" + cookie;
    }

    private String currentReplyUrl() {
        return config.post.serverUrl + "/r?c=" + cookie;
    }

    private static void assertSuccessResponse(Response response, String type) {
        if (!response.isSuccessful()) {
            throw new RuntimeException(type + " http code " + response.code());
        }
    }

    private static String newCookie() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private synchronized void ensureInitialized() {
        if (sendQueue == null) {
            sendQueue = new LinkedBlockingQueue<>();
        }
        if (backoff == null) {
            backoff = new ReconnectBackoff(config.transportReconnectBaseDelayMillis,
                    config.transportReconnectMaxDelayMillis, config.transportReconnectJitterMillis);
        }
        if (transportConnected == null) {
            transportConnected = new AtomicBoolean();
        }
        if (cookie == null || cookie.isEmpty()) {
            cookie = newCookie();
        }
    }
}
