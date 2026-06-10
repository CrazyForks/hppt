package org.wowtools.hppt.run.ss.post;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.common.utils.LruCache;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.util.Map;

/**
 * @author liuyu
 * @date 2024/1/24
 */
@Slf4j
public class PostServerSessionService extends ServerSessionService<PostCtx> {

    private NettyHttpServer server;

    public PostServerSessionService(SsConfig ssConfig) throws Exception {
        super(ssConfig);
        int size = null == ssConfig.clients ? 8 : ssConfig.clients.size() * 2;
        ctxMap = LruCache.buildCache(size, size);
    }

    @Override
    protected void init(SsConfig ssConfig) throws Exception {
        log.info("*********");
        server = new NettyHttpServer(ssConfig.port, this, ssConfig);
        server.start();
        startCtxCleanupThread(ssConfig);
    }

    protected final Map<String, PostCtx> ctxMap;

    @Override
    protected void sendBytesToClient(PostCtx ctx, byte[] bytes) {
        ctx.sendQueue.add(bytes);
    }

    @Override
    protected void closeCtx(PostCtx ctx) {
        ctxMap.remove(ctx.cookie);
    }

    @Override
    public void onExit() throws Exception {
        server.stop();
    }

    private void startCtxCleanupThread(SsConfig ssConfig) {
        long heartbeatTimeout = ssConfig.heartbeatTimeout;
        if (heartbeatTimeout <= 0) {
            return;
        }
        long sleepMillis = Math.max(1000L, heartbeatTimeout / 2);
        Thread.startVirtualThread(() -> {
            while (running) {
                try {
                    Thread.sleep(sleepMillis);
                } catch (InterruptedException ignored) {
                }
                long now = System.currentTimeMillis();
                ctxMap.values().forEach(ctx -> {
                    if (now - ctx.getLastRequestTime() > heartbeatTimeout) {
                        log.info("post ctx stale, remove {}", ctx.cookie);
                        removeCtx(ctx);
                    }
                });
            }
        });
    }


}
