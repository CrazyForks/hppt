package org.wowtools.hppt.run.sc;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.Constant;
import org.wowtools.hppt.common.util.ResourcesReader;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.util.Date;

/**
 * @author liuyu
 * @date 2024/1/30
 */
@Slf4j
public class RunSc {
    private static final int STARTUP_MAX_ATTEMPTS = 3;
    private static final long STARTUP_RETRY_DELAY_MILLIS = 10_000L;

    public static void main(String[] args) {
        String configPath;
        if (args.length <= 1) {
            configPath = "sc.yml";
        } else {
            configPath = args[1];
        }
        ScConfig config;
        try {
            config = Constant.ymlMapper.readValue(ResourcesReader.readStr(RunSc.class, configPath), ScConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("读取配置文件异常", e);
        }
        long restartCount = 0;
        while (true) {
            restartCount++;
            ClientSessionService clientSessionService = startWithRetry(config, restartCount);
            try {
                clientSessionService.close();
            } catch (Exception e) {
                log.warn("关闭ClientSessionService异常", e);
            }
            log.warn("SC service stopped restartCount={} lastExitReason={} transportReconnects={} lastReadyAt={}",
                    restartCount,
                    clientSessionService.getLastExitReason(),
                    clientSessionService.getTransportReconnectCount(),
                    formatTs(clientSessionService.getLastReadyTime()));
            long restartDelayMillis = Math.max(0L, config.restartDelayMillis);
            log.warn("----------------------销毁当前Service,{}毫秒后重启", restartDelayMillis);
            try {
                Thread.sleep(restartDelayMillis);
            } catch (InterruptedException e) {
            }
        }
    }

    private static ClientSessionService startWithRetry(ScConfig config, long restartCount) {
        Exception lastException = null;
        for (int attempt = 1; attempt <= STARTUP_MAX_ATTEMPTS; attempt++) {
            ClientSessionService clientSessionService = null;
            try {
                log.info("SC startupAttempt={}/{}", attempt, STARTUP_MAX_ATTEMPTS);
                clientSessionService = ClientSessionServiceBuilder.build(config);
                clientSessionService.sync();
                return clientSessionService;
            } catch (Exception e) {
                lastException = e;
                if (clientSessionService != null && clientSessionService.getLastReadyTime() > 0) {
                    return clientSessionService;
                }
                log.error("SC service startup failed restartCount={} startupAttempt={}/{} lastExitReason={} lastReadyAt={}",
                        restartCount,
                        attempt,
                        STARTUP_MAX_ATTEMPTS,
                        clientSessionService == null ? "build failed" : clientSessionService.getLastExitReason(),
                        clientSessionService == null ? "never" : formatTs(clientSessionService.getLastReadyTime()),
                        e);
                if (clientSessionService != null) {
                    try {
                        clientSessionService.close();
                    } catch (Exception closeErr) {
                        log.warn("关闭ClientSessionService异常", closeErr);
                    }
                }
                if (attempt < STARTUP_MAX_ATTEMPTS) {
                    sleepStartupRetryDelay();
                }
            }
        }
        throw new RuntimeException("SC service startup failed after " + STARTUP_MAX_ATTEMPTS + " attempts", lastException);
    }

    private static void sleepStartupRetryDelay() {
        log.warn("startup failed, retry after {} ms", STARTUP_RETRY_DELAY_MILLIS);
        try {
            Thread.sleep(STARTUP_RETRY_DELAY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("startup retry interrupted", e);
        }
    }

    private static String formatTs(long ts) {
        if (ts <= 0) {
            return "never";
        }
        return new Date(ts).toString();
    }
}
