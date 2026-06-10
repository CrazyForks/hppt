package org.wowtools.hppt.run.ss;

import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.AddonsLoader;
import org.wowtools.hppt.common.util.Constant;
import org.wowtools.hppt.common.util.ResourcesReader;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.file.FileServerSessionService;
import org.wowtools.hppt.run.ss.hppt.HpptServerSessionService;
import org.wowtools.hppt.run.ss.manage.ManagementHttpServer;
import org.wowtools.hppt.run.ss.manage.SsRuntimeState;
import org.wowtools.hppt.run.ss.pojo.SsConfig;
import org.wowtools.hppt.run.ss.post.PostServerSessionService;
import org.wowtools.hppt.run.ss.rhppt.RHpptServerSessionService;
import org.wowtools.hppt.run.ss.rpost.RPostServerSessionService;
import org.wowtools.hppt.run.ss.websocket.WebsocketServerSessionService;

import java.util.Date;

/**
 * @author liuyu
 * @date 2024/1/24
 */
@Slf4j
public class RunSs {
    private static final int STARTUP_MAX_ATTEMPTS = 3;
    private static final long STARTUP_RETRY_DELAY_MILLIS = 10_000L;

    public static void main(String[] args) throws Exception {

        String configPath;
        if (args.length <= 1) {
            configPath = "ss.yml";
        } else {
            configPath = args[1];
        }
        SsConfig config;
        try {
            config = Constant.ymlMapper.readValue(ResourcesReader.readStr(RunSs.class, configPath), SsConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("读取配置文件异常", e);
        }
        SsRuntimeState runtimeState = new SsRuntimeState(config);
        try (ManagementHttpServer managementHttpServer = new ManagementHttpServer(runtimeState, config.management)) {
            managementHttpServer.start();
            long restartCount = 0;
            while (!runtimeState.isStopRequested()) {
                restartCount++;
                ServerSessionService<?> sessionService;
                try {
                    sessionService = startWithRetry(config, runtimeState, restartCount);
                } catch (Exception e) {
                    runtimeState.markStartFailed(e);
                    throw e;
                }

                runtimeState.markStopped(sessionService);
                log.warn("SS service stopped restartCount={} lastExitReason={} transportReconnects={} lastReadyAt={} service={}",
                        restartCount,
                        sessionService.getLastExitReason(),
                        sessionService.getTransportReconnectCount(),
                        formatTs(sessionService.getLastReadyTime()),
                        sessionService);
                if (runtimeState.isStopRequested()) {
                    break;
                }
                long restartDelayMillis = Math.max(0L, config.restartDelayMillis);
                try {
                    Thread.sleep(restartDelayMillis);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("restart delay interrupted", e1);
                }
            }
        }

    }

    private static ServerSessionService<?> startWithRetry(SsConfig config, SsRuntimeState runtimeState, long restartCount) throws Exception {
        Exception lastException = null;
        for (int attempt = 1; attempt <= STARTUP_MAX_ATTEMPTS; attempt++) {
            ServerSessionService<?> sessionService = null;
            try {
                log.info("type {} startupAttempt={}/{}", config.type, attempt, STARTUP_MAX_ATTEMPTS);
                runtimeState.markStarting(restartCount);
                sessionService = buildSessionService(config);
                runtimeState.setCurrentService(sessionService);
                sessionService.syncStart(config);
                return sessionService;
            } catch (Exception e) {
                lastException = e;
                if (sessionService != null && sessionService.getLastReadyTime() > 0) {
                    return sessionService;
                }
                runtimeState.markStartFailed(e);
                log.error("SS service startup failed restartCount={} startupAttempt={}/{} lastExitReason={} lastReadyAt={} service={}",
                        restartCount,
                        attempt,
                        STARTUP_MAX_ATTEMPTS,
                        sessionService == null ? "build failed" : sessionService.getLastExitReason(),
                        sessionService == null ? "never" : formatTs(sessionService.getLastReadyTime()),
                        sessionService,
                        e);
                if (attempt < STARTUP_MAX_ATTEMPTS) {
                    sleepStartupRetryDelay();
                }
            }
        }
        throw new IllegalStateException("SS service startup failed after " + STARTUP_MAX_ATTEMPTS + " attempts", lastException);
    }

    private static ServerSessionService<?> buildSessionService(SsConfig config) throws Exception {
        return switch (config.type) {
            case "post" -> new PostServerSessionService(config);
            case "websocket" -> new WebsocketServerSessionService(config);
            case "hppt" -> new HpptServerSessionService(config);
            case "rhppt" -> new RHpptServerSessionService(config);
            case "rpost" -> new RPostServerSessionService(config);
            case "file" -> new FileServerSessionService(config);
            default -> {
                String addonsPath = config.addonsPath;
                if (null == addonsPath) {
                    addonsPath = ResourcesReader.getRootPath(RunSs.class) + "/addons";
                }
                AddonsLoader addonsLoader = new AddonsLoader(addonsPath);
                Class<?> clazz = addonsLoader.loadClass(config.type);
                ServerSessionService in = (ServerSessionService) clazz.getConstructor(SsConfig.class).newInstance(config);
                log.info("自定义服务启动成功 {}", clazz);
                yield in;
            }
        };
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
