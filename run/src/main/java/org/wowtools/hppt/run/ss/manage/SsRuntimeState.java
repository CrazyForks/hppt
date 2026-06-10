package org.wowtools.hppt.run.ss.manage;

import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class SsRuntimeState {
    public enum Phase {
        STARTING,
        RUNNING,
        STOPPING,
        STOPPED,
        START_FAILED
    }

    private final SsConfig config;
    private final long processStartTime = System.currentTimeMillis();
    private final AtomicLong restartCount = new AtomicLong();
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    private volatile Phase phase = Phase.STARTING;
    private volatile ServerSessionService<?> currentService;
    private volatile String lastStartupError = "";
    private volatile long lastServiceStartAttemptTime = -1L;
    private volatile long lastServiceStoppedTime = -1L;

    public SsRuntimeState(SsConfig config) {
        this.config = config;
    }

    public void markStarting(long restartCount) {
        this.restartCount.set(restartCount);
        lastServiceStartAttemptTime = System.currentTimeMillis();
        phase = Phase.STARTING;
    }

    public void setCurrentService(ServerSessionService<?> service) {
        currentService = service;
        phase = Phase.RUNNING;
    }

    public void markStartFailed(Exception e) {
        lastStartupError = e == null ? "" : e.getMessage();
        phase = Phase.START_FAILED;
    }

    public void markStopped(ServerSessionService<?> service) {
        if (currentService == service) {
            currentService = service;
        }
        lastServiceStoppedTime = System.currentTimeMillis();
        phase = stopRequested.get() ? Phase.STOPPING : Phase.STOPPED;
    }

    public boolean requestRestart() {
        ServerSessionService<?> service = currentService;
        if (service == null || !service.isRunning()) {
            return false;
        }
        service.exit("admin restart");
        return true;
    }

    public boolean requestStop() {
        stopRequested.set(true);
        phase = Phase.STOPPING;
        ServerSessionService<?> service = currentService;
        if (service != null) {
            service.exit("admin stop");
        }
        return service != null;
    }

    public boolean isStopRequested() {
        return stopRequested.get();
    }

    public SsConfig getConfig() {
        return config;
    }

    public Map<String, Object> health() {
        ServerSessionService<?> service = currentService;
        Map<String, Object> res = new LinkedHashMap<>();
        String status = healthStatus(service);
        res.put("status", status);
        res.put("phase", phase.name());
        res.put("type", config.type);
        res.put("servicePort", config.port);
        res.put("restartCount", restartCount.get());
        res.put("stopRequested", stopRequested.get());
        res.put("processStartTime", processStartTime);
        if (service != null) {
            res.put("serviceRunning", service.isRunning());
            res.put("lastReadyTime", service.getLastReadyTime());
            res.put("lastExitReason", service.getLastExitReason());
            res.put("lastHeartbeatTime", service.getLastHeartbeatTime());
            res.put("activeClientCount", service.getActiveClientCount());
            res.put("activeSessionCount", service.getActiveSessionCount());
            res.put("transportReconnectCount", service.getTransportReconnectCount());
        }
        if (!lastStartupError.isEmpty()) {
            res.put("lastStartupError", lastStartupError);
        }
        return res;
    }

    public Map<String, Object> status() {
        ServerSessionService<?> service = currentService;
        Map<String, Object> res = health();
        res.put("lastServiceStartAttemptTime", lastServiceStartAttemptTime);
        res.put("lastServiceStoppedTime", lastServiceStoppedTime);
        Map<String, Object> runtimeConfig = new LinkedHashMap<>();
        runtimeConfig.put("type", config.type);
        runtimeConfig.put("port", config.port);
        runtimeConfig.put("heartbeatTimeout", config.heartbeatTimeout);
        runtimeConfig.put("initSessionTimeout", config.initSessionTimeout);
        runtimeConfig.put("sessionTimeout", config.sessionTimeout);
        runtimeConfig.put("messageQueueSize", config.messageQueueSize);
        runtimeConfig.put("maxReturnBodySize", config.maxReturnBodySize);
        runtimeConfig.put("restartDelayMillis", config.restartDelayMillis);
        runtimeConfig.put("transportReconnectBaseDelayMillis", config.transportReconnectBaseDelayMillis);
        runtimeConfig.put("transportReconnectMaxDelayMillis", config.transportReconnectMaxDelayMillis);
        runtimeConfig.put("transportReconnectJitterMillis", config.transportReconnectJitterMillis);
        res.put("config", runtimeConfig);
        if (service != null) {
            res.put("hasActiveClient", service.hasActiveClient());
            res.put("clients", service.snapshotClients());
            res.put("sessions", service.snapshotSessions());
        } else {
            res.put("clients", List.of());
            res.put("sessions", List.of());
        }
        return res;
    }

    public List<Map<String, Object>> sessions() {
        ServerSessionService<?> service = currentService;
        return service == null ? List.of() : service.snapshotSessions();
    }

    public List<Map<String, Object>> clients() {
        ServerSessionService<?> service = currentService;
        return service == null ? List.of() : service.snapshotClients();
    }

    public int healthHttpStatus() {
        String status = String.valueOf(health().get("status"));
        return "UP".equals(status) || "DEGRADED".equals(status) ? 200 : 503;
    }

    private String healthStatus(ServerSessionService<?> service) {
        if (stopRequested.get()) {
            return "DOWN";
        }
        if (phase == Phase.START_FAILED) {
            return "DOWN";
        }
        if (service == null) {
            return "STARTING";
        }
        if (!service.isRunning()) {
            return "DOWN";
        }
        if (service.getLastReadyTime() <= 0) {
            return "STARTING";
        }
        if (config.heartbeatTimeout > 0 && service.hasActiveClient()) {
            long lastHeartbeatTime = service.getLastHeartbeatTime();
            if (lastHeartbeatTime <= 0
                    || System.currentTimeMillis() - lastHeartbeatTime > config.heartbeatTimeout * 2) {
                return "DEGRADED";
            }
        }
        return "UP";
    }
}
