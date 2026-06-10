package org.wowtools.hppt.run.ss.common;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * @author liuyu
 * @date 2024/9/26
 */
@Getter
@Setter
sealed abstract class Receiver<CTX> permits PortReceiver {
    public abstract void receiveClientBytes(CTX ctx, byte[] bytes) throws Exception;

    public abstract void removeCtx(CTX ctx);

    public abstract void exit();

    public abstract long getLastHeartbeatTime();

    public abstract boolean hasActiveClient();

    public abstract int getActiveClientCount();

    public abstract int getActiveSessionCount();

    public abstract List<Map<String, Object>> snapshotClients();

    public abstract List<Map<String, Object>> snapshotSessions();
}
