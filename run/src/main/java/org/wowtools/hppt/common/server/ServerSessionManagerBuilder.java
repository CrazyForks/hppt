package org.wowtools.hppt.common.server;

/**
 * @author liuyu
 * @date 2024/1/4
 */
public class ServerSessionManagerBuilder {
    protected ServerSessionLifecycle lifecycle;
    protected long sessionTimeout = 60000;

    public ServerSessionManagerBuilder setSessionTimeout(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public ServerSessionManagerBuilder setLifecycle(ServerSessionLifecycle lifecycle) {
        this.lifecycle = lifecycle;
        return this;
    }

    public ServerSessionManager build() {
        if (lifecycle == null) {
            throw new RuntimeException("lifecycle不能为空");
        }
        return new ServerSessionManager(this);
    }
}
