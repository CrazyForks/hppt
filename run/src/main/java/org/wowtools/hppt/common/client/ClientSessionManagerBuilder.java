package org.wowtools.hppt.common.client;

/**
 * @author liuyu
 * @date 2024/1/4
 */
public class ClientSessionManagerBuilder {
    protected int bufferSize;
    protected ClientSessionLifecycle lifecycle;
    protected ClientBytesSender clientBytesSender;

    public ClientSessionManagerBuilder setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public ClientSessionManagerBuilder setLifecycle(ClientSessionLifecycle lifecycle) {
        this.lifecycle = lifecycle;
        return this;
    }

    public ClientSessionManagerBuilder setClientBytesSender(ClientBytesSender clientBytesSender) {
        this.clientBytesSender = clientBytesSender;
        return this;
    }

    public ClientSessionManager build() {
        if (bufferSize <= 0) {
            bufferSize = 10240;
        }
        if (lifecycle == null) {
            throw new RuntimeException("lifecycle不能为空");
        }
        return new ClientSessionManager(this);
    }
}
