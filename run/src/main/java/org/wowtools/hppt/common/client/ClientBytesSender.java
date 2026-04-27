package org.wowtools.hppt.common.client;

import org.wowtools.hppt.common.pojo.SessionBytes;
import org.wowtools.hppt.common.util.RoughTimeUtil;

import java.net.Socket;

/**
 * @author liuyu
 * @date 2024/2/1
 */
public interface ClientBytesSender {

    /**
     * sessionId回调函数
     */
    public static abstract class SessionIdCallBack {
        public final long createTime = RoughTimeUtil.getTimestamp();
        public final Socket socket;

        public SessionIdCallBack(Socket socket) {
            this.socket = socket;
        }

        public abstract void cb(int sessionId);
    }

    /**
     * 用户建立连接后，准备新建一个ClientSession前触发
     *
     * @param port   本地端口
     * @param socket 建立连接对应的Socket
     * @param cb     生成一个唯一的sessionId返回，一般需要借助服务端接口来分配id
     */
    void connected(int port, Socket socket, SessionIdCallBack cb);

    /**
     * 向目标发送字节的具体方式，如post请求，websocket等
     *
     * @param clientSession clientSession
     * @param sessionBytes  bytes
     */
    void sendToTarget(ClientSession clientSession, SessionBytes sessionBytes);
}
