package org.wowtools.hppt.run.ss.pojo;

import org.wowtools.hppt.common.util.CommonConfig;

import java.util.ArrayList;

/**
 * @author liuyu
 * @date 2023/11/25
 */
public class SsConfig extends CommonConfig {

    /**
     * 运行类型 支持 websocket(以websocket协议传输数据)、post(以http post协议传输数据)、hppt(以hppt自定义的协议传输数据)
     */
    public String Type;

    /**
     * 服务端口
     */
    public int port;

    /**
     * 超过sessionTimeout，给客户端发送存活确认命令，若下一个sessionTimeout内未收到确认，则强制关闭服务
     */
    public long sessionTimeout = 120000;

    /**
     * 接收到客户端/真实端口的数据时，数据被暂存在一个队列里，队列满后强制关闭会话
     */
    public int messageQueueSize = 2048;


    /**
     * 允许的客户端
     */
    public ArrayList<String> clientIds;

    public static final class PostConfig {

    }

    public static final class WebSocketConfig {

    }

    public static final class HpptConfig {

    }
}