package org.wowtools.hppt.common.util;

import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.SystemPropertyUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadFactory;

/**
 *
 * @author liuyu
 * @date 2024/6/26
 */
@Slf4j
public class NettyChannelTypeChecker {
    private static final int DEFAULT_EVENT_LOOP_THREADS;

    static {
        //本项目用了虚拟线程，且写数据用了阻塞等待，所以把线程数调高一些
        DEFAULT_EVENT_LOOP_THREADS = Math.max(128, SystemPropertyUtil.getInt(
                "io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 16));
        log.debug("-Dio.netty.eventLoopThreads: {}", DEFAULT_EVENT_LOOP_THREADS);
    }


    //TODO epoll+虚拟线程+高线程数 会导致最多只有CPU核心数的线程工作，原因暂不明确，先全部使用NioEventLoopGroup
    public static Class<? extends ServerChannel> getServerSocketChannelClass() {
        return NioServerSocketChannel.class;
    }
    public static Class<? extends NioSocketChannel> getSocketChannelClass() {
        return NioSocketChannel.class;
    }


    public static EventLoopGroup buildVirtualThreadEventLoopGroup(int nThread) {
        return new NioEventLoopGroup(nThread, new VirtualThreadFactory());
    }

    public static EventLoopGroup buildVirtualThreadEventLoopGroup() {
        return buildVirtualThreadEventLoopGroup(DEFAULT_EVENT_LOOP_THREADS);
    }

    private static final class VirtualThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            return Thread.ofVirtual().unstarted(r);
        }
    }
}
