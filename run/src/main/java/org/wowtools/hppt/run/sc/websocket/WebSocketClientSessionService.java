package org.wowtools.hppt.run.sc.websocket;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.stream.ChunkedWriteHandler;
import lombok.extern.slf4j.Slf4j;
import org.wowtools.hppt.common.util.BytesUtil;
import org.wowtools.hppt.common.util.NettyObjectBuilder;
import org.wowtools.hppt.run.sc.common.ClientSessionService;
import org.wowtools.hppt.run.sc.pojo.ScConfig;

import java.net.URI;

/**
 * @author liuyu
 * @date 2024/3/12
 */
@Slf4j
public class WebSocketClientSessionService extends ClientSessionService {
    private Channel wsChannel;

    private EventLoopGroup group;

    public WebSocketClientSessionService(ScConfig config) throws Exception {
        super(config);
    }

    @Override
    public void connectToServer(ScConfig config, Cb cb) throws Exception {
        try {
            newWsConn(config, cb);
        } catch (Exception e) {
            cb.end(e);
            throw e;
        }
    }

    private void newWsConn(ScConfig config, Cb cb) throws Exception {
        doClose();
        final URI webSocketURL = new URI(config.websocket.serverUrl + "/s");//随便加一个后缀防止被nginx转发时识别不到
        group = NettyObjectBuilder.buildEventLoopGroup(config.websocket.workerGroupNum);
        Bootstrap boot = new Bootstrap();
        boot.option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .group(group)
                .channel(NettyObjectBuilder.getSocketChannelClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    protected void initChannel(SocketChannel sc) throws Exception {
                        int bodySize = config.maxSendBodySize * 2;
                        if (bodySize < 0) {
                            bodySize = Integer.MAX_VALUE;
                        }
                        ChannelPipeline pipeline = sc.pipeline();
                        pipeline.addLast(new HttpClientCodec());
                        pipeline.addLast(new ChunkedWriteHandler());
                        pipeline.addLast(new HttpObjectAggregator(bodySize));

                        pipeline.addLast(new WebSocketClientProtocolHandler(WebSocketClientHandshakerFactory.newHandshaker(webSocketURL, WebSocketVersion.V13, null, false, new DefaultHttpHeaders(), bodySize)));
                        pipeline.addLast(new SimpleChannelInboundHandler<BinaryWebSocketFrame>() {
                            boolean inited = false;

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame msg) throws Exception {
                                receiveServerBytes(BytesUtil.byteBuf2bytes(msg.content()));
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                super.exceptionCaught(ctx, cause);
                                ctx.close();
                                exit();
                                if (!inited) {
                                    cb.end(cause);
                                }
                            }

                            @Override
                            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                if (WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE.equals(evt)) {
                                    //初始化完成
                                    //定期发一个ping防止掉线
                                    long pingInterval = config.websocket.pingInterval;
                                    if (pingInterval > 0) {
                                        Thread.startVirtualThread(() -> {
                                            while (running) {
                                                try {
                                                    Thread.sleep(pingInterval);
                                                } catch (InterruptedException e) {
                                                    throw new RuntimeException(e);
                                                }
                                                Throwable e = BytesUtil.writeObjToChannel(ctx.channel(), new PingWebSocketFrame());
                                                if (null != e) {
                                                    log.warn("ping err", e);
                                                    exit();
                                                }
                                            }
                                        });
                                    }
                                    cb.end(null);
                                    inited = true;
                                }
                                super.userEventTriggered(ctx, evt);
                            }
                        });
                    }
                });

        ChannelFuture cf = boot.connect(webSocketURL.getHost(), webSocketURL.getPort()).sync();
        wsChannel = cf.channel();
    }

    @Override
    public void sendBytesToServer(byte[] bytes) {
        BinaryWebSocketFrame frame = new BinaryWebSocketFrame(BytesUtil.bytes2byteBuf(wsChannel, bytes));
        Throwable e = BytesUtil.writeObjToChannel(wsChannel, frame);
        if (null != e) {
            log.warn("sendBytesToServer err", e);
            exit();
        }
    }


    @Override
    protected void doClose() throws Exception {
        if (null != wsChannel) {
            wsChannel.close();
        }
        if (null != group) {
            group.shutdownGracefully();
        }
    }
}
