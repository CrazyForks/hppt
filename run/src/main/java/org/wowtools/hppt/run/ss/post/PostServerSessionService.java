package org.wowtools.hppt.run.ss.post;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.wowtools.common.utils.LruCache;
import org.wowtools.hppt.common.util.DisableTraceFilter;
import org.wowtools.hppt.run.ss.common.ServerSessionService;
import org.wowtools.hppt.run.ss.pojo.SsConfig;

import java.util.Map;

/**
 * @author liuyu
 * @date 2024/1/24
 */
@Slf4j
public class PostServerSessionService extends ServerSessionService<PostCtx> {

    private Server server;

    public PostServerSessionService(SsConfig ssConfig) throws Exception {
        super(ssConfig);
        ctxMap = LruCache.buildCache(ssConfig.clients.size() * 2, ssConfig.clients.size() * 2);
    }

    @Override
    public void init(SsConfig ssConfig) throws Exception {
        log.info("*********");
        server = new Server();
        // 创建一个ServerConnector
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(ssConfig.port);
        // 设置请求超时时间（以毫秒为单位）
        connector.setIdleTimeout(ssConfig.post.waitResponseTime * 2);
        // 将connector添加到server
        server.addConnector(connector);

        ServletContextHandler context = new ServletContextHandler(server, "/");
        context.addServlet(new ServletHolder(new SendServlet(this)), "/s");
        context.addServlet(new ServletHolder(new ReplyServlet(this, ssConfig.post.waitResponseTime, ssConfig.post.replyDelayTime)), "/r");
        context.addServlet(new ServletHolder(new ErrorServlet()), "/e");

        ErrorPageErrorHandler errorHandler = new ErrorPageErrorHandler();
        errorHandler.setShowServlet(false);
        errorHandler.setShowStacks(false);
        errorHandler.addErrorPage(400, 599, "/e");
        errorHandler.setServer(new Server());

        context.setErrorHandler(errorHandler);
        context.addFilter(DisableTraceFilter.class, "/*", null);

        log.info("服务端启动完成 端口 {}", ssConfig.port);
        server.start();
        server.join();
    }

    protected final Map<String, PostCtx> ctxMap;

    @Override
    protected void sendBytesToClient(PostCtx ctx, byte[] bytes) {
        ctx.sendQueue.add(bytes);
    }

    @Override
    protected void closeCtx(PostCtx ctx) {
        ctxMap.remove(ctx.cookie);
    }

    @Override
    public void doClose() throws Exception {
        server.stop();
    }


}
