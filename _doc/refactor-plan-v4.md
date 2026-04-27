# HPPT v4.x 核心层重构计划

> 重构目标：将核心代理层从 Netty 迁移到 Socket + 虚拟线程，简化缓冲队列架构
> 允许 breaking change（v4.x 分支）
> 策略：逐步迁移 — 第一期重构核心代理层，传输层保持不变

---

## 一、当前问题

### 1.1 Netty 在核心层的滥用

| 位置 | 问题 | 严重程度 |
|------|------|----------|
| `ClientSessionManager` | 用 Netty ServerBootstrap 做本地端口监听，引入不必要的复杂度 | 中 |
| `ClientSessionManager.SimpleHandler.decode()` | **synchronized** 方法阻塞 Netty EventLoop；内含最多 100 秒 busy-wait（`Thread.sleep(100)` 循环 1000 次） | 严重 |
| `ServerSessionManager` | 用 Netty Bootstrap 连接目标端口 | 中 |
| `ServerSessionManager.SimpleHandler.channelRead()` | 调用 `future.get(30, TimeUnit.SECONDS)` 阻塞 Netty I/O 线程 | 严重 |
| `BytesUtil.writeToChannelHandlerContext()` | 轮询等待 channel writable，最久 30 秒 | 中 |
| `ClientSession` / `ServerSession` | 持有 Netty `ChannelHandlerContext`/`Channel`，需要处理 ByteBuf 引用计数 | 中 |

### 1.2 缓冲队列层级过深

当前 SC 侧用户→目标数据路径：
```
用户Socket → [Netty decode(synchronized)] → ClientBytesSender.sendToTarget()
  → BufferPool<SessionBytes> sendBytesQueue          ← 第1层（在 PortReceiver 中）
  → ClientTalker 打包/加密 → Transport 发送
```

当前 SC 侧目标→用户数据路径：
```
Transport 接收 → ArrayBlockingQueue receiveServerBytesQueue  ← 第1层（在 ClientSessionService 中）
  → ClientTalker 解密/解析
  → ClientSession.sendToUserBytesQueue                        ← 第2层（在 ClientSession 中）
  → [虚拟线程消费] → Netty write → 用户Socket
```

当前 SS 侧目标→用户数据路径：
```
目标Socket InputStream → [虚拟线程] → ServerSession.sendBytesQueue  ← 第1层（在 ServerSession 中）
  → LoginClientService.Client.sessionBytesQueue                     ← 第2层
  → replyToClient 批量回复
```

问题：`ClientSession.sendToUserBytesQueue` 和 `ServerSession.sendBytesQueue` 是 per-session 的中间缓冲，增加延迟且无必要。

### 1.3 其他问题

- `PortReceiver`（SC 侧）中 `System.exit(0)`：登录失败时直接退出，嵌入使用时无法优雅关闭
- `ClientTalker.receiveServerBytes()` 第 185-203 行：找不到 ClientSession 时另起虚拟线程 busy-wait 最多 30 秒
- `ServerTalker` 第 202-217 行：静态 `BufferPool<SendAbleSessionBytesResult>` + 静态虚拟线程无限循环，无生命周期管理
- `ClientSessionManager` 中 `clientSessionMapByCtx` 用 Netty `ChannelHandlerContext` 做 key，新方案不再需要

---

## 二、新架构设计

### 2.1 数据流对比

**SC 侧 用户→目标（重构后）：**
```
用户Socket InputStream → 虚拟线程读取 → lifecycle.beforeSendToTarget()
  → 包装为 SessionBytes → 放入 sendBytesQueue（PortReceiver 的共享队列）
  → 发送线程：ClientTalker 打包加密 → Transport 发送
```
缓冲层级：1 层（sendBytesQueue）

**SC 侧 目标→用户（重构后）：**
```
Transport 接收 → receiveServerBytesQueue（ClientSessionService）
  → 虚拟线程消费 → ClientTalker 解密解析 → 路由到 ClientSession
  → lifecycle.beforeSendToUser() → 直接写入用户 Socket OutputStream
```
缓冲层级：1 层（receiveServerBytesQueue）

**SS 侧 用户→目标（重构后）：**
```
SC 发来数据 → ServerTalker 解密解析 → 路由到 ServerSession
  → lifecycle.beforeSendToTarget() → 直接写入目标 Socket OutputStream
```
缓冲层级：0 层（直接写入）

**SS 侧 目标→用户（重构后，不变）：**
```
目标 Socket InputStream → 虚拟线程读取 → lifecycle.sendToClientBuffer()
  → Client.sessionBytesQueue → replyToClient 批量回复
```
缓冲层级：1 层（sessionBytesQueue，多路复用需要，保留）

### 2.2 对比总结

| 方面 | 重构前 | 重构后 |
|------|--------|--------|
| 用户/目标连接 | Netty ServerBootstrap/Bootstrap | ServerSocket/Socket + 虚拟线程 |
| SC 侧缓冲层级 | 2 层 | 1 层 |
| SS 侧缓冲层级 | 2 层 | 0~1 层 |
| 线程模型 | Netty EventLoop + 虚拟线程混合 | 纯虚拟线程 |
| 内存管理 | ByteBuf 堆外内存 + 引用计数 | 纯 byte[] 堆内 |
| 核心层 Netty 依赖 | 全部 | 无 |

---

## 三、实施步骤

### Step 1：重写 ClientBytesSender 接口

**文件：** `run/src/main/java/org/wowtools/hppt/common/client/ClientBytesSender.java`

**改动说明：** 将接口中的 Netty 类型替换为 `java.net.Socket`

**当前代码：**
```java
public interface ClientBytesSender {
    public static abstract class SessionIdCallBack {
        public final long createTime = RoughTimeUtil.getTimestamp();
        public final ChannelHandlerContext channelHandlerContext;
        public SessionIdCallBack(ChannelHandlerContext channelHandlerContext) { ... }
        public abstract void cb(int sessionId);
    }
    void connected(int port, ChannelHandlerContext ctx, SessionIdCallBack cb);
    void sendToTarget(ClientSession clientSession, SessionBytes sessionBytes);
}
```

**改为：**
```java
public interface ClientBytesSender {
    public static abstract class SessionIdCallBack {
        public final long createTime = RoughTimeUtil.getTimestamp();
        public final Socket socket;
        public SessionIdCallBack(Socket socket) {
            this.socket = socket;
        }
        public abstract void cb(int sessionId);
    }
    void connected(int port, Socket socket, SessionIdCallBack cb);
    void sendToTarget(ClientSession clientSession, SessionBytes sessionBytes);
}
```

**影响范围：**
- `run/sc/common/PortReceiver.java` 第 243-272 行：`buildClientBytesSender()` 匿名实现需适配
- `run/sc/common/PortReceiver.java` 第 209 行：`entry.getValue().channelHandlerContext.close()` → `entry.getValue().socket.close()`

---

### Step 2：重写 ClientSession

**文件：** `run/src/main/java/org/wowtools/hppt/common/client/ClientSession.java`

**改动说明：** 用 `Socket` 替代 `ChannelHandlerContext`，去掉内部 `BufferPool`，改为直接写入

**当前代码核心结构：**
```java
public class ClientSession {
    private final int sessionId;
    private final ChannelHandlerContext channelHandlerContext;
    private final BufferPool<byte[]> sendToUserBytesQueue;  // ← 去掉
    private volatile boolean running = true;

    ClientSession(...) {
        // 启动虚拟线程从 sendToUserBytesQueue 取数据，写入 channel
    }
    public void sendToUser(byte[] bytes) { sendToUserBytesQueue.add(bytes); }
    void close() { running = false; channelHandlerContext.close(); }
}
```

**改为：**
```java
public class ClientSession {
    private final int sessionId;
    private final Socket socket;
    private final OutputStream out;
    private final ClientSessionLifecycle lifecycle;
    private volatile boolean running = true;

    ClientSession(int sessionId, Socket socket, ClientSessionLifecycle lifecycle) {
        this.sessionId = sessionId;
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.lifecycle = lifecycle;
    }

    /** 直接写入用户 Socket OutputStream，无需中间队列 */
    public synchronized void sendToUser(byte[] bytes) {
        if (!running) return;
        bytes = lifecycle.beforeSendToUser(this, bytes);
        if (bytes != null) {
            out.write(bytes);
            lifecycle.afterSendToUser(this, bytes);
        }
    }

    public int getSessionId() { return sessionId; }
    public Socket getSocket() { return socket; }

    void close() {
        running = false;
        try { socket.close(); } catch (IOException ignored) {}
        lifecycle.closed(this);
    }
}
```

**设计说明：**
- `sendToUser` 加 `synchronized` 是因为数据来自 Transport 层的回复线程，可能并发写入（一个 TalkMessage 中包含多个 SessionBytes）
- 去掉 `sendToUserBytesQueue`，数据直接写入 Socket OutputStream，减少一层缓冲
- `close()` 中调用 `lifecycle.closed()`，与原来在 `ClientSessionManager.disposeClientSession()` 中调用的方式不同——改为在 Session 自身关闭时触发，更合理

---

### Step 3：重写 ClientSessionManager

**文件：** `run/src/main/java/org/wowtools/hppt/common/client/ClientSessionManager.java`

**改动说明：** 用 `ServerSocket` + 虚拟线程替代 Netty `ServerBootstrap`

**当前代码核心结构：**
```java
public class ClientSessionManager {
    private final ServerBootstrap serverBootstrap;  // ← 去掉
    private final List<Channel> channels;           // ← 改为 List<ServerSocket>
    private final Map<ChannelHandlerContext, ClientSession> clientSessionMapByCtx;  // ← 去掉

    // 内部类 SimpleHandler extends ByteToMessageDecoder
    //   channelActive() → 创建 ClientSession（通过 ClientBytesSender 回调）
    //   decode() → synchronized，从 ByteBuf 读数据，busy-wait 等 ClientSession 初始化
}
```

**改为：**
```java
public class ClientSessionManager implements AutoCloseable {
    private final Map<Integer, ClientSession> clientSessionMap = new ConcurrentHashMap<>();
    private final List<ServerSocket> serverSockets = new ArrayList<>();
    private final ClientSessionLifecycle lifecycle;
    private final ClientBytesSender clientBytesSender;
    private final int bufferSize;
    private volatile boolean running = true;

    ClientSessionManager(ClientSessionManagerBuilder builder) {
        lifecycle = builder.lifecycle;
        clientBytesSender = builder.clientBytesSender;
        bufferSize = builder.bufferSize;
    }

    public boolean bindPort(String localHost, int port) {
        try {
            ServerSocket ss = (localHost != null)
                ? new ServerSocket(port, 128, InetAddress.getByName(localHost))
                : new ServerSocket(port, 128);
            serverSockets.add(ss);
            Thread.startVirtualThread(() -> acceptLoop(ss, port));
            return true;
        } catch (Exception e) {
            log.error("Failed to bind port {}.", port, e);
            return false;
        }
    }

    private void acceptLoop(ServerSocket ss, int port) {
        while (running) {
            try {
                Socket userSocket = ss.accept();
                userSocket.setTcpNoDelay(true);
                userSocket.setSendBufferSize(bufferSize);
                userSocket.setReceiveBufferSize(bufferSize);
                onNewConnection(port, userSocket);
            } catch (IOException e) {
                if (running) log.warn("accept 异常", e);
            }
        }
    }

    private void onNewConnection(int port, Socket userSocket) {
        ClientBytesSender.SessionIdCallBack cb = new ClientBytesSender.SessionIdCallBack(userSocket) {
            @Override
            public void cb(int sessionId) {
                ClientSession clientSession = new ClientSession(sessionId, userSocket, lifecycle);
                clientSessionMap.put(sessionId, clientSession);
                lifecycle.created(clientSession);
                // 启动读取线程
                startReadThread(clientSession, port);
            }
        };
        clientBytesSender.connected(port, userSocket, cb);
    }

    private void startReadThread(ClientSession clientSession, int port) {
        Thread.startVirtualThread(() -> {
            try (InputStream in = clientSession.getSocket().getInputStream()) {
                byte[] buf = new byte[bufferSize];
                int n;
                while ((n = in.read(buf)) != -1) {
                    byte[] bytes = n < buf.length ? Arrays.copyOf(buf, n) : buf;
                    bytes = lifecycle.beforeSendToTarget(clientSession, bytes);
                    if (bytes != null) {
                        SessionBytes sessionBytes = new SessionBytes(clientSession.getSessionId(), bytes);
                        clientBytesSender.sendToTarget(clientSession, sessionBytes);
                        lifecycle.afterSendToTarget(clientSession, bytes);
                    }
                }
            } catch (IOException e) {
                // 连接断开
            } finally {
                disposeClientSession(clientSession, "连接关闭");
            }
        });
    }

    public void disposeClientSession(ClientSession clientSession, String type) {
        if (clientSessionMap.remove(clientSession.getSessionId()) != null) {
            clientSession.close();
            log.info("ClientSession {} close, type [{}]", clientSession.getSessionId(), type);
        }
    }

    public void close() {
        running = false;
        for (ServerSocket ss : serverSockets) {
            try { ss.close(); } catch (IOException ignored) {}
        }
        for (ClientSession session : clientSessionMap.values()) {
            session.close();
        }
    }
}
```

**关键改进：**
- 去掉 `synchronized decode()` 和 busy-wait
- 去掉 `clientSessionMapByCtx`（Netty Channel 映射）
- 用虚拟线程直接从 Socket InputStream 读取，不需要 Netty ByteToMessageDecoder
- 新建连接时先调用 `clientBytesSender.connected()`，回调中同步创建 ClientSession 并启动读取线程——不存在原来"session 尚未创建就开始收到数据"的竞态问题

---

### Step 4：重写 ClientSessionManagerBuilder

**文件：** `run/src/main/java/org/wowtools/hppt/common/client/ClientSessionManagerBuilder.java`

**改动说明：** 去掉 Netty EventLoopGroup 参数

**当前代码：**
```java
public class ClientSessionManagerBuilder {
    protected int bufferSize;
    protected EventLoopGroup bossGroup;   // ← 去掉
    protected EventLoopGroup workerGroup;  // ← 去掉
    protected ClientSessionLifecycle lifecycle;
    protected ClientBytesSender clientBytesSender;
    // setter 方法...
    public ClientSessionManager build() {
        if (bossGroup == null) bossGroup = NettyObjectBuilder.buildVirtualThreadEventLoopGroup(1);
        if (workerGroup == null) workerGroup = NettyObjectBuilder.buildVirtualThreadEventLoopGroup();
        ...
    }
}
```

**改为：**
```java
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
        if (bufferSize <= 0) bufferSize = 10240;
        if (lifecycle == null) throw new RuntimeException("lifecycle不能为空");
        if (clientBytesSender == null) throw new RuntimeException("clientBytesSender不能为空");
        return new ClientSessionManager(this);
    }
}
```

---

### Step 5：重写 ServerSession

**文件：** `run/src/main/java/org/wowtools/hppt/common/server/ServerSession.java`

**改动说明：** 用 `Socket` 替代 Netty `Channel`，去掉内部 `sendBytesQueue`，目标读取改为虚拟线程

**当前代码核心结构：**
```java
public class ServerSession {
    private final Channel channel;  // ← Netty Channel
    private final BufferPool<SessionBytes> sendBytesQueue;  // ← 去掉

    ServerSession(...) {
        // 启动虚拟线程从 sendBytesQueue 取数据，写入 channel
    }
    public void sendToTarget(SessionBytes sessionBytes) {
        // 加入 sendBytesQueue
    }
}
```

**改为：**
```java
public class ServerSession {
    private final LoginClientService.Client client;
    private final Socket socket;       // 目标连接
    private final OutputStream out;
    private final int sessionId;
    private final long sessionTimeout;
    private final ServerSessionLifecycle lifecycle;
    private volatile boolean running = true;
    private long activeTime;

    ServerSession(long sessionTimeout, int sessionId, LoginClientService.Client client,
                  ServerSessionLifecycle lifecycle, Socket socket) {
        this.sessionId = sessionId;
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.sessionTimeout = sessionTimeout;
        this.lifecycle = lifecycle;
        this.client = client;
        activeSession();
        client.addSession(this);
        // 启动从目标 Socket 读取数据的虚拟线程
        startReadThread();
    }

    /** 从目标 Socket 读取数据，放入 Client 的共享队列 */
    private void startReadThread() {
        Thread.startVirtualThread(() -> {
            try (InputStream in = socket.getInputStream()) {
                byte[] buf = new byte[65536];
                int n;
                while ((n = in.read(buf)) != -1) {
                    activeSession();
                    byte[] data = n < buf.length ? Arrays.copyOf(buf, n) : buf;
                    lifecycle.afterSendToUser(this, data); // 原有的 afterSendToUser 语义
                    SessionBytes sessionBytes = new SessionBytes(sessionId, data);
                    lifecycle.sendToClientBuffer(sessionBytes, client, null);
                }
            } catch (IOException e) {
                // 目标断开
            } finally {
                if (running) close();
            }
        });
    }

    /** 直接写入目标 Socket，无需中间队列 */
    public synchronized void sendToTarget(SessionBytes sessionBytes) {
        if (!running) return;
        byte[] bytes = sessionBytes.getBytes();
        bytes = lifecycle.beforeSendToTarget(this, bytes);
        if (bytes != null) {
            out.write(bytes);
            lifecycle.afterSendToTarget(this, bytes);
        }
    }

    public void activeSession() { activeTime = System.currentTimeMillis(); }
    public boolean isTimeout() { return System.currentTimeMillis() - activeTime > sessionTimeout; }
    public int getSessionId() { return sessionId; }
    public LoginClientService.Client getClient() { return client; }

    void close() {
        running = false;
        try { socket.close(); } catch (IOException ignored) {}
        lifecycle.closed(this);
    }
}
```

**设计说明：**
- `sendToTarget` 加 `synchronized`：数据来自 ServerTalker 解析路由，可能在同一批次中包含多个 SessionBytes
- `startReadThread` 在构造函数中启动，从目标 Socket 读取数据后直接放入 Client 的 sessionBytesQueue
- 去掉 `sendBytesQueue`，sendToTarget 直接写 Socket OutputStream

---

### Step 6：重写 ServerSessionManager

**文件：** `run/src/main/java/org/wowtools/hppt/common/server/ServerSessionManager.java`

**改动说明：** 用 `Socket` 连接目标替代 Netty Bootstrap

**当前代码核心结构：**
```java
public class ServerSessionManager {
    private final Bootstrap bootstrap;  // ← Netty Bootstrap
    // 内部类 SimpleHandler，channelRead() 中 future.get(30s) 阻塞

    public int createServerSession(Client client, String host, int port, long timeoutMillis) {
        // 用 Bootstrap 连接目标，CompletableFuture + future.get() 等待
    }
}
```

**改为：**
```java
public class ServerSessionManager implements AutoCloseable {
    private volatile boolean running = true;
    private final AtomicInteger sessionIdBuilder = new AtomicInteger();
    private final Map<Integer, ServerSession> serverSessionMap = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, ServerSession>> clientIdServerSessionMap = new ConcurrentHashMap<>();
    private final ServerSessionLifecycle lifecycle;
    private final long sessionTimeout;

    ServerSessionManager(ServerSessionManagerBuilder builder) {
        lifecycle = builder.lifecycle;
        sessionTimeout = builder.sessionTimeout;
        startTimeoutScanner();
    }

    public int createServerSession(LoginClientService.Client client, String host, int port, long timeoutMillis) {
        int sessionId = sessionIdBuilder.incrementAndGet();
        try {
            Socket socket = new Socket(host, port);
            socket.setTcpNoDelay(true);
            ServerSession serverSession = new ServerSession(
                sessionTimeout, sessionId, client, lifecycle, socket);
            serverSessionMap.put(sessionId, serverSession);
            clientIdServerSessionMap
                .computeIfAbsent(client.clientId, k -> new ConcurrentHashMap<>())
                .put(sessionId, serverSession);
            lifecycle.created(serverSession);
            return sessionId;
        } catch (IOException e) {
            log.error("连接目标 {}:{} 失败", host, port, e);
            return sessionId; // 返回 sessionId 但 session 不存在，触发 CloseSession 命令
        }
    }

    private void startTimeoutScanner() {
        Thread.startVirtualThread(() -> {
            while (running) {
                try { Thread.sleep(30000); } catch (InterruptedException ignored) {}
                for (ServerSession session : serverSessionMap.values()) {
                    if (session.isTimeout()) {
                        disposeServerSession(session, "session超时");
                    }
                }
            }
        });
    }

    public void disposeServerSession(ServerSession session, String type) {
        if (serverSessionMap.remove(session.getSessionId()) != null) {
            Map<Integer, ServerSession> map = clientIdServerSessionMap.get(session.getClient().clientId);
            if (map != null) map.remove(session.getSessionId());
            session.close();
            log.info("ServerSession {} close, type [{}]", session.getSessionId(), type);
        }
    }

    public Map<Integer, ServerSession> getServerSessionMapByClientId(String clientId) {
        return clientIdServerSessionMap.get(clientId);
    }

    public ServerSession getServerSessionBySessionId(int sessionId) {
        return serverSessionMap.get(sessionId);
    }

    public void setLastHeartbeatTime(long t) { /* 由子类/receiver 管理 */ }

    @Override
    public void close() {
        running = false;
        for (ServerSession session : serverSessionMap.values()) {
            session.close();
        }
    }
}
```

**关键改进：**
- 去掉 Netty Bootstrap + CompletableFuture + future.get(30s) 阻塞
- 直接 `new Socket(host, port)` 连接目标，简单直接
- 保留超时扫描虚拟线程
- 保留 `clientIdServerSessionMap` 用于按客户端 ID 查找 sessions

---

### Step 7：重写 ServerSessionManagerBuilder

**文件：** `run/src/main/java/org/wowtools/hppt/common/server/ServerSessionManagerBuilder.java`

**改动说明：** 去掉 Netty EventLoopGroup 参数

**当前代码：**
```java
public class ServerSessionManagerBuilder {
    protected EventLoopGroup group;  // ← 去掉
    protected ServerSessionLifecycle lifecycle;
    protected long sessionTimeout = 60000;
    // ...
    public ServerSessionManager build() {
        if (group == null) group = NettyObjectBuilder.buildEventLoopGroup();
        ...
    }
}
```

**改为：**
```java
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
        if (lifecycle == null) throw new RuntimeException("lifecycle不能为空");
        return new ServerSessionManager(this);
    }
}
```

---

### Step 8：简化 BytesUtil

**文件：** `run/src/main/java/org/wowtools/hppt/common/util/BytesUtil.java`

**改动说明：** 删除核心层不再使用的 Netty 依赖方法

**删除的方法：**
- `writeToChannelHandlerContext(ChannelHandlerContext, byte[])` — 被 Socket 直接写入替代
- `writeToChannel(Channel, Object)` — 同上
- `writeObjToChannel(Channel, Object)` — 同上
- `bytes2byteBuf(Channel, byte[])` — 核心 Session 不再用 ByteBuf
- `byteBuf2bytes(ByteBuf)` — 同上
- `waitChannelWritable(Channel, long)` — 不再需要
- `afterWrite(ChannelFuture, long)` — 不再需要

**保留的方法：**
- `splitBytes(byte[], int)` — 通用工具
- `merge(Collection<byte[]>)` — 通用工具
- `bytes2base64(byte[])` / `base642bytes(String)` — 加密相关
- `compress(byte[])` / `decompress(byte[])` — 压缩
- `bytesCollection2PbBytes(Collection<byte[]>)` / `pbBytes2BytesList(byte[])` — protobuf 相关

**注意：** 传输层（websocket、hppt 等）仍需使用 ByteBuf，那些方法在传输层代码中自行处理。如果传输层代码仍需这些工具方法，可以将它们移到传输层各自的 util 中。

---

### Step 9：适配 SC 侧 PortReceiver

**文件：** `run/src/main/java/org/wowtools/hppt/run/sc/common/PortReceiver.java`

**改动说明：** 适配新的 ClientBytesSender 接口和 ClientSession

**需修改的位置：**

1. **第 247 行 `buildClientBytesSender()` 匿名实现：**
```java
// 改前
public void connected(int port, ChannelHandlerContext ctx, SessionIdCallBack cb) {
    // ... ctx.hashCode() 用于日志
}
// 改后
public void connected(int port, Socket socket, SessionIdCallBack cb) {
    // ... socket.getRemoteSocketAddress() 用于日志
}
```

2. **第 209 行 `checkSessionInit()` 方法：**
```java
// 改前
entry.getValue().channelHandlerContext.close();
// 改后
try { entry.getValue().socket.close(); } catch (IOException ignored) {}
```

3. **第 161 行 `System.exit(0)` — 改为调用 `clientSessionService.exit()`：**
```java
// 改前
log.error("登录失败 {}", code);
System.exit(0);
// 改后
log.error("登录失败 {}", code);
clientSessionService.exit();
```

4. **去掉 `import io.netty.channel.ChannelHandlerContext;`**

---

### Step 10：适配 SS 侧 PortReceiver

**文件：** `run/src/main/java/org/wowtools/hppt/run/ss/common/PortReceiver.java`

**改动说明：** 适配新的 ServerSession 接口

**需检查的位置：**
- `LoginClientService.Client.addSession()` / `removeSession()` — 接口不变
- `ServerSession.sendToTarget()` — 从异步队列改为同步写入，接口签名变化需适配
- `ServerTalker.receiveClientBytes()` 中第 59 行 `severSession.sendToTarget(sessionByte)` — SessionBytes → 需要检查适配
- `ServerTalker.replyToClient()` — 不变（操作的是 Client 的共享队列）
- 超时扫描中对 ServerSession 的操作 — `isTimeout()`、`activeSession()` 接口不变

---

### Step 11：适配 ClientTalker 和 ServerTalker

**文件：**
- `run/src/main/java/org/wowtools/hppt/common/client/ClientTalker.java`
- `run/src/main/java/org/wowtools/hppt/common/server/ServerTalker.java`

**ClientTalker 改动：**

1. **`receiveServerBytes()` 第 180-203 行：** 去掉找不到 ClientSession 时的 busy-wait 虚拟线程
   - 新架构中 `connected()` 回调是同步创建 ClientSession 后才启动读取，不存在竞态
   - 如果找不到 ClientSession，直接丢弃数据（或发送 CloseSession 命令）

2. **去掉 `import io.netty.*` 相关引用**（如果有）

**ServerTalker 改动：**

1. **第 59 行 `severSession.sendToTarget(sessionByte)`：** 适配新签名
   - 原：`sendToTarget(SessionBytes)` 内部异步（入队列）
   - 新：`sendToTarget(SessionBytes)` 内部同步（直接写 Socket）
   - 可能需要在 ServerTalker 中 catch IOException，处理目标连接断开的情况

2. **第 202-217 行静态 BufferPool 和虚拟线程：** 保留（用于回调处理，与新架构兼容）
   - 但需注意 `sendAbleSessionBytesResultQueue` 是否有内存泄漏风险

---

### Step 12：检查传输层适配

**传输层文件**不需要大改，因为核心层重构不影响传输层接口。但需检查以下内容：

**SC 侧传输层**（`run/src/main/java/org/wowtools/hppt/run/sc/`）：
- 各传输实现继承 `ClientSessionService`，接口不变
- `ClientSessionService` 中的 `buildClientSessionLifecycle()` 引用了 `ClientSession`，接口不变
- 如果有代码引用 `ClientBytesSender.SessionIdCallBack` 中的 `channelHandlerContext`，需改为 `socket`

**SS 侧传输层**（`run/src/main/java/org/wowtools/hppt/run/ss/`）：
- 各传输实现继承 `ServerSessionService<CTX>`，接口不变
- `CTX` 泛型在各传输中仍然是 `ChannelHandlerContext`（传输层继续使用 Netty）
- 这些传输层的 `CTX` 与核心层的 Socket 无关，不需要改

**需检查的文件列表：**

| 文件 | 检查内容 |
|------|----------|
| `run/sc/common/ScUtil.java` | `createClientSessionManager()` builder 调用是否需适配 |
| `run/ss/common/SsUtil.java` | `createServerSessionManagerBuilder()` builder 调用是否需适配 |
| `run/ss/common/Receiver.java` | 接口定义是否受影响 |
| `run/sc/common/Receiver.java` | 接口定义是否受影响 |

---

### Step 13：更新 addons-kafka 插件

**文件：** `addons-kafka/src/main/java/org/wowtools/hppt/addons/kafka/`

**改动说明：** Kafka 插件不直接引用 Netty 核心层类型（`ClientBytesSender`、`ChannelHandlerContext` 等），它继承的是 `ClientSessionService` 和 `ServerSessionService`。

需检查：
- `KafkaClientSessionService` 和 `KafkaServerSessionService` 是否编译通过
- 如果 `ClientSessionService` 的接口有变化，需适配

预期：**无需修改或极少量修改**

---

### Step 14：删除 NettyObjectBuilder 在核心层的使用

**文件：** `run/src/main/java/org/wowtools/hppt/common/util/NettyObjectBuilder.java`

**改动说明：** 保留此文件（传输层仍需要），但核心层代码不应再 import 它。

验证方式：重构完成后，在以下文件中 grep `NettyObjectBuilder` 应无结果：
- `common/client/*.java`
- `common/server/*.java`（除 `ServerSessionLifecycle` 中的无关引用）
- `common/util/BytesUtil.java`

---

### Step 15：编译验证

**验证步骤：**
```bash
cd /mnt/d/IDEA_me/work/mygithub/hppt
mvn clean compile
```

确保：
1. 核心层文件不再 import 任何 `io.netty.*`（`ClientSession`、`ClientSessionManager`、`ServerSession`、`ServerSessionManager`）
2. 所有传输层实现编译通过
3. addons-kafka 编译通过

---

### Step 16：使用 POST 协议进行端到端功能验证

**目的：** 编译通过后，使用 POST 传输协议实际运行 SC + SS，验证核心层重构的正确性。POST 协议是所有传输方式中最简单的（SC 侧用 OkHttp，SS 侧用 Netty HTTP Server），适合作为首个验证对象。

**POST 协议数据流回顾：**
```
SC 侧 (PostClientSessionService):
  SendThread: sendQueue → bytesCollection2PbBytes → POST /s?c=cookie → 服务端
  ReplyThread: POST /r?c=cookie → 接收响应 → pbBytes2BytesList → receiveServerBytes()

SS 侧 (PostServerSessionService + NettyHttpServer):
  /s 路由: 收到 POST → pbBytes2BytesList → receiveClientBytes(ctx, sub)
  /r 路由: ctx.sendQueue → bytesCollection2PbBytes → HTTP 响应
```

**验证配置：**

SS 配置 (`run/src/test/resources/demo/post/ss.yml`)：
```yaml
type: post
port: 20871
clients:
  - user: user1
    password: 12345
```

SC 配置 (`run/src/test/resources/demo/post/sc.yml`)：
```yaml
type: post
clientUser: user1
clientPassword: 12345
maxSendBodySize: 1024000
post:
  serverUrl: "http://localhost:20871"
forwards:
  - localPort: 10022
    remoteHost: "127.0.0.1"
    remotePort: 22
```

**验证步骤：**

1. **启动 SS（服务端）：**
   ```bash
   cd run
   mvn exec:java -Dexec.mainClass="org.wowtools.hppt.run.ss.RunSs" -Dexec.args="ss src/test/resources/demo/post/ss.yml"
   ```
   或在 IDE 中直接运行 `RunSs.main()` 指定配置文件路径

2. **启动 SC（客户端）：**
   ```bash
   mvn exec:java -Dexec.mainClass="org.wowtools.hppt.run.sc.RunSc" -Dexec.args="sc src/test/resources/demo/post/sc.yml"
   ```
   或在 IDE 中直接运行 `RunSc.main()` 指定配置文件路径

3. **稳定传输验证：**
   通过代理端口 SSH 连接到 WSL，上传和下载 1.5GB 文件：
   ```bash
   # 上传
   scp -P 10022 /mydata/models/Qwen3-0.6B.gguf root@wsl:/tmp/test_upload.gguf

   # 下载
   scp -P 10022 root@wsl:/mydata/models/Qwen3-0.6B.gguf /tmp/test_download.gguf
   ```

   **成功标准：** 两个方向的传输均能完整完成，文件大小一致，无异常中断。不需要与重构前对比性能，稳定传完 1.5GB 即算验证通过。

4. **观察点：**
   - SC 和 SS 启动日志中无异常
   - 登录成功
   - 心跳正常
   - Session 创建和销毁正常
   - 传输过程中无报错、无中断

---

## 四、核心层与传输层的边界

重构后，核心层和传输层的职责划分如下：

```
┌──────────────────────────────────────────────────┐
│  核心层（Socket + 虚拟线程，无 Netty 依赖）         │
│                                                    │
│  ClientSession       - 持有用户 Socket             │
│  ClientSessionManager - ServerSocket 监听          │
│  ServerSession       - 持有目标 Socket             │
│  ServerSessionManager - 创建目标 Socket 连接       │
│  ClientTalker        - protobuf 编解码 + 加密      │
│  ServerTalker        - protobuf 编解码 + 加密      │
│  LoginClientService  - 客户端认证管理              │
│  ClientBytesSender   - 连接事件回调接口            │
└──────────────────────────────────────────────────┘
         ↕ byte[] 数据交换
┌──────────────────────────────────────────────────┐
│  传输层（保留 Netty / OkHttp / 文件系统等）         │
│                                                    │
│  ClientSessionService - SC 传输基类               │
│  ServerSessionService - SS 传输基类               │
│  post/websocket/hppt/rpost/rhppt/file 实现       │
└──────────────────────────────────────────────────┘
```

核心层通过 `ClientBytesSender` 接口与传输层交互：
- `connected(port, socket, cb)` — 新连接通知
- `sendToTarget(session, sessionBytes)` — 用户数据转发

传输层通过 `ClientTalker.buildSendToServerBytes()` / `ClientTalker.receiveServerBytes()` 编解码数据。

---

## 五、风险与注意事项

1. **并发安全**：`ClientSession.sendToUser()` 和 `ServerSession.sendToTarget()` 需要 synchronized，因为数据来自多路复用的传输通道
2. **Socket 关闭顺序**：关闭 Socket 时需确保 OutputStream flush 完成后再 close
3. **异常传播**：直接写入 Socket 时，IOException 应触发 session 关闭，不能吞掉
4. **传输层 ByteBuf 工具方法**：删除 `BytesUtil` 中的 ByteBuf 方法前，先 grep 确认传输层是否有使用，若有则移到传输层各自的 util 中
5. **ServerSessionLifecycle.sendToClientBuffer()**：原有默认实现调用 `client.addBytes()`，此行为保持不变
6. **ServerTalker.replyToClient()** 中的 `SendAbleSessionBytes.CallBack`：新架构中 ServerSession 不再有内部 BufferPool，callback 机制可能需要调整（若不再需要回调通知写入成功，可简化）

---

## 六、验证方案

1. **编译验证**：`mvn clean compile` 全模块通过
2. **功能验证**：使用 POST 协议启动 SC + SS，通过代理端口 `ssh root@wsl`，上传和下载 `/mydata/models/Qwen3-0.6B.gguf`（1.5GB），稳定传输完成即算成功（详见 Step 16）

---

*计划文件 — 等待确认后开始实施*
