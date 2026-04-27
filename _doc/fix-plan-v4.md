# HPPT v4.x 三大问题修复计划

> 基于 Netty→Socket+虚拟线程 重构后发现的系统性问题
> 涉及 synchronized 线程固定、对象生命周期可见性、网络断连处理三个维度

---

## 一、JDK 21 synchronized 固定虚拟线程

### 1.1 问题原理

JDK 21 中，虚拟线程在 `synchronized` 块内执行阻塞操作（I/O、wait、sleep）时，会被**固定（pin）到载体线程（carrier thread）**，不会自动卸载。如果多个虚拟线程同时进入同一个 synchronized 块，会耗尽有限的 carrier thread 池，导致整个应用挂起。

解决方式：将 `synchronized` 替换为 `ReentrantLock`。ReentrantLock 不触发 pinning。

### 1.2 涉及位置与风险等级

#### HIGH — synchronized 内含阻塞 I/O（必须改）

| # | 文件 | 行号 | 代码 | 风险说明 |
|---|------|------|------|----------|
| 1 | `common/client/ClientSession.java` | 35 | `synchronized sendToUser()` → `out.write()` | Socket 写入可能阻塞，pin carrier thread |
| 2 | `common/server/ServerSession.java` | 93 | `synchronized sendToTarget()` → `out.write()` | 同上 |
| 3 | `sc/hppt/HpptClientSessionService.java` | 59 | `synchronized(out) { FrameIo.writeFrame() }` | Socket 写入 |
| 4 | `sc/rhppt/RHpptClientSessionService.java` | 62 | `synchronized(out) { FrameIo.writeFrame() }` | 同上 |
| 5 | `ss/hppt/HpptServerSessionService.java` | 71 | `synchronized(out) { FrameIo.writeFrame() }` | 同上 |
| 6 | `ss/rhppt/RHpptServerSessionService.java` | 58 | `synchronized(out) { FrameIo.writeFrame() }` | 同上 |

#### MEDIUM — synchronized 内含 wait（建议改）

| # | 文件 | 行号 | 代码 | 风险说明 |
|---|------|------|------|----------|
| 7 | `ss/common/PortReceiver.java` | 141 | `synchronized(cell.clientActiveWatcher) { wait(10_000) }` | 等待 10 秒会 pin |
| 8 | `ss/common/PortReceiver.java` | 192 | `synchronized(cell.clientActiveWatcher) { wait(10_000) }` | 同上 |
| 9 | `sc/post/PostClientSessionService.java` | 119 | `synchronized(replyThreadEmptyLock) { wait(10_000) }` | 同上 |

#### LOW — synchronized 内无阻塞操作（可暂不改）

| # | 文件 | 行号 | 代码 |
|---|------|------|------|
| 10 | `sc/common/ClientSessionService.java` | 110 | `synchronized(this) { notify() }` |
| 11 | `sc/common/ClientSessionService.java` | 131 | `synchronized(this) { wait() }` |
| 12 | `sc/common/PortReceiver.java` | 140 | `synchronized(this) { dt 初始化 }` |
| 13 | `ss/common/PortReceiver.java` | 103 | `synchronized(ctxClientCellMap) { map操作 }` |
| 14 | `ss/common/PortReceiver.java` | 56 | `synchronized(clientActiveWatcher) { notifyAll() }` |
| 15 | `sc/post/PostClientSessionService.java` | 190,199 | `synchronized(replyThreadEmptyLock) { notify() }` |
| 16 | `common/server/LoginClientService.java` | 65,75 | `synchronized(sessions) { map操作 }` |

#### LOW — while(true) 内 synchronized（循环本身极短，风险低）

| # | 文件 | 行号 | 代码 |
|---|------|------|------|
| 17 | `sc/file/FileClientSessionService.java` | 36 | `synchronized(path) { while(true) {文件读取} }` |
| 18 | `ss/file/FileServerSessionService.java` | 47 | `synchronized(path) { while(true) {文件读取} }` |

### 1.3 修复方案

**HIGH 级别（#1~#6）**：将 `synchronized` 替换为 `ReentrantLock`

以 `ClientSession.sendToUser` 为例：
```java
// 改前
public synchronized void sendToUser(byte[] bytes) {
    // ... out.write(bytes)
}

// 改后
private final ReentrantLock sendLock = new ReentrantLock();
public void sendToUser(byte[] bytes) {
    sendLock.lock();
    try {
        // ... out.write(bytes)
    } finally {
        sendLock.unlock();
    }
}
```

以 `HpptClientSessionService.sendBytesToServer` 为例：
```java
// 改前
synchronized (out) {
    FrameIo.writeFrame(out, bytes, lengthFieldLength);
}

// 改后
private final ReentrantLock writeLock = new ReentrantLock();
// ...
writeLock.lock();
try {
    FrameIo.writeFrame(out, bytes, lengthFieldLength);
} finally {
    writeLock.unlock();
}
```

**MEDIUM 级别（#7~#9）**：用 `ReentrantLock` + `Condition` 替代 `synchronized` + `wait/notify`

```java
// 改前
synchronized (replyThreadEmptyLock) {
    replyThreadEmptyLock.wait(10_000);
}

// 改后
private final ReentrantLock replyLock = new ReentrantLock();
private final Condition replyCondition = replyLock.newCondition();
// ...
replyLock.lock();
try {
    replyCondition.await(10, TimeUnit.SECONDS);
} finally {
    replyLock.unlock();
}
```

**LOW 级别（#10~#18）**：暂不改动。这些 synchronized 块要么极短（notify/map操作），要么已用 ConcurrentHashMap 保护。可后续统一处理。

---

## 二、对象生命周期与状态可见性

### 2.1 字段可见性问题（缺少 volatile）

以下字段被多线程读写但缺少 `volatile` 声明，可能导致一个线程的写入对另一个线程不可见：

| # | 文件 | 字段 | 写入线程 | 读取线程 | 修复 |
|---|------|------|----------|----------|------|
| 1 | `ss/common/ServerSessionService.java:20` | `running` | exit() 线程 | accept循环/读取循环 | 加 `volatile` |
| 2 | `sc/common/PortReceiver.java:45` | `serverHeartbeatTime` | 心跳回调线程 | 心跳检测线程 | 加 `volatile` |
| 3 | `sc/common/PortReceiver.java:36` | `aesCipherUtil` | sendLoginCommand() 线程 | 多个线程 | 加 `volatile` |
| 4 | `common/util/RoughTimeUtil.java:11` | `timestamp` | 静态更新线程 | 所有 getTimestamp() 调用者 | 加 `volatile` |
| 5 | `sc/hppt/HpptClientSessionService.java:22-23` | `socket`, `out` | connectToServer() 虚拟线程 | sendBytesToServer() 线程 | 加 `volatile` |
| 6 | `ss/hppt/HpptServerSessionService.java:22` | `serverSocket` | init() 线程 | onExit() 线程 | 加 `volatile` |

### 2.2 exit() 幂等性问题

`ClientSessionService.exit()` 和 `ServerSessionService.exit()` 均未做幂等保护。如果被多次调用（心跳超时 + 网络异常同时触发），`receiver.exit()` 和 `doClose()`/`onExit()` 会被重复执行。

**修复方案**：用 `volatile boolean exited` 标志做一次性的退出保护。

```java
// ClientSessionService
private volatile boolean exited = false;

public void exit() {
    if (exchanged) return;
    exited = true;
    running = false;
    receiver.exit();
    try { doClose(); } catch (Exception e) { log.warn("doClose error", e); }
    synchronized (this) { this.notify(); }
}
```

同理应用于 `ServerSessionService.exit()`。

### 2.3 不可停止的虚拟线程

以下虚拟线程内部是 `while(true)` 循环，没有 `running` 标志控制退出，且 `InterruptedException` 被吞掉（continue）：

| # | 文件 | 行号 | 问题 |
|---|------|------|------|
| 1 | `common/util/RoughTimeUtil.java` | 15-21 | 静态初始化的虚拟线程，`InterruptedException` 被 `continue` 吞掉，线程永远不会停止 |
| 2 | `common/server/ServerTalker.java` | 206-217 | 静态初始化的分发线程，`Exception` 被 catch 后继续循环，无法停止 |

**修复方案**：

`RoughTimeUtil`：将 `continue` 改为 `break`（线程退出不影响功能，getTimestamp() 会返回最后一次更新的值）。
```java
// 改前
catch (InterruptedException ignored) { continue; }

// 改后
catch (InterruptedException ignored) { break; }
```

`ServerTalker`：加入 `running` 标志，或改为 `break`。
```java
// 改后：Exception 时 break
catch (Exception e) {
    log.warn("ServerTalker dispatcher error", e);
    break;
}
```

### 2.4 PostClientSessionService 死代码

`PostClientSessionService.replyThreadEmptySleep` 字段始终为 `false`（赋值后从未被设为 `true`），相关 wait/notify 逻辑为死代码。

**修复方案**：移除 `replyThreadEmptySleep` 字段及相关 wait/notify 逻辑，简化代码。

---

## 三、网络断连与重连处理

### 3.1 当前问题总览

当前各传输层在遇到网络异常时，行为不一致且多数不够健壮：

| 传输层 | 断连检测 | 重连能力 | 退出方式 |
|--------|----------|----------|----------|
| HPpt (SC) | Socket read 返回 -1 或 IOException | 无重连，调 `clientSessionService.exit()` | 优雅退出 |
| HPpt (SS) | 同上 | 无重连 | 优雅退出 |
| RHppt (SC) | 同上 | 无重连 | 优雅退出 |
| RHppt (SS) | 同上 | 无重连 | 优雅退出 |
| Post (SC) | HTTP 请求失败 | 无重连，连接池可能重试 | 退出 |
| Post (SS) | 长轮询超时 | 无重连 | 退出 |
| RPost (SC) | HTTP 请求失败 | 无重连 | 退出 |
| RPost (SS) | 长轮询超时 | 无重连 | 退出 |
| WebSocket | onClose/onError 回调 | 无重连 | 退出 |
| File | 文件读取失败 | 无重连 | 退出 |
| Kafka | Kafka consumer 异常 | 无重连 | 退出 |

### 3.2 需要统一的异常处理模式

各传输层的读/写方法中对异常的处理应该遵循统一模式：

```
1. IOException / SocketException → 网络断开
   → 日志 INFO 级别（非 WARN）
   → 调用 exit() 触发清理
   → 如果是 SC 侧且有重连需求，由上层（PortReceiver）决定是否重连

2. 其他 RuntimeException → 逻辑错误
   → 日志 WARN 级别 + 完整堆栈
   → 调用 exit()

3. InterruptedException → 主动关闭
   → break 退出循环
   → 不打 WARN 日志
```

### 3.3 Socket 关闭触发线程退出

当前较健壮的模式是通过关闭 Socket 来中断阻塞的 read() 调用。已正确实现的：
- `ClientSession` — 关闭 Socket，读取线程收到 IOException 后退出
- `ServerSession` — 同上

需要确认的：
- `HpptClientSessionService` — `doClose()` 是否关闭了 `socket`？如果只设 running=false 而不关 socket，读取线程会一直阻塞在 `FrameIo.readFrame()`
- `HpptServerSessionService` — `onExit()` 是否关闭了 `serverSocket`？

**修复方案**：所有 `doClose()`/`onExit()` 方法必须：
1. 设置 `running = false`
2. 关闭对应的 Socket/ServerSocket
3. 中断相关线程（如需要）

### 3.4 HPpt/RHppt 读写线程的退出

当前 HPpt/RHppt 的读写循环类似：
```java
while (running) {
    byte[] bytes = FrameIo.readFrame(in, lengthFieldLength); // 阻塞读取
    // ...
}
```

如果 `running` 被设为 `false`，但线程阻塞在 `readFrame()` 上，线程不会退出。
必须通过关闭 Socket 使 `readFrame()` 抛出 IOException 来退出。

**修复方案**：确保 `doClose()`/`onExit()` 中关闭 Socket，并捕获 IOException 后 break 而非 continue：
```java
while (running) {
    try {
        byte[] bytes = FrameIo.readFrame(in, lengthFieldLength);
        // ...
    } catch (IOException e) {
        if (!running) break; // 正常关闭
        log.info("连接断开: {}", e.getMessage());
        break; // 异常断开也退出
    }
}
```

### 3.5 心跳超时后的行为统一

`sc/common/PortReceiver` 中，心跳超时后调用 `clientSessionService.exit()` 然后自己 break 退出心跳循环。但 `PortReceiver.exit()` 也会调用 `receiver.exit()`，存在双重退出的可能（见 2.2 幂等性问题）。

**修复方案**：统一为 exit 幂等后，心跳超时 → 调用 `clientSessionService.exit()` → 自动触发 `PortReceiver.exit()`。心跳线程只需 break 自身循环。

---

## 四、实施顺序

建议按以下顺序实施，每步完成后运行 `mvn clean compile` 确认编译通过：

### 第一步：字段可见性修复（最小改动，最大收益）
1. `ServerSessionService.running` 加 `volatile`
2. `PortReceiver.serverHeartbeatTime` 加 `volatile`
3. `PortReceiver.aesCipherUtil` 加 `volatile`
4. `RoughTimeUtil.timestamp` 加 `volatile`
5. `HpptClientSessionService.socket`、`out` 加 `volatile`
6. `HpptServerSessionService.serverSocket` 加 `volatile`

### 第二步：exit() 幂等性
1. `ClientSessionService.exit()` 加 `exited` 标志
2. `ServerSessionService.exit()` 加 `exited` 标志

### 第三步：synchronized → ReentrantLock（HIGH 级别 #1~#6）
1. `ClientSession.sendToUser()` — 改为 ReentrantLock
2. `ServerSession.sendToTarget()` — 改为 ReentrantLock
3. `HpptClientSessionService.sendBytesToServer()` — 改为 ReentrantLock
4. `RHpptClientSessionService.sendBytesToServer()` — 改为 ReentrantLock
5. `HpptServerSessionService.sendBytesToClient()` — 改为 ReentrantLock
6. `RHpptServerSessionService.sendBytesToClient()` — 改为 ReentrantLock

### 第四步：synchronized → ReentrantLock（MEDIUM 级别 #7~#9）
1. `ss/common/PortReceiver` — `clientActiveWatcher` 的 wait/notifyAll → ReentrantLock + Condition
2. `PostClientSessionService` — `replyThreadEmptyLock` 的 wait/notify → ReentrantLock + Condition

### 第五步：不可停止线程修复
1. `RoughTimeUtil` — InterruptedException 改为 break
2. `ServerTalker` — 加入退出条件

### 第六步：网络断连处理统一
1. 检查并修复所有 `doClose()`/`onExit()` 确保关闭 Socket
2. 统一读写循环中的异常处理模式（IOException → break，InterruptedException → break）
3. 移除 `PostClientSessionService` 的死代码

### 第七步：编译验证
- `mvn clean compile` 确认无编译错误
- 全局搜索确认核心层不再有 synchronized + 阻塞I/O 的组合

---

## 五、改动文件清单

| 文件 | 改动类型 | 涉及步骤 |
|------|----------|----------|
| `common/client/ClientSession.java` | synchronized→ReentrantLock | 第三步 |
| `common/server/ServerSession.java` | synchronized→ReentrantLock | 第三步 |
| `sc/common/PortReceiver.java` | volatile 修复 | 第一步 |
| `sc/common/ClientSessionService.java` | exit 幂等 | 第二步 |
| `ss/common/ServerSessionService.java` | volatile + exit 幂等 | 第一步、第二步 |
| `sc/hppt/HpptClientSessionService.java` | volatile + ReentrantLock + 断连处理 | 第一步、第三步、第六步 |
| `sc/rhppt/RHpptClientSessionService.java` | ReentrantLock + 断连处理 | 第三步、第六步 |
| `ss/hppt/HpptServerSessionService.java` | volatile + ReentrantLock + 断连处理 | 第一步、第三步、第六步 |
| `ss/rhppt/RHpptServerSessionService.java` | ReentrantLock + 断连处理 | 第三步、第六步 |
| `ss/common/PortReceiver.java` | synchronized→ReentrantLock+Condition | 第四步 |
| `sc/post/PostClientSessionService.java` | synchronized→ReentrantLock+Condition + 清理死代码 | 第四步、第六步 |
| `common/util/RoughTimeUtil.java` | volatile + break | 第一步、第五步 |
| `common/server/ServerTalker.java` | break 退出 | 第五步 |
| `sc/file/FileClientSessionService.java` | 断连处理（如有） | 第六步 |
| `ss/file/FileServerSessionService.java` | 断连处理（如有） | 第六步 |

共计 **15 个文件**，按 7 个步骤分批实施。
