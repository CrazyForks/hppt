# 配置文件说明

本文档以当前代码中的配置类为准：

- 客户端：`run/src/main/java/org/wowtools/hppt/run/sc/pojo/ScConfig.java`
- 服务端：`run/src/main/java/org/wowtools/hppt/run/ss/pojo/SsConfig.java`
- 公共配置：`run/src/main/java/org/wowtools/hppt/common/util/CommonConfig.java`

说明：

- 当前已移除 relay / 中继模式，文档中的配置均为普通模式
- 未识别字段会被忽略，但建议尽量只保留本文档中的有效字段
- `type` 既可以是内置类型，也可以是自定义插件类名

## 公共配置

客户端与服务端都支持：

```yaml
# 是否启用内容加密，默认 true
enableEncrypt: true
```

## 客户端 sc.yml

### 顶层字段

```yaml
# 内置类型：websocket / post / hppt / rhppt / rpost / file
# 也可以填写自定义 ClientSessionService 实现类名
type: post

# 客户端用户名，每个 sc 进程应使用唯一 user
clientUser: user1
clientPassword: 12345

# 插件目录，默认是程序根目录下的 addons
addonsPath: ./addons

# 客户端 netty workerGroup 线程数，默认 0，表示按 CPU 数动态计算
workerGroupNum: 0

# 默认绑定到本机哪个 IP；若为空则绑定到 0.0.0.0
localHost: 127.0.0.1

# 限制每次向服务端发送的最大包体，默认 10 MiB
maxSendBodySize: 10485760

# 生命周期实现类，全类名；为空则使用默认实现
lifecycle: com.example.MyClientLifecycle

# 心跳周期，毫秒；大于 0 时定期向服务端发心跳，默认 120000
heartbeatPeriod: 120000

# 接收服务端数据的内部队列大小，默认 2048
receiveQueueSize: 2048

# 整个 service 退出后的外层重启等待时间，默认 1000
restartDelayMillis: 1000

# 传输层断开后的首次重连等待时间，默认 1000
transportReconnectBaseDelayMillis: 1000

# 传输层断开后的最大重连等待时间，默认 15000
transportReconnectMaxDelayMillis: 15000

# 传输层重连等待抖动，默认 300
transportReconnectJitterMillis: 300
```

### 端口映射

```yaml
forwards:
  - localPort: 10022
    remoteHost: 127.0.0.1
    remotePort: 22

    # 可选，覆盖顶层 localHost
    localHost: 127.0.0.1
```

### `post`

```yaml
type: post
post:
  # 服务端 HTTP 地址，例如 http://host:20871 或 nginx 代理地址
  serverUrl: http://example.com:20871

  # 人为增加一个发送等待时间，单位毫秒，默认 5
  sendSleepTime: 5
```

### `websocket`

```yaml
type: websocket
websocket:
  # 必须使用 ws:// 或 wss://
  serverUrl: ws://example.com:20871

  # 发送 websocket ping 的周期，毫秒；小于等于 0 则不发送，默认 30000
  pingInterval: 30000

  # 预留字段，默认 0
  workerGroupNum: 0
```

### `hppt`

```yaml
type: hppt
hppt:
  host: 127.0.0.1
  port: 20871

  # 长度字段字节数，支持 1/2/3/4，默认 3
  lengthFieldLength: 3

  # 预留字段，默认 0
  workerGroupNum: 0
```

### `rhppt`

```yaml
type: rhppt
rhppt:
  # 本机监听端口，等待服务端反向连接
  port: 20871

  # 长度字段字节数，支持 1/2/3/4，默认 3
  lengthFieldLength: 3
```

### `rpost`

```yaml
type: rpost
rpost:
  # 本机监听 HTTP 端口，等待服务端通过 HTTP 反向访问
  port: 20871

  # 等待真实端口返回数据的毫秒数，默认 30000
  waitResponseTime: 30000

  # 回复前的人为延迟，默认 0
  replyDelayTime: 0

  # 预留字段
  bossGroupNum: 1
  workerGroupNum: 0
```

### `file`

```yaml
type: file
file:
  fileDir: /path/to/shared/dir
```

## 服务端 ss.yml

### 顶层字段

```yaml
# 内置类型：websocket / post / hppt / rhppt / rpost / file
# 也可以填写自定义 ServerSessionService 实现类名
type: post

# 插件目录，默认是程序根目录下的 addons
addonsPath: ./addons

# 服务端监听端口
port: 20871

# 服务端心跳超时，毫秒；小于等于 0 表示不启用
heartbeatTimeout: -1

# 新建真实连接时的超时时间，默认 30000
initSessionTimeout: 30000

# session 空闲检查周期相关超时，默认 300000
sessionTimeout: 300000

# 服务端缓存队列大小，默认 2048
messageQueueSize: 2048

# 每次向客户端返回的最大包体，默认 10 MiB
maxReturnBodySize: 10485760

# 生命周期实现类，全类名；为空则使用默认实现
lifecycle: com.example.MyServerLifecycle

# 密码最大尝试次数，默认 5
passwordRetryNum: 5

# 整个 service 退出后的外层重启等待时间，默认 1000
restartDelayMillis: 1000

# 传输层断开后的首次重连等待时间，默认 1000
transportReconnectBaseDelayMillis: 1000

# 传输层断开后的最大重连等待时间，默认 15000
transportReconnectMaxDelayMillis: 15000

# 传输层重连等待抖动，默认 300
transportReconnectJitterMillis: 300
```

### 管理接口

`ss` 可选开启本机 RESTful 管理接口。`management.port <= 0` 时关闭此特性；管理端口绑定失败会导致进程启动失败，避免进程存活但管理面不可用。

详细接口说明见 [`management-api.md`](management-api.md)。

```yaml
management:
  # <=0 表示关闭管理接口，默认 0
  port: 0

  # 默认仅监听本机
  host: 127.0.0.1

  # 可选 Bearer token；host 不是 localhost/127.0.0.1/::1 时必须配置
  token: ""
```

启用示例：

```yaml
management:
  host: 127.0.0.1
  port: 19091
  token: "change-me"
```

接口：

- `GET /api/v1/health`：健康状态，`UP`/`DEGRADED` 返回 HTTP 200，`STARTING`/`DOWN` 返回 HTTP 503
- `GET /api/v1/status`：进程、配置、当前 service、客户端与 session 明细
- `GET /api/v1/sessions`：活跃目标 TCP session 列表
- `GET /api/v1/clients`：已登录客户端列表
- `POST /api/v1/restart`：重启当前业务 service，不退出 JVM；管理端口保持可用
- `POST /api/v1/stop`：正常停止业务 service 并退出 `ss` 进程
- `POST /api/v1/stop?force=true`：强制执行 `System.exit(0)`

配置 `token` 后，请求需携带：

```bash
curl -H 'Authorization: Bearer change-me' http://127.0.0.1:19091/api/v1/health
```

### 允许的客户端

```yaml
clients:
  - user: user1
    password: 12345
  - user: user2
    password: 112233
```

### `post`

```yaml
type: post
post:
  # 等待真实端口返回数据的毫秒数，默认 10000
  waitResponseTime: 10000

  # 回复前的人为延迟，默认 0
  replyDelayTime: 0

  # 预留字段
  bossGroupNum: 1
  workerGroupNum: 0
```

### `websocket`

```yaml
type: websocket
websocket:
  # 预留字段
  bossGroupNum: 1
  workerGroupNum: 0
```

### `hppt`

```yaml
type: hppt
hppt:
  # 长度字段字节数，支持 1/2/3/4，默认 3
  lengthFieldLength: 3

  # 预留字段
  bossGroupNum: 1
  workerGroupNum: 0
```

### `rhppt`

```yaml
type: rhppt
rhppt:
  # 指向客户端监听端
  host: 127.0.0.1
  port: 20871

  # 长度字段字节数，支持 1/2/3/4，默认 3
  lengthFieldLength: 3
```

### `rpost`

```yaml
type: rpost
rpost:
  # 指向客户端监听的 HTTP 地址
  serverUrl: http://example.com:20871
```

### `file`

```yaml
type: file
file:
  fileDir: /path/to/shared/dir
```

## 关于重启与重连

推荐区分两个概念：

- `restartDelayMillis`
  - 整个 service 已经退出，由外层重新拉起时等待多久
- `transportReconnect*`
  - service 还活着，只是传输层断开时的局部重连退避

例如：

```yaml
restartDelayMillis: 1000
transportReconnectBaseDelayMillis: 1000
transportReconnectMaxDelayMillis: 60000
transportReconnectJitterMillis: 500
```

这表示：

- service 真退出时，1 秒后由外层重新启动
- transport 断开时按 1s、2s、4s... 的指数退避重试，直到上限 60s
