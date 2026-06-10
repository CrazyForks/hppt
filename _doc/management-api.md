# ss 管理接口

本文档说明 `ss` 服务端 RESTful 管理接口的配置、鉴权、响应字段和各接口行为。管理接口仅用于运行期观测与控制 `ss` 进程，不参与业务转发链路。

## 开启方式

在 `ss.yml` 中配置 `management`：

```yaml
management:
  # <=0 表示关闭管理接口，默认 0
  port: 19091

  # 默认 127.0.0.1，建议保持仅本机监听，再由 ssh tunnel 或内网网关访问
  host: 127.0.0.1

  # 可选，鉴权token Bearer token；host 不是 localhost/127.0.0.1/::1 时必须配置
  token: "change-me"
```

规则：

- `management.port <= 0` 时不启动管理接口。
- `management.host` 为空时按 `127.0.0.1` 处理。
- `management.host` 不是 `localhost`、`127.0.0.1`、`::1` 时，`management.token` 不能为空，否则 `ss` 启动失败。
- 管理端口绑定失败会导致 `ss` 启动失败，不会静默忽略。
- 管理接口使用 JDK 内置 HTTP server，业务 service 重启时管理端口保持可用。

## 鉴权

如果 `management.token` 为空，则不做 HTTP 鉴权。

如果配置了 `management.token`，所有接口都必须携带：

```http
Authorization: Bearer change-me
```

示例：

```bash
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/health
```

鉴权失败返回：

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json;charset=UTF-8
```

```json
{"error":"unauthorized"}
```

请求方法不匹配返回：

```http
HTTP/1.1 405 Method Not Allowed
Content-Type: application/json;charset=UTF-8
```

```json
{"error":"method not allowed"}
```

## 状态说明

`health.status` 取值：

- `UP`：service 已启动且处于运行状态。
- `DEGRADED`：service 仍在运行，但健康检查发现异常迹象；当前实现中，开启 `heartbeatTimeout` 且存在活跃客户端时，如果心跳超时会进入此状态。
- `STARTING`：service 尚未就绪。
- `DOWN`：service 已停止、正在停止、启动失败，或收到停止请求。

`health.phase` 取值：

- `STARTING`：外层正在创建或启动当前 service。
- `RUNNING`：当前 service 已创建并进入运行阶段。
- `STOPPING`：收到停止请求，正在退出。
- `STOPPED`：当前 service 已停止，但外层可能随后按 `restartDelayMillis` 重新拉起。
- `START_FAILED`：当前启动尝试失败。

HTTP 状态码：

- `UP` / `DEGRADED` 返回 `200`。
- `STARTING` / `DOWN` 返回 `503`。

时间字段均为毫秒级 Unix timestamp。

## GET /api/v1/health

查询简要健康状态。

请求：

```bash
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/health
```

响应示例：

```json
{
  "status": "UP",
  "phase": "RUNNING",
  "type": "hppt",
  "servicePort": 19071,
  "restartCount": 1,
  "stopRequested": false,
  "processStartTime": 1781009294343,
  "serviceRunning": true,
  "lastReadyTime": 1781009294406,
  "lastExitReason": "running",
  "lastHeartbeatTime": 1781009294392,
  "activeClientCount": 0,
  "activeSessionCount": 0,
  "transportReconnectCount": 0
}
```

字段：

- `status`：健康状态，见“状态说明”。
- `phase`：当前生命周期阶段，见“状态说明”。
- `type`：当前 `ss` 协议类型，例如 `hppt`、`post`、`websocket`、`rhppt`、`rpost`、`file`。
- `servicePort`：业务 service 监听端口。
- `restartCount`：外层 `RunSs` 已启动 service 的次数；首次启动为 `1`。
- `stopRequested`：是否已收到管理停止请求。
- `processStartTime`：`ss` 进程启动时间。
- `serviceRunning`：当前 service 是否仍处于 running 状态。
- `lastReadyTime`：当前 service 最近一次就绪时间；未就绪时通常为 `-1`。
- `lastExitReason`：当前 service 最近一次退出原因。
- `lastHeartbeatTime`：服务端最近一次收到客户端心跳的时间。
- `activeClientCount`：当前已登录或仍保留的客户端连接数。
- `activeSessionCount`：当前活跃目标 TCP session 数。
- `transportReconnectCount`：当前 service 记录到的传输层重连次数。
- `lastStartupError`：仅当最近启动失败时出现。

## GET /api/v1/status

查询完整状态。该接口包含 `health` 的所有字段，并额外返回启动/停止时间、运行配置、客户端与 session 快照。

请求：

```bash
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/status
```

响应示例：

```json
{
  "status": "UP",
  "phase": "RUNNING",
  "type": "hppt",
  "servicePort": 19071,
  "restartCount": 1,
  "stopRequested": false,
  "processStartTime": 1781009294343,
  "serviceRunning": true,
  "lastReadyTime": 1781009294406,
  "lastExitReason": "running",
  "lastHeartbeatTime": 1781009294392,
  "activeClientCount": 1,
  "activeSessionCount": 1,
  "transportReconnectCount": 0,
  "lastServiceStartAttemptTime": 1781009294383,
  "lastServiceStoppedTime": -1,
  "config": {
    "type": "hppt",
    "port": 19071,
    "heartbeatTimeout": -1,
    "initSessionTimeout": 30000,
    "sessionTimeout": 300000,
    "messageQueueSize": 2048,
    "maxReturnBodySize": 10485760,
    "restartDelayMillis": 1000,
    "transportReconnectBaseDelayMillis": 1000,
    "transportReconnectMaxDelayMillis": 15000,
    "transportReconnectJitterMillis": 300
  },
  "hasActiveClient": true,
  "clients": [
    {
      "clientId": "user1",
      "running": true,
      "active": true,
      "sessionCount": 1,
      "pendingCommandCount": 0,
      "pendingSessionBytesCount": 0,
      "pendingReceiveBytesCount": 0,
      "ctx": "Socket[addr=/127.0.0.1,port=57616,localport=19071]"
    }
  ],
  "sessions": [
    {
      "sessionId": 1,
      "clientId": "user1",
      "running": true,
      "activeTime": 1781009786463,
      "targetAddress": "/127.0.0.1:22",
      "bytesFromTarget": 1024,
      "bytesToTarget": 128,
      "needCheckActive": false,
      "timeout": false
    }
  ]
}
```

注意：

- `config` 不包含客户端密码和管理 token。
- `clients` 和 `sessions` 是瞬时快照，遍历并发容器时允许弱一致性；它用于运维观测，不保证与业务线程同一时刻严格一致。

## GET /api/v1/clients

查询当前客户端快照。

请求：

```bash
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/clients
```

响应示例：

```json
{
  "clients": [
    {
      "clientId": "user1",
      "running": true,
      "active": true,
      "sessionCount": 1,
      "pendingCommandCount": 0,
      "pendingSessionBytesCount": 0,
      "pendingReceiveBytesCount": 0,
      "ctx": "Socket[addr=/127.0.0.1,port=57616,localport=19071]"
    }
  ]
}
```

字段：

- `clientId`：客户端登录用户。
- `running`：该客户端 cell 的收发线程是否仍运行。
- `active`：客户端是否被判定为活跃。
- `sessionCount`：该客户端当前关联的目标 TCP session 数。
- `pendingCommandCount`：待发送给客户端的控制命令数量。
- `pendingSessionBytesCount`：待发送给客户端的目标端返回数据数量。
- `pendingReceiveBytesCount`：客户端发来但服务端尚未处理的数据包数量。
- `ctx`：传输层连接上下文，仅用于排查。

## GET /api/v1/sessions

查询当前目标 TCP session 快照。

请求：

```bash
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/sessions
```

响应示例：

```json
{
  "sessions": [
    {
      "sessionId": 1,
      "clientId": "user1",
      "running": true,
      "activeTime": 1781009786463,
      "targetAddress": "/127.0.0.1:22",
      "bytesFromTarget": 1024,
      "bytesToTarget": 128,
      "needCheckActive": false,
      "timeout": false
    }
  ]
}
```

字段：

- `sessionId`：服务端目标 TCP session ID。
- `clientId`：该 session 所属客户端。
- `running`：目标 TCP session 是否仍运行。
- `activeTime`：最近活跃时间。
- `targetAddress`：服务端实际连接的目标地址。
- `bytesFromTarget`：从目标端读取并发往客户端的累计字节数。
- `bytesToTarget`：从客户端写入目标端的累计字节数。
- `needCheckActive`：是否已到需要向客户端确认 session 存活的时间。
- `timeout`：是否达到超时关闭条件。

## POST /api/v1/restart

重启当前业务 service，不退出 JVM，管理端口保持可用。该接口用于修复业务 service 卡死、传输状态异常、协议端口重绑等问题。

请求：

```bash
curl -X POST -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/restart
```

成功响应：

```http
HTTP/1.1 202 Accepted
Content-Type: application/json;charset=UTF-8
```

```json
{"accepted":true,"message":"restart requested"}
```

当前没有可重启的 running service 时：

```http
HTTP/1.1 409 Conflict
Content-Type: application/json;charset=UTF-8
```

```json
{"accepted":false,"message":"no running service"}
```

行为：

- 管理接口线程调用当前 service 的 `exit("admin restart")`。
- `RunSs` 外层循环观察到当前 service 退出后，按 `restartDelayMillis` 等待，然后重新创建 service。
- 管理 HTTP server 不会关闭，重启期间仍可访问 `health/status`。
- `restartCount` 会在下一次 service 启动时增加。

## POST /api/v1/stop

正常停止 `ss` 进程。该接口会停止当前业务 service，并让 `RunSs` 外层循环退出。

请求：

```bash
curl -X POST -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/stop
```

响应：

```http
HTTP/1.1 202 Accepted
Content-Type: application/json;charset=UTF-8
```

```json
{"accepted":true,"force":false,"message":"stop requested"}
```

行为：

- 设置 `stopRequested=true`。
- 调用当前 service 的 `exit("admin stop")`。
- 当前 service 停止后，外层循环不再重启。
- 管理 HTTP server 随 `RunSs` 退出被关闭。

## POST /api/v1/stop?force=true

强制停止 `ss` 进程。该接口用于正常 stop 无法退出、进程处于异常状态、外部 watchdog 必须确认进程消失的场景。

请求：

```bash
curl -X POST -H 'Authorization: Bearer change-me' \
  'http://127.0.0.1:19091/api/v1/stop?force=true'
```

响应：

```http
HTTP/1.1 202 Accepted
Content-Type: application/json;charset=UTF-8
```

```json
{"accepted":true,"force":true,"message":"force stop requested"}
```

行为：

- 管理接口先返回 JSON 响应。
- 随后触发 `System.exit(0)`。
- 该路径不会等待业务 service 自然退出。

## 运维建议

- 默认只监听 `127.0.0.1`，通过 SSH tunnel 访问管理端口，例如 `ssh -L 19091:127.0.0.1:19091 server`。
- 如果必须监听 `0.0.0.0`，必须配置强 token，并建议放在内网或反向代理鉴权后面。
- 外部 watchdog 不应只检查 JVM 进程存在，还应检查 `GET /api/v1/health`。
- 自动化脚本调用 `restart` 后，应轮询 `health.status=UP` 且 `restartCount` 增加，再判定恢复完成。
- `force=true` 是最后手段，适合无法自然退出时使用。

## 常见排查命令

```bash
# 健康检查
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/health

# 查看完整状态
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/status

# 查看客户端
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/clients

# 查看活跃 session
curl -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/sessions

# 重启业务 service
curl -X POST -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/restart

# 正常停止进程
curl -X POST -H 'Authorization: Bearer change-me' \
  http://127.0.0.1:19091/api/v1/stop

# 强制退出进程
curl -X POST -H 'Authorization: Bearer change-me' \
  'http://127.0.0.1:19091/api/v1/stop?force=true'
```
