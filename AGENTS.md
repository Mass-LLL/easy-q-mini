# AGENTS.md — EasyQ 工程内核速览（给 AI 协作者）

修改本仓库前请先读本文件；详细设计与取舍在 `DESIGN.md`，冲突时以 DESIGN.md 为准。本文件是可执行摘要。

## 1. 这是什么

EasyQ 是微信小程序：老板建队列、亮二维码，客人扫码拿号，老板按序叫号。**零服务端**：所有状态由客户端 + 第三方 MQTT broker（哑管道）承担。

架构本质：**没有发号权威**。客人入队 = 给自己发一条 retained 消息；号码 = 每台设备对同一组入队消息做全序排序（`(t 升序, cid 字典序)`）后自己的下标 +1。排序是纯函数且全序确定 ⇒ 不需要仲裁者、幂等表、计数器落盘、重启恢复；老板离线时客人照样拿得到号。

为什么没有服务端（改架构前必读 DESIGN.md「目标与取舍」）：不承担 7×24 运维责任、不给后台域名备案、成本按用量而非常驻月费。**若将来必须引入服务端，既定形状是微信云开发——届时大部分自愈机制应删除而非保留。**

约定：代码注释与 UI 文案都是中文；新增用户可见文案必须中文。

## 2. 协议内核

Topic 前缀 `eq`，全部 QoS 1 + retained + MQTT 5 `Message Expiry Interval = 24h`：

| Topic | 发布者 | payload | 作用 |
| --- | --- | --- | --- |
| `eq/{sid}/m` | 老板 | `{n: 队列名, t}` | 队列名 |
| `eq/{sid}/j/{cid}` | 客人 | `{t}` | 入队（唯一事实来源） |
| `eq/{sid}/c` | 老板 | `{n: called, t}` | 叫号进度（绝对计数） |
| `eq/{sid}/n` | 客人 | `{n: 自己的号, t}` | 已取号人数（下界） |

- `sid` = 32 位大写 hex，只通过二维码传播，是 v1 **唯一**访问控制；`cid` = 16 位 hex，每个队列一个。
- 客人订阅 `m` + `c` + `j/+`；号码定稿后只留 `m` + `c`。老板只订 `m` + `c` + `n`（不订 `j/+`）。
- 两套坐标系：`rank` 是**可变集合上的下标**，`called` 是**绝对计数**。它们兼容的唯一前提是入队集合只增不删。

## 3. 内核机制总表：每条机制都是某次事故的防波堤

改任何一处之前，先确认你明白它在防什么；把机制改回去 = 把事故放回来（§4 的教训、§7 的测试都为此存在）。

| 机制 | 防的故障 | 位置 |
| --- | --- | --- |
| 定稿标记 `f`（`q` 只是缓存） | 扫完码 5 秒内锁屏 → 回放途中的中间号码被当终值永久钉死，两人撞号 | `queue.js` freezeRank / watchAll |
| 重发/重新取号必须换新 `cid` + 新 `t` | 旧消息其实已落在 broker（PUBACK 回来了只是自己没收到）→ 覆盖 = 集合中间插入，已定稿者不重算 → 撞号 | `queue.js` rejoin / scan |
| `settle()` 离线时不判定、只推迟；重连后 `rearmAll()` 重置静默窗口 | 断线被当成集合收敛，两头出错：回放中途断线把偏小号码永久定稿；页面开着时没网把已拿 1 号的票换 cid 挤到队尾 | `queue.js` settle / rearmAll |
| `called`/`total` 收端取 `max` | 单 retained 槽位后写覆盖：多老板并发叫号、慢半拍客人上报，晚到的小值把计数打回去 | `mqttc.js` route |
| `num()` / `count()` 数值校验（有限数；called/total 再收窄为非负整数、上限 `MAX_COUNT`） | `{"n":"abc"}` → NaN 毒化两端界面；`3.5` → 叫 4.5 号；`1e308` → 全队永久「已过号」 | `mqttc.js` route |
| topic **订上了才进 `filters`** | 每次新建连接被「connect 恢复订阅」和「watch 自身订阅」各订一遍，而每次成功订阅 broker 都重推 retained → 流量主成本翻倍 | `mqttc.js` watch |
| 订阅没有「彻底放弃」：快重试耗尽转慢重试直到订上 | 开页面时网络坏十几秒 → 永久零订阅，且横幅在下一次 publish 成功时自己消失、骗人说已连接 | `mqttc.js` watch / slowRetry |
| 单例连接 + 三态 `connect()` + `waiters` | client 还在但 mqtt.js 正在重连时另开连接 → 僵尸连接没人 end()、白烧会话分钟、真机失联 | `mqttc.js` connect |
| `teardown()` 串行 + `mine()` 事件过滤 | `end(true)` 异步完成前开新连接 → 两个 client 交叉引用模块级 socket 状态（wx transport 的连接状态在模块作用域） | `mqttc.js` teardown / connect |
| 订阅按 sid 记总集（`w.topics`），最后一个 watcher 走时按总集拆 | 同一 sid 多 watcher 时，第二个退订杀掉第一个的订阅并残留 filters 里的 topic | `mqttc.js` watch |
| `publishTimeoutMs` 覆盖「建连 + 等 PUBACK」 | 连接半开时 mqtt.js 要等 keepalive 才报错 → 按钮转圈一分钟 | `mqttc.js` publishRetained |
| 二维码在 `onReady` 绘制 + 重试 | `type="2d"` canvas 节点首屏前查不稳，失败只弹 toast、无重试 | `host.js` onReady / drawQr |
| host 的 `synced` 门禁（回放静默才解锁叫号） | `called` 不落盘，回放前 `called+1` 是抢跑，会把 c 槽位改小，`max` 合并救不了之后才订阅的人 | `host.js` sync |
| `n` 上报失败重试 + 老板端「重新统计」 | 人数下界丢到 0 → 「叫下一位」按钮被锁死，客人站在柜台前老板点不动 | `queue.js` report / `host.js` recount |
| `store.isExpired` 两端共用 | 本地窗口 3 天 ≠ broker TTL 24h：老板端对过期队列照常画码叫号，客人扫进没人叫号的队列 | `store.js` / `host.js` / `mine.js` |
| 回放合帧：setData 值没变不写、落盘挂 80ms 合帧 | 每条消息一次 setData + 全量存储扫描（大店 N=200，取号那一刻卡顿） | `queue.js` onState / `host.js` apply |
| `sid` 只接受 `wx.getRandomValues` | `Math.random` 内部状态只有 128 位，先后生成的 sid 可互推——唯一门禁静默降级 | `id.js` |
| sitemap disallow host 页 + 三个页面都不加 `onShareAppMessage` | sid 随页面收录（微信带参数收录）/ 转发卡片离开小程序 | `sitemap.json` / 各页面 |
| 重扫放行 over/ended 票（只拦「正在排」） | 办完事当天想再排一次、一家人共用一台手机 → 被自己的旧票锁死 3 天 | `queue.js` scan |

## 4. 演进史（为什么反复栽在同一类地方）

三轮审查的教训，按时间顺序：

1. **第一轮：主链路正确性。** 重扫被旧票拦死、二维码在 onLoad 画、`called` 无 max、payload 不校验、订阅失败无重试、publish 无超时、断线无感知、end/connect 竞争、多 watcher 记账。
2. **第二轮：「号码定稿」概念被一个 `q > 0` 表达。** 中间值被当终值（引入 `f`）；重连期间另开连接（引入三态 + waiters）；retained 回放被推两遍（引入「订上才进 filters」）；人数为 0 锁死叫号（引入上报重试 + 重新统计）；重发覆盖旧 cid（改为换新 cid）。
3. **第三轮：把「消息不来了」当成「集合已收敛」。** 断线与回放结束在代码里长得一模一样——settle 离线不判定、rearmAll 重置窗口；订阅重试没有终点；老板端补上「已结束」概念；回放期间的 setData/存储扫描合帧。

贯穿三轮的主线：**这套设计没有服务端兜底，一切自愈都建立在客户端正确观察集合之上**——所以「是否还订着」「连接是否活着」「看到的是不是中间值」这三问，是每一个新机制的必答题。

## 5. 限制与已接受的缺口（不要顺手「修」）

完整清单在 DESIGN.md「后续加固清单」，动代码前先读。要点：

- **P0**：broker 域名进「socket 合法域名」白名单需要 ICP 备案（免费 EMQX 海外域名大概率没有）——不通过则整个传输方案要重估；《用户隐私保护指引》需声明 `wx.scanCode`。
- **P1**：broker 凭证在客户端 = 公开（根治要 per-client JWT + 最小服务端）；免费额度按会话分钟/流量计费，大店流量随队列长度平方增长。
- **P2**：改本机时钟可插队（且连累他人撞号）；同队列客人可伪造 `c`/`n`；没有小程序码（入口多两步）；没有跨天队列。

**原则：这些是已记录的决策，不是 bug。** 不要在不相关改动里顺手"修"它们；要动，必须先改 DESIGN.md 的决策记录。

## 6. 工程结构与框架约定

- `utils/config.js` — broker 配置 + 全部时间/重试旋钮（测试靠覆写它们跑得快）。`PLACEHOLDER` 哨兵永远不能是真实值，否则 `isConfigured()` 反转为自认未配置。
- `utils/mqttc.js` — 单例 MQTT 客户端（§3）；`route()` 做校验与 max 合并。
- `utils/store.js` — 按天分 key 本地存储 + `isExpired`；客人票 `{s,n,cid,t,q,f}`，老板队列 `{s,n,t,p}`。
- `pages/queue` 客人端 / `pages/mine` 老板列表 / `pages/host` 老板详情（二维码、synced、ended、重新统计）。
- 框架：`componentFramework: "glass-easel"`、`style: "v2"`、`lazyCodeLoading: "requiredComponents"`；无组件、无 weui；共享样式在 `app.wxss`，页面 `.wxss` 只放页面自己的。
- `pages/host/host.wxss` 的 canvas 是 `220px`，必须与 `host.js` 传给 `qr.drawTo` 的尺寸一致。
- **页面生命周期 = 连接生命周期**：onShow 订阅、onHide/onUnload 退订 + `mqttc.disconnect()`（会话分钟按连接时长计费）。

## 7. 依赖与 vendor 补丁

- `libs/mqtt.js`、`libs/qrcode.js` 直接入库（绕开「构建 npm」对 node 内置模块的解析）。
- **`libs/mqtt.js` 打了两个补丁**：① 强制注册 `wx`/`wxs` transport（mqtt.js 只在认为自己是 webpack 产物时才注册）；② 修 `socketTask.send({data: chunk.buffer})` 丢 `byteOffset` 的发送 bug（否则 CONNECT 报文多出 3 个填充字节，broker 解不出协议名，表现是「MQTTX 连得上、小程序一直超时」）。**升级必须 `npm run vendor:mqtt`，禁止直接 cp 覆盖。**
- 不要用 `global.__webpack_require__` 之类全局对象技巧"修复"——小程序逻辑层的 global 不是真正的全局对象，任何依赖全局语义的方案都不可靠。
- `mqtt` / `qrcode-generator` 在 devDependencies（只用于升级时重拷，别让「构建 npm」碰它们）；只有 `dayjs` 走 npm + 构建 npm。

## 8. 测试

- `npm test`：logic（rank/store + 逐字节校验 CONNECT 报文、钉住两个 vendor 补丁）、protocol（老板 + 三客人端到端，**改 topic 契约或排序规则必须扩展它**）、resilience（健壮性路径）。
- `test/harness.js` 提供内存 broker 与故障注入：`failSubs`（订阅被拒）、`stallPublish`（PUBACK 不来）、`muteJoins`（retain 但不投递）、`failConns`（建连失败）、`skipReplay`（回放只到一半）、`dropAll`/`reviveAll`（断线再恢复，即 mqtt.js 自身重连的形状）、`canvas.failTries`（canvas 节点未就绪）。
- 每台「设备」= 清 `require.cache` + 换 storage（`mqttc` 是进程级单例）；时间旋钮从 `utils/config.js` 覆写。
- 两个异步测试文件有退出哨兵：promise 永不 settle 会让 node 退出 0 假绿，新测试沿用。
- 修回归性 bug 的流程：先用故障注入写出能复现的用例，再修，再把修复改回去确认用例变红（变异验证）。

## 9. 安全边界

`sid` 不可猜是唯一访问控制。三处硬约束：`sitemap.json` disallow `pages/host/host`（路径带 `?s={sid}`）；三个页面都不加 `onShareAppMessage`；`utils/id.js` 的 sid 只接受强随机源。`route()` 的 payload 校验（`num`/`count`）不可放宽。

## 10. DO / DON'T

**DO**
- 改设计前先读 DESIGN.md 对应章节；改协议或排序后扩展 `test/protocol.test.js`
- 新机制先过三问：「断线时它成立吗」「慢半拍/多设备并发时它成立吗」「看到的是中间值吗」
- 注释保持中文、简洁；长论证放 DESIGN.md 并指路

**DON'T**
- 不要引入服务端（除非重估 DESIGN.md 的成本模型）
- 不要顺手修「后续加固清单」里的条目
- 不要直接 cp 覆盖或手工编辑 `libs/mqtt.js`
- 不要给页面加转发入口、不要动 sitemap 的 disallow、不要缩短/记录 sid
- 不要移除：订阅慢重试、`max` 合并、数值校验、`f` 定稿判据、`synced` 门禁、离线不判定——每一处都有对应的血案（§3）
