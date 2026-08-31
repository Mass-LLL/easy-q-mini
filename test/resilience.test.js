// 客户端健壮性：二维码重画、订阅重试、发布超时、断线横幅、关连接串行化、多 watcher 记账，
// 以及「断线 / 没网 → 恢复」这一整类路径。
// 这些都是「多设备 + 坏网络 + 坏 payload」路径，protocol.test.js 的主链路走不到。
const assert = require('assert')
const { P, broker, canvas, freshUtils, makeDevice, tick } = require('./harness.js')

const SID = 'D'.repeat(32)
const sleep = ms => new Promise(r => setTimeout(r, ms))

// 一份干净的 mqttc（进程级单例，必须清缓存才能重来）
const freshMqttc = tune => {
  const config = freshUtils()
  if (tune) Object.assign(config, tune)
  return require(P('utils/mqttc.js'))
}

// 一台带队列记录的老板设备
const hostDevice = tune => {
  const host = makeDevice('pages/host/host.js', tune)
  host.__activate()
  require(P('utils/store.js')).saveQueue({ s: SID, n: '测试队列', t: Date.now() })
  return host
}

// 哨兵：Promise 永远不 settle 时 node 会静默退出并返回 0，那样 npm test 会假绿
let finished = false
process.on('exit', code => {
  if (finished || code !== 0) return
  console.error('\n失败: 没跑到结尾就退出了 —— 大概有个 Promise 永远不会 settle')
  process.exitCode = 1
})

;(async () => {
  // ---------- A2 二维码：canvas 2d 节点没就绪要重画 ----------
  broker.reset()
  canvas.failTries = 3
  canvas.queries = 0
  canvas.rects = 0
  const qrHost = hostDevice({ qrRetries: 5, qrRetryMs: 5 })
  qrHost.onLoad({ s: SID })
  assert.strictEqual(canvas.queries, 0, 'onLoad 不应查 canvas 节点：首屏还没渲染')
  qrHost.onReady()
  await sleep(80)
  assert.ok(canvas.queries >= 4, '节点拿不到时应重试，实际查询次数: ' + canvas.queries)
  assert.ok(canvas.rects > 100, '重试成功后应真的把二维码画出来，实际格子数: ' + canvas.rects)
  assert.strictEqual(canvas.width, 220 * 2, 'canvas 尺寸应是 220 逻辑像素 × dpr')
  qrHost.onUnload()
  console.log('OK 二维码：onLoad 不画、onReady 画，节点没就绪时重试', canvas.queries, '次后画成')

  // 重试次数用尽仍失败时不能死循环
  broker.reset()
  canvas.failTries = 99
  canvas.queries = 0
  const qrFail = hostDevice({ qrRetries: 2, qrRetryMs: 5 })
  qrFail.onLoad({ s: SID })
  qrFail.onReady()
  await sleep(60)
  assert.strictEqual(canvas.queries, 3, '应只查 1 + 2 次，实际: ' + canvas.queries)
  qrFail.onUnload()
  canvas.failTries = 0
  console.log('OK 二维码：重试用尽就报错，不死循环')

  // ---------- B1 订阅被拒：连接是好的，得自己重订 ----------
  broker.reset()
  const subHost = hostDevice({ subRetries: 3, subRetryMs: 5 })
  broker.failSubs = 2               // 前两次 SUBSCRIBE 被 ACL 拒掉
  subHost.onLoad({ s: SID })
  subHost.onShow()
  await tick(6)
  assert.strictEqual(subHost.data.offline, true, '订不上时应先亮离线横幅')
  await subHost.watcher.ready       // 不应 reject
  await tick(4)
  assert.strictEqual(broker.failSubs, 0, '应把两次失败都重试掉')
  const subs = broker.clients[broker.clients.length - 1].subs
  assert.ok(subs.indexOf(`eq/${SID}/c`) !== -1, '重订成功后应订上叫号进度，实际: ' + subs.join(' '))
  assert.strictEqual(subHost.data.offline, false, '重订成功后横幅应消失')
  subHost.onUnload()
  console.log('OK 订阅被拒：亮横幅 + 自动重订，成功后横幅消失')

  // 一直被拒时 ready 要 reject，界面停在离线态
  broker.reset()
  const subFail = hostDevice({ subRetries: 2, subRetryMs: 5 })
  broker.failSubs = 99
  subFail.onLoad({ s: SID })
  subFail.onShow()
  let rejected = false
  subFail.watcher.ready.catch(() => { rejected = true })
  await sleep(60)
  assert.strictEqual(rejected, true, '重试用尽后 ready 应 reject')
  assert.strictEqual(subFail.data.offline, true)
  subFail.onUnload()
  broker.failSubs = 0
  console.log('OK 订阅一直被拒：重试用尽后停在离线态，不无限重试')

  // ---------- B2 发布超时：连接半开时按钮不能一直转圈 ----------
  broker.reset()
  const pubHost = hostDevice({ publishTimeoutMs: 30, syncSettleMs: 10 })
  broker.publish(`eq/${SID}/n`, JSON.stringify({ n: 1, t: 1 }), { retain: true })
  pubHost.onLoad({ s: SID })
  pubHost.onShow()
  await tick(8)
  await sleep(30)                    // 等回放静默，叫号才解锁（见 host.js sync）
  assert.strictEqual(pubHost.data.waiting, 1, '应看到 1 位等待，实际: ' + pubHost.data.waiting)

  broker.stallPublish = true         // 消息收下了，PUBACK 永远不来
  const t0 = Date.now()
  await pubHost.callNext()
  const cost = Date.now() - t0
  assert.ok(cost >= 25 && cost < 500, '应在超时预算内失败，实际耗时: ' + cost + 'ms')
  assert.strictEqual(pubHost.data.busy, false, '按钮不能卡在 loading')
  assert.strictEqual(pubHost.data.offline, true, '失败后应提示离线')
  assert.strictEqual(pubHost.data.called, 0, '没确认的叫号不应乐观更新')
  broker.stallPublish = false
  pubHost.onUnload()
  console.log('OK 发布超时：', cost + 'ms 就失败，按钮不卡 loading')

  // ---------- B3 运行中断线：横幅要跟着连接状态走 ----------
  broker.reset()
  const netHost = hostDevice()
  netHost.onLoad({ s: SID })
  netHost.onShow()
  await tick(8)
  assert.strictEqual(netHost.data.offline, false)

  const live = broker.clients[broker.clients.length - 1]
  live.emit('close')                 // 页面开着、连接中途断掉
  await tick(4)
  assert.strictEqual(netHost.data.offline, true, '断线时应亮横幅（此刻界面上的进度是旧的）')

  live.connected = true
  live.emit('connect')               // mqtt.js 自己重连上了
  await tick(4)
  assert.strictEqual(netHost.data.offline, false, '重连后横幅应消失')
  netHost.onUnload()
  console.log('OK 运行中断线：close/offline 事件会把离线横幅点亮，重连后自动消失')

  // ---------- B4 关连接与重连必须串行 ----------
  // wxs transport 的连接状态在模块作用域里，抢在旧连接关完之前开新的，
  // 两个 client 会交叉引用同一份 socket 状态。
  broker.reset()
  const mq = freshMqttc()
  const w1 = mq.watch(SID, () => {}, { joins: false })
  await tick(6)
  assert.strictEqual(broker.clients.length, 1)
  w1.unwatch()
  assert.strictEqual(mq.disconnect(), true)
  assert.strictEqual(broker.clients.length, 0, '断开应真的摘掉连接')

  const w2 = mq.watch(SID, () => {}, { joins: false })
  assert.strictEqual(broker.clients.length, 0, '旧连接关完之前不应开新连接')
  await w2.ready
  assert.strictEqual(broker.clients.length, 1, '旧连接关完后应接上新连接')
  w2.unwatch()
  mq.disconnect()
  await tick(4)
  console.log('OK 关连接与重连串行：旧连接没关完不开新的')

  // ---------- B5 同一 sid 多个 watcher：订阅按总集记账 ----------
  broker.reset()
  const mq2 = freshMqttc()
  const a = mq2.watch(SID, () => {}, {})                          // 客人端：m + c + j/+
  const b = mq2.watch(SID, () => {}, { joins: false, total: true }) // 老板端：m + c + n
  await Promise.all([a.ready, b.ready])
  await tick(4)
  const both = broker.clients[broker.clients.length - 1].subs
  assert.ok(both.indexOf(`eq/${SID}/j/+`) !== -1 && both.indexOf(`eq/${SID}/n`) !== -1,
    '两个 watcher 要的 topic 都应订上，实际: ' + both.join(' '))

  // 退掉一个，另一个还得继续收到消息
  a.unwatch()
  broker.publish(`eq/${SID}/c`, JSON.stringify({ n: 7, t: 1 }), { retain: true })
  await tick(4)
  assert.strictEqual(b.state.called, 7, '还有 watcher 在，路由不能停')

  // 两个都退掉后 filters 要被清干净：重连时不该再订任何跟这个 sid 有关的 topic
  b.unwatch()
  mq2.disconnect()
  await tick(6)
  const other = mq2.watch('E'.repeat(32), () => {}, { joins: false })
  await other.ready
  await tick(4)
  const leftover = broker.clients[broker.clients.length - 1].subs.filter(f => f.indexOf(SID) !== -1)
  assert.deepStrictEqual(leftover, [], '重连后不应残留上个 sid 的订阅，实际: ' + leftover.join(' '))
  other.unwatch()
  mq2.disconnect()
  await tick(4)
  console.log('OK 多 watcher：任一存在即路由，全退掉后订阅记账清零')

  // ---------- 断线期间再 publish，不能另开一条连接 ----------
  // connect() 原来只认「已连上」和「正在连」两种状态，漏了「client 还在、mqtt.js 正在
  // 自己重连」这一种，于是会新开一条并把旧那条丢下不管：旧 client 自己的重连循环还在跑，
  // 而 wx transport 的连接状态在模块作用域里，两条 client 会交叉引用同一份 socket。
  broker.reset()
  const dropHost = hostDevice({ syncSettleMs: 10 })
  dropHost.onLoad({ s: SID })
  dropHost.onShow()
  await tick(8)
  await sleep(30)                    // 先同步完，否则叫号是锁的
  broker.publish(`eq/${SID}/n`, JSON.stringify({ n: 2, t: 1 }), { retain: true })
  await tick(4)
  assert.strictEqual(dropHost.data.waiting, 2)

  const dropped = broker.clients[broker.clients.length - 1]
  dropped.connected = false
  dropped.emit('close')              // 中途断线，mqtt.js 开始自己重连（client 还活着）
  await tick(4)
  assert.strictEqual(dropHost.data.offline, true)

  const calling = dropHost.callNext() // 老板正好这时候点了叫号
  await tick(8)
  assert.strictEqual(broker.clients.length, 1, '重连期间不该另开一条连接，实际: ' + broker.clients.length)

  dropped.connected = true
  dropped.emit('connect')            // 重连上了
  await calling
  assert.strictEqual(dropHost.data.called, 1, '重连上之后那次叫号应正常发出去')
  assert.strictEqual(broker.clients.length, 1)
  dropHost.onUnload()
  console.log('OK 断线期间 publish：挂在原连接上等重连，不会多出第二条 client')

  // ---------- 新建连接只订一次：retained 回放不能被推两遍 ----------
  // topics 先记进 filters、再由 connect 事件恢复订阅，然后 watch 自己又订一次 ——
  // 按 MQTT 规范每成功订阅一次 broker 就重推一遍 retained，客人首次取号那次全量回放
  // 是流量大头（DESIGN.md P1#3），翻倍很贵。
  broker.reset()
  const mqDup = freshMqttc()
  for (let i = 0; i < 3; i++) {
    broker.publish(`eq/${SID}/j/${String(i).repeat(16)}`, JSON.stringify({ t: 1000 + i }), { retain: true })
  }
  broker.publish(`eq/${SID}/m`, JSON.stringify({ n: '测试队列', t: 1 }), { retain: true })
  broker.publish(`eq/${SID}/c`, JSON.stringify({ n: 0, t: 1 }), { retain: true })

  let hits = 0
  const wDup = mqDup.watch(SID, () => { hits++ }, {})
  await wDup.ready
  await tick(6)
  const dupSubs = broker.clients[broker.clients.length - 1].subs
  assert.strictEqual(dupSubs.length, new Set(dupSubs).size, '同一个 topic 不能订两遍，实际: ' + dupSubs.join(' '))
  assert.strictEqual(hits, 5, 'retained 应只回放一遍（3 条入队 + m + c），实际: ' + hits)
  wDup.unwatch()
  mqDup.disconnect()
  await tick(6)
  console.log('OK 新建连接只订一次：retained 回放', hits, '条，没有翻倍')

  // ---------- 人数上报失败要重试 ----------
  // 老板端不订 j/+，「等待中」只有 eq/{sid}/n 这一个来源：这条消息丢了他就少算，
  // 队列里只有一位客人时人数会是 0，「叫下一位」直接 disabled。
  broker.reset()
  const cust = makeDevice('pages/queue/queue.js', { settleMs: 30, publishTimeoutMs: 30 })
  cust.__activate()
  const custStore = require(P('utils/store.js'))
  const custCid = 'C'.repeat(16)
  custStore.saveTicket({ s: SID, n: '测试队列', cid: custCid, t: Date.now(), q: 0 })
  cust.onLoad()
  cust.onShow()
  await tick(8)
  broker.publish(`eq/${SID}/j/${custCid}`, JSON.stringify({ t: 1000 }), { retain: true })
  await tick(6)

  broker.stallPublish = true          // 上报发不出去
  cust.freezeRank(SID)
  await sleep(80)
  assert.ok(!broker.retained.has(`eq/${SID}/n`), '前两次上报应该都失败')
  assert.strictEqual(custStore.findTicket(SID).f, 1, '上报失败不影响号码本身定稿')
  broker.stallPublish = false
  await sleep(150)
  assert.ok(broker.retained.has(`eq/${SID}/n`), '上报失败必须重试，否则老板端永远少算一个人')
  assert.strictEqual(JSON.parse(broker.retained.get(`eq/${SID}/n`)).n, 1)
  cust.onHide()
  console.log('OK 人数上报失败会重试，重试成功后老板端才数得对')

  // ---------- 客人的人数上报彻底丢了：老板可以「重新统计」 ----------
  broker.reset()
  const lonely = hostDevice({ settleMs: 30, syncSettleMs: 10 })
  lonely.onLoad({ s: SID })
  lonely.onShow()
  await tick(8)
  await sleep(30)
  // 客人取了号（入队消息在 broker 上），但他那条人数上报一直没到
  broker.publish(`eq/${SID}/j/${'C'.repeat(16)}`, JSON.stringify({ t: 1000 }), { retain: true })
  await tick(6)
  assert.strictEqual(lonely.data.waiting, 0, '没人上报时老板端是 0 —— 此时叫号按钮 disabled')

  lonely.recount()
  await tick(8)
  assert.strictEqual(lonely.data.waiting, 1, '重新统计应直接数入队消息，实际: ' + lonely.data.waiting)
  await lonely.callNext()
  await tick(6)
  assert.strictEqual(lonely.data.called, 1, '统计完就能正常叫号')
  await sleep(60)
  assert.strictEqual(lonely.data.counting, false)
  const lonelySubs = broker.clients[broker.clients.length - 1].subs
  assert.strictEqual(lonelySubs.indexOf(`eq/${SID}/j/+`), -1, '统计完要把 j/+ 退掉，实际: ' + lonelySubs.join(' '))
  lonely.onUnload()
  console.log('OK 没人上报人数时，老板端「重新统计」能数出人来并解锁叫号')

  // ---------- 没网时打开页面：不能把手上的号重发掉 ----------
  // 断线会让入队消息停下来，而「消息停下来」正是 settle 用来判断「集合已收敛」的信号。
  // 分不清这两件事，就会拿着一个空集合走上重发阶梯：那条入队消息明明好好地在 broker 上，
  // 只是这台设备没订上 —— 重发换掉 cid，等于把自己从队首挪到队尾。
  broker.reset()
  const T0 = Date.now() - 3600 * 1000
  const meCid = 'C'.repeat(16)
  broker.publish(`eq/${SID}/m`, JSON.stringify({ n: '测试队列', t: 1 }), { retain: true })
  broker.publish(`eq/${SID}/c`, JSON.stringify({ n: 0, t: 1 }), { retain: true })
  broker.publish(`eq/${SID}/j/${meCid}`, JSON.stringify({ t: T0 }), { retain: true })   // 我是 1 号
  for (let i = 1; i <= 4; i++) {
    broker.publish(`eq/${SID}/j/O${String(i).repeat(15)}`, JSON.stringify({ t: T0 + i }), { retain: true })
  }
  const joinCount = () => Array.from(broker.retained.keys()).filter(k => k.indexOf(`eq/${SID}/j/`) === 0).length

  const off = makeDevice('pages/queue/queue.js',
    { settleMs: 20, subRetries: 1, subRetryMs: 5, subRetryLongMs: 30, publishTimeoutMs: 20 })
  off.__activate()
  const offStore = require(P('utils/store.js'))
  offStore.saveTicket({ s: SID, n: '测试队列', cid: meCid, t: T0, q: 0 })

  broker.failConns = 99            // 整段时间都没网
  off.onLoad()
  off.onShow()
  await sleep(250)                 // 远超 settleMs × (重发次数 + 1)
  assert.strictEqual(off.data.offline, true, '没网时应亮离线横幅')
  assert.strictEqual(offStore.findTicket(SID).cid, meCid, '没网期间不能重发换 cid —— 那会把自己挪到队尾')
  assert.strictEqual(joinCount(), 5, 'broker 上不该多出幽灵号，实际: ' + joinCount())

  broker.failConns = 0             // 网络恢复，慢重试把订阅接回来
  await sleep(250)
  off.rebuild()
  assert.strictEqual(off.data.groups[0].items[0].num, 1, '恢复后还应该是 1 号，实际: ' + off.data.groups[0].items[0].num)
  assert.strictEqual(offStore.findTicket(SID).f, 1, '集合到齐之后才定稿')
  assert.strictEqual(joinCount(), 5)
  off.onHide()
  await tick(6)
  console.log('OK 没网时打开页面：不重发、不换号，网络恢复后仍是 1 号')

  // ---------- 回放刚开个头就断线：不能把偏小的号码永久定稿 ----------
  // `f` 挡住了「回放途中锁屏」，但「回放途中断线」是同一形状的另一扇门 —— 定稿之后
  // j/+ 就退了，这个号永远不会再被纠正，等于和真正的 1 号撞号。
  broker.reset()
  const meCid2 = 'M'.repeat(16)
  broker.publish(`eq/${SID}/m`, JSON.stringify({ n: '测试队列', t: 1 }), { retain: true })
  broker.publish(`eq/${SID}/c`, JSON.stringify({ n: 0, t: 1 }), { retain: true })
  for (let i = 1; i <= 3; i++) {   // 我前面还有 3 个人，我应该是 4 号
    broker.publish(`eq/${SID}/j/A${String(i).repeat(15)}`, JSON.stringify({ t: T0 + i }), { retain: true })
  }
  broker.publish(`eq/${SID}/j/${meCid2}`, JSON.stringify({ t: T0 + 9 }), { retain: true })

  broker.skipReplay = `/j/A`       // 那三条「还在路上」
  const half = makeDevice('pages/queue/queue.js', { settleMs: 30 })
  half.__activate()
  const halfStore = require(P('utils/store.js'))
  halfStore.saveTicket({ s: SID, n: '测试队列', cid: meCid2, t: T0 + 9, q: 0 })
  half.onLoad()
  half.onShow()
  await tick(8)
  broker.dropAll()                 // 回放刚开个头，断了
  await sleep(120)                 // 超过 settleMs
  assert.ok(!halfStore.findTicket(SID).f, '断线期间不能定稿 —— 集合根本没到齐')
  assert.strictEqual(half.joinsOn[SID], true, '没定稿就不能退掉 j/+')

  broker.skipReplay = null
  broker.reviveAll()               // mqtt.js 自己重连上，clean session → 全量重推
  await sleep(150)
  half.rebuild()
  assert.strictEqual(half.data.groups[0].items[0].num, 4, '集合到齐后应算出 4 号，实际: ' + half.data.groups[0].items[0].num)
  assert.strictEqual(halfStore.findTicket(SID).f, 1, '这时候才该定稿')
  half.onHide()
  await tick(6)
  console.log('OK 回放中途断线：不定稿，重连补齐后号码是 4 而不是 1')

  // ---------- 快重试用尽之后，网络恢复要能自己订回来 ----------
  // 没订上的 topic 进不了 filters，mqtt.js 的自动重连也就恢复不了它；而横幅还会在
  // 下一次 publish 成功建连时自己消失 —— 看起来一切正常，实际零订阅。
  broker.reset()
  broker.publish(`eq/${SID}/n`, JSON.stringify({ n: 2, t: 1 }), { retain: true })
  const heal = hostDevice({ subRetries: 1, subRetryMs: 5, subRetryLongMs: 60 })
  broker.failConns = 3             // 初次 + 1 次快重试 + 第 1 次慢重试都连不上
  heal.onLoad({ s: SID })
  heal.onShow()
  let healRejected = false
  heal.watcher.ready.catch(() => { healRejected = true })
  await sleep(40)                  // 快重试预算（1 次 × 5ms）早就用完，慢重试还没轮到
  assert.strictEqual(healRejected, true, '快重试用尽后仍要把失败抛给页面')
  assert.strictEqual(heal.data.offline, true)
  assert.strictEqual(broker.failConns, 1, '此刻只该试过 2 次，实际剩余: ' + broker.failConns)

  await sleep(300)                 // 慢重试：先再失败一次，再成功
  assert.strictEqual(broker.failConns, 0, '慢重试要一直试到连上')
  assert.strictEqual(heal.data.offline, false, '网络恢复后应自己订回来，横幅消失')
  assert.strictEqual(heal.data.waiting, 2, '订上了 retained 才回放得到，实际: ' + heal.data.waiting)
  const healSubs = broker.clients[broker.clients.length - 1].subs
  assert.strictEqual(healSubs.length, new Set(healSubs).size, '自愈不能把 topic 订两遍，实际: ' + healSubs.join(' '))
  heal.onUnload()
  await tick(6)
  console.log('OK 快重试用尽后网络恢复：慢重试自己把订阅接回来，没有重复订阅')

  // ---------- 进页面时回放还在路上：叫号必须是锁的 ----------
  // 老板从「我创建的」点进一个已经叫到头的队列。called 不存本地，进页面时是 0，而 waiting
  // 是拿本地峰值 p 垫上去的 —— 于是在 retained 回放到达之前，「叫下一位」是亮的，标签还写着
  // 「叫下一位（1 号）」。老板这一下点出去的是 called + 1 = 1，而 `eq/{sid}/c` 只有一个
  // retained 槽位，就这么被改小了。
  //
  // 已经在线的设备靠 route() 里的 max 合并不受影响，所以这个 bug 在单设备上看不见；
  // 出事的是**之后**才订阅的人：客人重开页面、新客人扫码，拿到的回放是 1，界面上写着
  // 「已叫到 1 号」，而实际叫到 3 号了 —— 要等下一次真的叫号才会被纠正。max 合并防的
  // 正是这种倒退，这里是老板自己那台还没同步的设备把它捅穿了。
  broker.reset()
  const late = makeDevice('pages/host/host.js', { syncSettleMs: 40 })
  late.__activate()
  require(P('utils/store.js')).saveQueue({ s: SID, n: '叫到头的队列', t: Date.now(), p: 3 })
  // broker 上的真实进度：3 个人取号，3 个都叫过了
  broker.publish(`eq/${SID}/n`, JSON.stringify({ n: 3, t: 1 }), { retain: true })
  broker.publish(`eq/${SID}/c`, JSON.stringify({ n: 3, t: 1 }), { retain: true })

  late.onLoad({ s: SID })
  assert.strictEqual(late.data.waiting, 3, '本地峰值先垫上 —— 这正是按钮会亮的原因')
  assert.strictEqual(late.data.synced, false, '什么都还没收到，不能算同步完')
  late.onShow()
  // 一个 tick 都不给：建连和 retained 回放都还在路上，正是老板点得到按钮的那一瞬间
  await late.callNext()
  assert.strictEqual(late.data.called, 0, '被挡住的点击应立刻返回，不发消息也不乐观更新')
  await tick(10)
  assert.strictEqual(JSON.parse(broker.retained.get(`eq/${SID}/c`)).n, 3,
    '同步完成前的点击不能把 c 槽位改小，实际: ' + broker.retained.get(`eq/${SID}/c`))

  await sleep(120)
  assert.strictEqual(late.data.synced, true, '回放静默之后应解锁')
  assert.strictEqual(late.data.called, 3, '同步后应看到真实进度')
  assert.strictEqual(late.data.waiting, 0, '叫到头了 —— 到这时按钮才该是 disabled')
  late.onUnload()
  console.log('OK 进页面时回放还没到：叫号锁住，点了也不会把叫号进度改小')

  // ---------- 失效的队列：老板端不订阅、不画二维码、也不能叫号 ----------
  // 本地保留 3 天而 broker 上的 retained 只活 24h。举着一张失效的二维码，客人会扫进一个
  // m/c 都已过期、没人叫号的队列里干等。
  broker.reset()
  canvas.queries = 0
  const stale = makeDevice('pages/host/host.js')
  stale.__activate()
  const staleT = Date.now() - 25 * 3600 * 1000
  require(P('utils/store.js')).saveQueue({ s: SID, n: '昨天的队列', t: staleT, p: 12 })
  stale.onLoad({ s: SID })
  assert.strictEqual(stale.data.ended, true, '超过 retainTtlSec 的队列应判为已结束')
  assert.strictEqual(stale.data.waiting, 0, '失效队列不能拿本地峰值当等待人数，否则叫号按钮是亮的')
  stale.onReady()
  stale.onShow()
  await sleep(60)
  assert.strictEqual(canvas.queries, 0, '失效队列不该再画二维码')
  assert.strictEqual(stale.watcher, undefined, '失效队列不该订阅（broker 上什么都不剩了）')
  assert.strictEqual(broker.clients.length, 0, '也不该为它建连接')
  stale.onUnload()
  console.log('OK 失效的队列：不订阅、不画二维码、叫号按钮锁死')

  // ---------- 页面开着跨过 24h 边界：回到前台要重新判定失效 ----------
  // host 页主动 setKeepScreenOn，本来就是让老板举着屏幕开一整天的，所以它是全 app 里
  // 最容易跨过边界的那个页面。只在 onLoad 判一次的话，隔夜切回来仍然举着一张失效的
  // 二维码，客人扫进去是一个 m 已过期、没人叫号的队列 —— 正是 ended 要挡的那件事。
  broker.reset()
  canvas.queries = 0
  const crossing = makeDevice('pages/host/host.js', { retainTtlSec: 1 })
  crossing.__activate()
  const freshT = Date.now()
  require(P('utils/store.js')).saveQueue({ s: SID, n: '开着的队列', t: freshT, p: 3 })
  crossing.onLoad({ s: SID })
  crossing.onReady()
  crossing.onShow()
  await sleep(30)
  assert.strictEqual(crossing.data.ended, false, '刚建的队列不该判为已结束')
  assert.ok(crossing.watcher, '未失效时应当订阅')
  await sleep(1100)                       // 跨过「24 小时」
  crossing.onHide()
  crossing.onShow()                       // 切后台再回来
  await sleep(30)
  assert.strictEqual(crossing.data.ended, true, '回到前台时应重新判定为已结束')
  assert.strictEqual(crossing.data.waiting, 0, '已结束后等待人数要归零，否则叫号按钮还是亮的')
  assert.strictEqual(crossing.watcher, null, '已结束就不该再订阅')
  assert.strictEqual(broker.clients.length, 0, '也不该再占着连接')
  crossing.onUnload()
  console.log('OK 页面开着跨过 24h：切回前台重新判定为已结束，订阅与连接一并释放')

  // ---------- retained 回放不能把 setData 和存储扫描按条数放大 ----------
  // setData 是跨线程调用，store.findTicket 会走一遍 wx.getStorageInfoSync 全量扫描。
  // 大店首次取号那一下是 N ≈ 200 条消息挤在一两秒内（DESIGN.md P1#3）。
  broker.reset()
  const N = 30
  const meCid3 = 'Z'.repeat(16)
  broker.publish(`eq/${SID}/m`, JSON.stringify({ n: '测试队列', t: 1 }), { retain: true })
  broker.publish(`eq/${SID}/c`, JSON.stringify({ n: 0, t: 1 }), { retain: true })
  for (let i = 0; i < N - 1; i++) {
    broker.publish(`eq/${SID}/j/B${String(i).padStart(15, '0')}`, JSON.stringify({ t: T0 + i }), { retain: true })
  }
  broker.publish(`eq/${SID}/j/${meCid3}`, JSON.stringify({ t: T0 + N }), { retain: true })

  const cheap = makeDevice('pages/queue/queue.js', { settleMs: 100000 })   // 静默窗口远没到
  cheap.__activate()
  require(P('utils/store.js')).saveTicket({ s: SID, n: '测试队列', cid: meCid3, t: T0 + N, q: 0 })
  cheap.onLoad()

  let sets = 0
  const rawSetData = cheap.setData
  cheap.setData = function (d) { sets++; return rawSetData.call(this, d) }
  let scans = 0
  const rawInfo = global.wx.getStorageInfoSync
  global.wx.getStorageInfoSync = () => { scans++; return rawInfo() }

  cheap.onShow()
  await tick(20)
  await sleep(150)
  global.wx.getStorageInfoSync = rawInfo
  assert.strictEqual(cheap.data.groups[0].items[0].num, N, `回放应算出 ${N} 号，实际: ` + cheap.data.groups[0].items[0].num)
  assert.ok(sets <= 10, `${N + 2} 条消息的回放不该按条数放大 setData，实际: ` + sets)
  assert.ok(scans <= 10, `也不该每条消息扫一遍本地存储，实际: ` + scans)
  cheap.onHide()
  await tick(6)
  console.log(`OK 回放 ${N + 2} 条消息：setData ${sets} 次、存储扫描 ${scans} 次，不随条数增长`)

  finished = true
  console.log('\n健壮性全部通过')
})().catch(e => { console.error('\n失败:', e.stack || e.message); process.exit(1) })
