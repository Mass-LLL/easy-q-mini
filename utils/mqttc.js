// MQTT 单例客户端 + 队列状态订阅。
// 必须是单例：wx transport 把连接状态放在模块作用域，同时只能一条连接，end/connect 必须
// 串行（teardown）。clean:true 让每次 connect 事件重订阅并触发 retained 回放，顺带完成
// 状态重建。详见 DESIGN.md「网络不好的时候」。
//
// libs/mqtt.js 是打过补丁的 vendored 产物，升级用 `npm run vendor:mqtt`，别直接拷贝。
const mqtt = require('../libs/mqtt.js')
const { config, isConfigured } = require('./config.js')

let client = null
let pending = null
let pendingFail = null         // pending 的 reject，disconnect 时放掉，别让订阅链永远悬着
let closing = null             // 上一条连接正在关，关完之前不开新的
let waiters = []               // 正在重连时挂在这儿等连接的调用方（见 connect）
let connOk = true              // 乐观初始：没观察到断开就当在线，免得开页面闪「未连接」
const filters = new Set()      // 重连后要恢复的订阅（订上了才进，见 watch）
const watchers = new Map()     // sid -> {state, topics:Set, listeners:Set}

const t = {
  meta: sid => `${config.topicPrefix}/${sid}/m`,
  called: sid => `${config.topicPrefix}/${sid}/c`,
  join: (sid, cid) => `${config.topicPrefix}/${sid}/j/${cid}`,
  joinsFilter: sid => `${config.topicPrefix}/${sid}/j/+`,
  total: sid => `${config.topicPrefix}/${sid}/n`,
}

const emptyState = () => ({ name: '', joins: [], called: 0, total: 0, online: connOk })

const wait = ms => new Promise(resolve => setTimeout(resolve, ms))

const withTimeout = (p, ms, msg) => new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error(msg)), ms)
  const done = fn => v => { clearTimeout(timer); fn(v) }
  p.then(done(resolve), done(reject))
})

// 重连期间挂起的调用方：连上了放行，主动断开就放弃 —— 不能悬着，也不能各自另开连接
const settleWaiters = err => {
  const list = waiters
  waiters = []
  list.forEach(w => (err ? w.reject(err) : w.resolve(client)))
}

// 连接状态变化通知所有 watcher：断线时 mqtt.js 会静默重连，但这期间界面的号码是旧的
const setConn = ok => {
  if (connOk === ok) return
  connOk = ok
  watchers.forEach(w => {
    w.state.online = ok
    w.listeners.forEach(fn => fn(w.state))
  })
}

// 关连接并在关完之前挡住新连接：end(true) 是异步的，而 transport 是模块级单例态，
// 抢跑会让两个 client 交叉引用同一份 socket 状态（见文件头）
const teardown = c => {
  const closed = new Promise(resolve => {
    let timer = null
    const fin = () => {
      if (timer === false) return
      clearTimeout(timer)
      timer = false
      resolve()
    }
    timer = setTimeout(fin, config.closeTimeoutMs)
    try {
      c.end(true, {}, fin)
    } catch (e) {
      fin()
    }
  })
  const mine = closed.then(() => { if (closing === mine) closing = null })
  closing = mine
  return mine
}

const connect = () => {
  if (client && client.connected) return Promise.resolve(client)
  if (pending) return pending
  if (!isConfigured()) return Promise.reject(new Error('MQTT 未配置，请填写 utils/config.js'))
  if (closing) {
    pendingFail = null   // 这条 pending 没有自己的 reject，disconnect 也够不到它（那时 client 已置空）
    pending = closing.then(() => { pending = null; return connect() })
    return pending
  }
  // 第三种状态：client 还在、mqtt.js 正在自己重连。必须等它，绝不能另开一条 ——
  // 被丢下的 client 没人 end()，重连循环一直跑，白烧会话分钟（真机上表现为
  // 「网络抖一下之后整个小程序失联」）
  if (client) return new Promise((resolve, reject) => waiters.push({ resolve, reject }))

  pending = new Promise((resolve, reject) => {
    pendingFail = reject
    const c = mqtt.connect(config.brokerUrl, {
      username: config.username,
      password: config.password,
      protocolVersion: 5,
      clean: true,
      keepalive: 30,
      connectTimeout: 10000,
      reconnectPeriod: 3000,
    })
    client = c   // 立刻记住，切后台时连还在握手的连接也能关掉
    // 已被换掉的 client 的事件一概不理：旧连接的 close 会把新连接标成离线
    const mine = () => c === client

    c.on('connect', () => {
      if (!mine()) return
      pending = null
      pendingFail = null
      setConn(true)
      // clean session：重连后订阅全丢，按 filters 全量恢复；失败就标离线，等下次重连
      if (filters.size > 0) {
        c.subscribe(Array.from(filters), { qos: 1 }, err => { if (mine() && err) setConn(false) })
      }
      resolve(c)
      settleWaiters()
    })
    c.on('message', (topic, payload) => { if (mine()) route(topic, payload) })
    c.on('close', () => { if (mine()) setConn(false) })
    c.on('offline', () => { if (mine()) setConn(false) })
    c.on('error', err => {
      if (!mine()) return
      setConn(false)
      if (!pending) return
      pending = null
      pendingFail = null
      client = null
      teardown(c)
      reject(err)
    })
  })
  return pending
}

// 只认有限数值。sid 对队列里所有人都可见（见 DESIGN.md P1#2），伪造数字是已接受的
// 缺口（P2#8），但 {"n":"abc"} 会顺着 Math.max/aheadOf 变成 NaN 毒化两端界面，零成本防线
const num = v => (typeof v === 'number' && isFinite(v) ? v : null)

// called/total 再窄一档：非负整数、且在人间量级内（挡掉 3.5 → 「叫下一位（4.5 号）」、
// 1e308 → 全队永久「已过号」）
const MAX_COUNT = 1e6
const count = v => {
  const n = num(v)
  return n !== null && n >= 0 && n <= MAX_COUNT && Math.floor(n) === n ? n : null
}

// 把收到的消息合并进对应 sid 的状态
const route = (topic, payload) => {
  const parts = topic.split('/')
  const sid = parts[1]
  const w = watchers.get(sid)
  if (!w) return

  const kind = parts[2]
  const body = payload && payload.length > 0 ? safeParse(payload.toString()) : null

  if (kind === 'm') {
    w.state.name = (body && typeof body.n === 'string') ? body.n : ''
  } else if (kind === 'c') {
    // 取 max：单个 retained 槽位后写覆盖，两台老板设备同时叫号时慢半拍的那条会把计数
    // 打回去（「已到号」退回「下一位」）。和 n 同构，见 DESIGN.md
    const n = count(body && body.n)
    if (n !== null) w.state.called = Math.max(w.state.called, n)
  } else if (kind === 'n') {
    // 客人各自上报自己的号，是「已取号人数」的下界；同样取 max（慢半拍的客人会写回更小的值）
    const n = count(body && body.n)
    if (n !== null) w.state.total = Math.max(w.state.total, n)
  } else if (kind === 'j') {
    const cid = parts[3]
    const jt = num(body && body.t)
    const joins = w.state.joins.filter(j => j.cid !== cid)
    // 入队消息只增不删（没有取消排队），空/坏 payload 只当作不存在
    if (jt !== null) joins.push({ cid, t: jt })
    w.state.joins = joins
  } else {
    return
  }

  w.listeners.forEach(fn => fn(w.state))
}

const safeParse = s => {
  try {
    return JSON.parse(s)
  } catch (e) {
    return null
  }
}

// 订阅一个队列的状态变化，listener 会被多次调用（retained 陆续到达，界面渐进刷新）。
// opts.joins = false 时不订 `j/+`（客人定稿后 rank 不会再变，「前面还有几位」= q − c.n − 1，
// 大队列时省掉的流量是数量级的）；opts.total = true 时订 `n`（老板端要的只是人数标量）。
const watch = (sid, listener, opts) => {
  const withJoins = !(opts && opts.joins === false)
  const withTotal = !!(opts && opts.total)
  let w = watchers.get(sid)
  if (!w) {
    w = { state: emptyState(), topics: new Set(), listeners: new Set() }
    watchers.set(sid, w)
  }
  w.listeners.add(listener)

  const topics = [t.meta(sid), t.called(sid)]
  if (withJoins) topics.push(t.joinsFilter(sid))
  if (withTotal) topics.push(t.total(sid))
  // 订阅按 sid 记总集：同一个 sid 可以挂多个 watcher、要的 topic 还不一样（老板不要 j/+、
  // 客人不要 n），拆订阅必须等最后一个 watcher 走掉时按总集拆，否则 filters 里残留
  // 别人加过的 topic，重连后订一堆没人要的消息
  topics.forEach(x => w.topics.add(x))

  let alive = true
  let retryTimer = null

  const subscribe = tries => connect()
    .then(c => new Promise((resolve, reject) => {
      c.subscribe(topics, { qos: 1 }, err => (err ? reject(err) : resolve()))
    }))
    .then(() => {
      if (!alive) return
      // 订上了才记进 filters（重连恢复用）。先记再订的话，新建连接会被恢复订阅和这里的
      // subscribe 各订一遍，而每次成功订阅 broker 都重推一遍 retained —— 首次取号的
      // 全量回放是流量大头（DESIGN.md P1#3），翻倍很贵
      topics.forEach(x => { if (w.topics.has(x)) filters.add(x) })
      setConn(true)
    })
    .catch(err => {
      // 订不上等于什么都收不到，先亮离线横幅再重订（连接可能好着却被 ACL 拒，也可能
      // 压根没建起来 —— mqtt.js 的重连对这两条都帮不上忙）
      setConn(false)
      if (!alive) throw err
      if (tries > 0) return wait(config.subRetryMs).then(() => subscribe(tries - 1))
      // 快重试用完：把失败抛给调用方（亮横幅），转入慢重试。不能彻底放弃 —— 没订上的
      // topic 永远进不了 filters，连「重连后恢复」都救不了它；也不能一直快重试，
      // ACL 配错时那只是空转
      slowRetry()
      throw err
    })

  // 慢重试：一直试到订上或 watcher 被退掉，间隔拉长到 subRetryLongMs
  const slowRetry = () => {
    if (!alive || retryTimer) return
    retryTimer = setTimeout(() => {
      retryTimer = null
      if (alive) subscribe(0).catch(() => {})
    }, config.subRetryLongMs)
  }

  const unwatch = () => {
    alive = false
    clearTimeout(retryTimer)
    retryTimer = null
    w.listeners.delete(listener)
    if (w.listeners.size > 0) return
    watchers.delete(sid)
    const all = Array.from(w.topics)
    all.forEach(x => filters.delete(x))
    if (client && client.connected) client.unsubscribe(all)
  }

  // 老板端「重新统计人数」用：临时订回 j/+ 直接数入队消息（见 host.js recount）。
  // 订失败不重试：老板主动点的一次性动作，没数出来再点一次
  const startJoins = () => {
    const f = t.joinsFilter(sid)
    if (w.topics.has(f)) return
    w.topics.add(f)
    filters.add(f)
    if (client && client.connected) client.subscribe(f, { qos: 1 })
  }

  // 号码定稿后退掉 j/+，保留 m/c：之后只关心叫号进度
  const stopJoins = () => {
    const f = t.joinsFilter(sid)
    w.topics.delete(f)
    if (!filters.delete(f)) return
    if (client && client.connected) client.unsubscribe(f)
  }

  return { ready: subscribe(config.subRetries), unwatch, stopJoins, startJoins, state: w.state }
}

// 会话分钟按连接时长计费，退订不省钱 —— 没有 watcher 了就必须真的断开。
// clean:true + retained 让重连几乎无代价：重新订阅时 broker 会把状态全推回来
const disconnect = () => {
  if (watchers.size > 0 || !client) return false
  const c = client
  const fail = pendingFail
  client = null
  pending = null
  pendingFail = null
  connOk = true   // 这次断开是我们自己要的，下次开页面重新乐观
  settleWaiters(new Error('连接已关闭'))
  // 还在握手就被切后台：connect 事件之后会被 mine() 挡掉，不主动 reject 会永远悬着
  if (fail) fail(new Error('连接已关闭'))
  teardown(c)
  return true
}

// retained + QoS1 + 24h 自动过期；payload 传 null 表示删除 retained。
// 整条链路（建连 + 等 PUBACK）都在 publishTimeoutMs 预算内（见 config.js）；超时不取消
// 已发出的消息：叫号重发同一个 n 是幂等的，入队消息最坏留下一条无主的号（见 DESIGN.md）
const publishRetained = (topic, obj) =>
  withTimeout(connect().then(c => new Promise((resolve, reject) => {
    const opts = { qos: 1, retain: true }
    if (obj !== null) opts.properties = { messageExpiryInterval: config.retainTtlSec }
    c.publish(topic, obj === null ? '' : JSON.stringify(obj), opts, err => (err ? reject(err) : resolve()))
  })), config.publishTimeoutMs, '网络超时，请重试')

module.exports = {
  disconnect,
  watch,
  publishMeta: (sid, name) => publishRetained(t.meta(sid), { n: name, t: Date.now() }),
  publishCalled: (sid, n) => publishRetained(t.called(sid), { n, t: Date.now() }),
  publishJoin: (sid, cid, joinedAt) => publishRetained(t.join(sid, cid), { t: joinedAt }),
  // 客人号码定稿时上报，作为「已取号人数」的下界，供老板端取 max
  publishTotal: (sid, n) => publishRetained(t.total(sid), { n, t: Date.now() }),
}
