// 测试脚手架：内存 MQTT broker（retained + 通配符）+ wx 桩 + 「每台设备一份模块实例」。
//
// 被 protocol.test.js（协议端到端）和 resilience.test.js（客户端健壮性）共用。
// utils/mqttc.js 是进程级单例，所以「另一台设备」只能靠清 require.cache + 换一份
// storage 来模拟。
const path = require('path')
const { EventEmitter } = require('events')

const ROOT = path.join(__dirname, '..')
const P = rel => path.join(ROOT, rel)

// ---------- 内存 broker ----------
const matches = (filter, topic) => {
  const f = filter.split('/'), t = topic.split('/')
  for (let i = 0; i < f.length; i++) {
    if (f[i] === '#') return true
    if (f[i] === '+') { if (t[i] === undefined) return false; continue }
    if (f[i] !== t[i]) return false
  }
  return f.length === t.length
}

const broker = {
  retained: new Map(),
  clients: [],
  // 故障注入：拒掉接下来几次 SUBSCRIBE（模拟 ACL 配错）
  failSubs: 0,
  // 故障注入：PUBLISH 收下但永不回 PUBACK（模拟连接半开 / 网络黑洞）
  stallPublish: false,
  // 故障注入：入队消息照常 retain，但一条都不投递
  //（模拟「PUBACK 回来了、消息确实在 broker 上，但自己始终没收到」—— 客人端的重发
  //   路径正是为这种情况准备的，也正是「重发不能覆盖旧那条」的理由）
  muteJoins: false,
  // 故障注入：接下来 n 次建连直接失败（模拟没网时 wx.connectSocket 根本打不通）。
  // 「没网」和「订阅被拒」是两条不同的路径：前者压根不会有 client，也就不会有 connect
  // 事件可以挂重试。
  failConns: 0,
  // 故障注入：SUBSCRIBE 时不回放 topic 里含这个片段的 retained（模拟「回放只到一半」）
  skipReplay: null,
  reset() {
    this.retained = new Map()
    this.clients = []
    this.failSubs = 0
    this.stallPublish = false
    this.muteJoins = false
    this.failConns = 0
    this.skipReplay = null
  },
  publish(topic, payload, opts) {
    if (opts && opts.retain) {
      if (payload.length === 0) this.retained.delete(topic)
      else this.retained.set(topic, payload)
    }
    if (this.muteJoins && topic.indexOf('/j/') !== -1) return
    this.clients.forEach(c => {
      if (c.subs.some(f => matches(f, topic))) c.emit('message', topic, Buffer.from(payload))
    })
  },
  subscribe(client, topics) {
    topics.forEach(f => {
      client.subs.push(f)
      this.retained.forEach((payload, topic) => {
        if (this.skipReplay && topic.indexOf(this.skipReplay) !== -1) return
        if (matches(f, topic)) client.emit('message', topic, Buffer.from(payload))
      })
    })
  },
  // 断线但**不销毁 client**：真实 mqtt.js 会自己重连，同一个 client 上会再来一次
  // 'connect'，而 clean session 让订阅全部作废 —— 这正是 mqttc 靠 filters 重建状态的前提。
  dropAll() {
    this.clients.forEach(c => { c.connected = false; c.subs = []; c.emit('close') })
  },
  reviveAll() {
    this.clients.forEach(c => { c.connected = true; c.emit('connect') })
  },
}

// 连不上的那种 client：mqtt.js 在 wx.connectSocket 失败时也是先给出 client 再 emit error，
// mqttc 会 teardown 掉它，所以它不进 broker.clients
const deadClient = () => {
  const c = new EventEmitter()
  c.connected = false
  c.subs = []
  c.subscribe = (topics, opts, cb) => cb && cb(new Error('未连接'))
  c.unsubscribe = () => {}
  c.publish = (topic, payload, opts, cb) => cb && cb(new Error('未连接'))
  c.end = (force, opts, cb) => {
    const fn = [force, opts, cb].filter(x => typeof x === 'function')[0]
    if (fn) setImmediate(fn)
  }
  process.nextTick(() => c.emit('error', new Error('建立连接失败')))
  return c
}

const fakeMqtt = {
  connect() {
    if (broker.failConns > 0) {
      broker.failConns--
      return deadClient()
    }
    const c = new EventEmitter()
    c.connected = false
    c.subs = []
    c.subscribe = (topics, opts, cb) => {
      if (broker.failSubs > 0) {
        broker.failSubs--
        return cb && cb(new Error('订阅被拒绝'))
      }
      broker.subscribe(c, [].concat(topics))
      cb && cb(null)
    }
    c.unsubscribe = topics => { c.subs = c.subs.filter(f => [].concat(topics).indexOf(f) === -1) }
    c.publish = (topic, payload, opts, cb) => {
      if (broker.stallPublish) return          // 消息收下了，PUBACK 永远不来
      broker.publish(topic, payload, opts)
      cb && cb(null)
    }
    // 断开要真的从 broker 上摘掉，否则测不出 disconnect（会话分钟按连接时长计费）。
    // 回调是异步的 —— 真实 mqtt.js 的 end 也要走 _cleanUp + nextTick，
    // mqttc 的「关完再开」串行化就依赖它。
    c.end = (force, opts, cb) => {
      c.connected = false
      broker.clients = broker.clients.filter(x => x !== c)
      const fn = [force, opts, cb].filter(x => typeof x === 'function')[0]
      if (fn) setImmediate(fn)
    }
    broker.clients.push(c)
    process.nextTick(() => { c.connected = true; c.emit('connect') })
    return c
  },
}

// ---------- wx 桩：storage 可按设备切换 ----------
let storage = new Map()
const scan = { result: null }
// canvas 桩：failTries 次查询先返回 null（模拟 canvas 2d 节点还没就绪）
const canvas = { failTries: 0, queries: 0, rects: 0, width: 0 }

global.wx = {
  // sid 现在只接受强随机源（utils/id.js），所以这两个必须是能用的
  canIUse: name => name === 'getRandomValues',
  getSystemInfoSync: () => ({ pixelRatio: 2 }),
  getStorageInfoSync: () => ({ keys: Array.from(storage.keys()) }),
  getStorageSync: k => (storage.has(k) ? JSON.parse(JSON.stringify(storage.get(k))) : ''),
  setStorageSync: (k, v) => storage.set(k, JSON.parse(JSON.stringify(v))),
  removeStorageSync: k => storage.delete(k),
  getRandomValues: o => {
    const bytes = new Uint8Array(o.length)
    for (let i = 0; i < o.length; i++) bytes[i] = Math.floor(Math.random() * 256)
    o.success({ randomValues: bytes.buffer })
  },
  showToast: () => {},
  showModal: o => { o && o.complete && o.complete(); return Promise.resolve({ confirm: true }) },
  navigateTo: () => {},
  navigateBack: () => {},
  setNavigationBarTitle: () => {},
  setKeepScreenOn: () => {},
  scanCode: () => (scan.result ? Promise.resolve({ result: scan.result }) : Promise.reject(new Error('cancel'))),
  createSelectorQuery: () => ({
    in: () => ({
      select: () => ({
        fields: () => ({
          exec: cb => {
            canvas.queries++
            if (canvas.failTries > 0) { canvas.failTries--; return cb([null]) }
            const ctx = { fillStyle: '', scale: () => {}, fillRect: () => { canvas.rects++ } }
            const node = { width: 0, height: 0, getContext: () => ctx }
            cb([{ node }])
            canvas.width = node.width
          },
        }),
      }),
    }),
  }),
}

// ---------- 每台「设备」= 一份独立的模块实例 + 独立 storage ----------
require.cache[require.resolve(P('libs/mqtt.js'))] = { id: 'mqtt', filename: 'mqtt', loaded: true, exports: fakeMqtt }

const UTILS = ['utils/config.js', 'utils/store.js', 'utils/mqttc.js', 'utils/qr.js', 'utils/id.js', 'utils/rank.js']

// 清掉 utils 的模块缓存并填好 broker 配置，返回这一份新的 config
const freshUtils = (extra = []) => {
  UTILS.concat(extra).forEach(r => delete require.cache[require.resolve(P(r))])
  const { config } = require(P('utils/config.js'))
  config.brokerUrl = 'wxs://test:8084/mqtt'
  config.username = 'u'
  config.password = 'p'
  return config
}

const makeDevice = (pageRel, tune) => {
  storage = new Map()
  const deviceStorage = storage
  const config = freshUtils([pageRel])
  if (tune) Object.assign(config, tune)

  let captured = null
  global.Page = obj => { captured = obj }
  require(P(pageRel))

  const page = Object.assign({}, captured)
  page.data = JSON.parse(JSON.stringify(captured.data))
  page.setData = function (d) { Object.assign(this.data, d) }
  page.__config = config
  page.__storage = deviceStorage
  page.__activate = () => { storage = deviceStorage }
  return page
}

const useStorage = m => { storage = m }
const tick = (n = 6) => new Promise(r => { const step = i => (i === 0 ? r() : setImmediate(() => step(i - 1))); step(n) })

module.exports = { ROOT, P, matches, broker, fakeMqtt, scan, canvas, freshUtils, makeDevice, useStorage, tick }
