// Node 桩测试：验证 vendored 库可 require，以及 rank/store 的纯逻辑
const assert = require('assert')
const ROOT = require('path').join(__dirname, '..')

// ---- wx 桩 ----
const storage = new Map()
global.wx = {
  canIUse: () => false,
  getStorageInfoSync: () => ({ keys: Array.from(storage.keys()) }),
  getStorageSync: k => (storage.has(k) ? JSON.parse(JSON.stringify(storage.get(k))) : ''),
  setStorageSync: (k, v) => storage.set(k, JSON.parse(JSON.stringify(v))),
  removeStorageSync: k => storage.delete(k),
  getRandomValues: o => o.fail && o.fail(),
}

// ---- vendored 的 mqtt.js 必须真的走 wxs transport ----
// 光断言 `typeof mqtt.connect === 'function'` 是不够的：mqtt.js 4.3.8 只有在认为自己是
// webpack 产物时才注册 wx/wxs，否则 `wxs://` 会落到 node 的 tcp transport 上，第一次
// connect 就抛 `net.createConnection is not a function`。而这一步只有真正调用 connect()
// 才看得见 —— 之前那条 require 断言从头到尾没碰过协议表，所以这个 bug 一路走到了真机。
//
// 必须先 require utils/mqttc.js（它负责在 require bundle 之前立起那面旗子），这样这条
// 用例验的就是**生产环境真实的 require 顺序**，而不是测试自己另搭的一套。
const socketCalls = []
const sentFrames = []
global.wx.connectSocket = o => {
  socketCalls.push(o)
  const task = {}
  ;['onMessage', 'onClose', 'onError'].forEach(k => { task[k] = () => {} })
  // 必须真的把「连上了」这一步走完：不 onOpen 的话 duplexify 一直没有可写端，
  // CONNECT 报文永远卡在缓冲里，下面的字节校验就变成了对空数组的断言
  task.onOpen = f => f()
  task.send = ({ data, success }) => { sentFrames.push(Buffer.from(data)); success && success() }
  task.close = ({ success }) => success && success()
  return task
}
require(ROOT + '/utils/mqttc.js')
const mqtt = require(ROOT + '/libs/mqtt.js')
assert.strictEqual(typeof mqtt.connect, 'function', 'mqtt.connect 应可用')

const client = mqtt.connect('wxs://example.emqxsl.cn:8084/mqtt', { protocolVersion: 5, clean: true })
client.on('error', () => {})
assert.strictEqual(socketCalls.length, 1, 'wxs:// 应当走 wx.connectSocket，而不是 tcp transport')
assert.strictEqual(socketCalls[0].url, 'wss://example.emqxsl.cn:8084/mqtt', 'wxs 应映射成 wss 并保留端口与路径')
assert.deepStrictEqual(socketCalls[0].protocols, ['mqtt'], 'WebSocket 子协议必须是 mqtt，否则 EMQX 会拒握手')

// 发出去的字节必须逐字节正确。wx transport 原样发 `chunk.buffer`（整块底层 ArrayBuffer），
// byteOffset/byteLength 全丢 —— mqtt-packet 写变长整数时是 allocUnsafe(4) 再 slice(0,1)，
// 于是 1 字节的「剩余长度」会被当 4 字节发出去，CONNECT 里凭空多出 `00 00 00`。
// broker 解不出协议名，闭连接、不回 CONNACK，客户端只能干等到 connectTimeout。
// 这条 bug 在「能连上吗」这种断言下完全隐形：wx.connectSocket 照样被调用、URL 和子协议
// 也都对，只有把字节拼起来数长度才看得见。
//
// 报文是经 duplexify 流出去的，落到 send() 上要等下一个 tick，所以这段只能延后跑；
// 延后的断言不跑也不会让测试变红，因此配一个退出哨兵。
let bytesChecked = false
process.on('exit', code => {
  if (code === 0 && !bytesChecked) {
    console.error('CONNECT 字节校验根本没跑到 —— transport 可能没把报文发出来')
    process.exitCode = 1
  }
})
setImmediate(() => {
  const wire = Buffer.concat(sentFrames)
  assert.strictEqual(wire[0], 0x10, '第一个报文应当是 CONNECT')
  let remLen = 0, mul = 1, i = 1
  do { remLen += (wire[i] & 0x7f) * mul; mul *= 128 } while (wire[i++] & 0x80)
  assert.strictEqual(
    wire.length, i + remLen,
    '发出的字节数应恰好等于「固定头 + 剩余长度」—— 多出来就是 transport 把 chunk 后面的填充也发了'
  )
  assert.strictEqual(
    wire.slice(i, i + 6).toString('hex'), '00044d515454',
    '剩余长度之后必须紧跟协议名 MQTT'
  )
  bytesChecked = true
  client.end(true)
})

const qrcode = require(ROOT + '/libs/qrcode.js')
assert.strictEqual(typeof qrcode, 'function', 'qrcode 应为函数')
console.log('OK libs: wxs:// 走的是 wx.connectSocket（wss + 子协议 mqtt），CONNECT 字节无填充，qrcode 可用')

// ---- QR: 35 字符 alnum 载荷 ----
const qr = qrcode(0, 'M')
qr.addData('EQ1' + 'A1B2C3D4E5F60718293A4B5C6D7E8F90', 'Alphanumeric')
qr.make()
assert.strictEqual(qr.getModuleCount(), 25, 'sid 载荷应落在 version 2 (25x25)')
console.log('OK qr: 25x25')

// ---- rank ----
const { sortJoins, rankOf, aheadOf } = require(ROOT + '/utils/rank.js')
const joins = [
  { cid: 'FF', t: 300 },
  { cid: 'AA', t: 100 },
  { cid: 'CC', t: 200 },
]
assert.deepStrictEqual(sortJoins(joins).map(j => j.cid), ['AA', 'CC', 'FF'])
assert.strictEqual(rankOf(joins, 'AA'), 1)
assert.strictEqual(rankOf(joins, 'FF'), 3)
assert.strictEqual(rankOf(joins, 'NOPE'), 0)

// 全序确定性：任意输入顺序都算出同一结果
const shuffled = [joins[2], joins[0], joins[1]]
assert.deepStrictEqual(sortJoins(shuffled).map(j => j.cid), sortJoins(joins).map(j => j.cid))

// 时间戳相同时 cid 字典序兜底
const tie = [{ cid: 'B', t: 5 }, { cid: 'A', t: 5 }]
assert.deepStrictEqual(sortJoins(tie).map(j => j.cid), ['A', 'B'])

// aheadOf: called=n 表示 1..n 已叫过
assert.strictEqual(aheadOf(3, 0), 2, '3号、还没叫号 -> 前面 2 位')
assert.strictEqual(aheadOf(3, 2), 0, '3号、已叫到2 -> 轮到我了')
assert.strictEqual(aheadOf(3, 5), 0, '已叫过头 -> 0')
assert.strictEqual(aheadOf(1, 0), 0, '1号 -> 前面 0 位')
console.log('OK rank: 排序确定性 / 平局兜底 / aheadOf 边界')

// ---- store ----
const store = require(ROOT + '/utils/store.js')
const dayjs = require(ROOT + '/node_modules/dayjs')
const now = Date.now()

store.saveQueue({ s: 'SID1', n: '一号窗口', t: now })
store.saveQueue({ s: 'SID2', n: '二号窗口', t: now })
assert.strictEqual(store.findQueue('SID1').n, '一号窗口')
assert.strictEqual(store.listQueuesByDay()[0].items.length, 2)

// upsert 就地更新，不重复插入
store.saveQueue({ s: 'SID1', n: '改名了', t: now })
assert.strictEqual(store.listQueuesByDay()[0].items.length, 2, 'upsert 不应重复插入')
assert.strictEqual(store.findQueue('SID1').n, '改名了')

// 保留窗口：keepDays=3，第 4 天前的 key 应被清掉
const old = dayjs().subtract(5, 'day').valueOf()
storage.set('eqh' + dayjs(old).format('YYYYMMDD'), [{ s: 'OLD', n: '过期', t: old }])
const twoDaysAgo = dayjs().subtract(2, 'day').valueOf()
storage.set('eqh' + dayjs(twoDaysAgo).format('YYYYMMDD'), [{ s: 'KEEP', n: '前天', t: twoDaysAgo }])
const days = store.listQueuesByDay()
assert.ok(!store.findQueue('OLD'), '5 天前的应被清理')
assert.ok(store.findQueue('KEEP'), '前天的应保留')
assert.ok(!storage.has('eqh' + dayjs(old).format('YYYYMMDD')), '过期 key 应从 storage 删除')
// 新的一天在前
assert.deepStrictEqual(days.map(g => g.day), [...days.map(g => g.day)].sort().reverse())

// countQueuesToday 只数今天，不含前天
assert.strictEqual(store.countQueuesToday(), 2, '今天 2 个（SID1/SID2），不含前天的 KEEP')

// removeQueue：删掉后腾出当天名额
store.saveQueue({ s: 'SID3', n: '三号窗口', t: now })
assert.strictEqual(store.countQueuesToday(), 3)
store.removeQueue('SID3')
assert.ok(!store.findQueue('SID3'), 'removeQueue 应生效')
assert.strictEqual(store.countQueuesToday(), 2, '删除后名额回落')

// 客人侧独立命名空间
store.saveTicket({ s: 'SID1', n: '一号窗口', cid: 'C1', t: now, q: 7 })
assert.strictEqual(store.findTicket('SID1').q, 7)
assert.strictEqual(store.findQueue('SID1').n, '改名了', '两个命名空间不应互相污染')

store.removeTicket('SID1')
assert.ok(!store.findTicket('SID1'), 'removeTicket 应生效')
assert.ok(store.findQueue('SID1'), 'removeTicket 不应动老板侧')
console.log('OK store: upsert / 3天保留窗口 / 命名空间隔离 / 每日计数与删除')

console.log('\n全部通过')
