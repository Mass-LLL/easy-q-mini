// 本地存储：按天分 key，只保留最近 config.keepDays 天，读列表时顺手清理过期 key。
const dayjs = require('dayjs')
const { config } = require('./config.js')

const CUSTOMER = 'eqs' // 我排过的
const HOST = 'eqh'     // 我创建的

const dayKey = (prefix, ts) => prefix + dayjs(ts).format('YYYYMMDD')

// broker 上 retained 按 retainTtlSec 自动过期，之后队列不再有任何状态更新：客人端标
// 「已结束」、老板端不画码不叫号。本地窗口（3 天）和 TTL（24h）不是一回事，两端必须
// 用同一个判定（见 DESIGN.md「本地存储」）。
const isExpired = ts => Date.now() - ts > config.retainTtlSec * 1000

// 删掉超出保留窗口的 key，返回窗口内的 key（新的在前）
const liveKeys = prefix => {
  const oldest = Number(dayjs().subtract(config.keepDays - 1, 'day').format('YYYYMMDD'))
  const keys = []
  wx.getStorageInfoSync().keys.forEach(k => {
    if (k.indexOf(prefix) !== 0) return
    const day = Number(k.slice(prefix.length))
    if (!day) return
    if (day >= oldest) keys.push({ k, day })
    else wx.removeStorageSync(k)
  })
  return keys.sort((a, b) => b.day - a.day).map(x => x.k)
}

// 返回 [{day: 'YYYYMMDD', items: [...]}]，新的一天在前
const listByDay = prefix =>
  liveKeys(prefix)
    .map(k => ({ day: k.slice(prefix.length), items: wx.getStorageSync(k) || [] }))
    .filter(g => g.items.length > 0)

const listAll = prefix => listByDay(prefix).reduce((acc, g) => acc.concat(g.items), [])

const find = (prefix, sid) => listAll(prefix).filter(x => x.s === sid)[0] || null

// 按 sid 插入或就地更新，写回它所属那一天的 key
const upsert = (prefix, record) => {
  const key = dayKey(prefix, record.t)
  const items = wx.getStorageSync(key) || []
  const idx = items.findIndex(x => x.s === record.s)
  if (idx === -1) items.push(record)
  else items[idx] = Object.assign({}, items[idx], record)
  wx.setStorageSync(key, items)
}

const remove = (prefix, sid) => {
  liveKeys(prefix).forEach(k => {
    const items = wx.getStorageSync(k) || []
    const left = items.filter(x => x.s !== sid)
    if (left.length !== items.length) wx.setStorageSync(k, left)
  })
}

module.exports = {
  isExpired,

  // 客人侧：{s: sid, n: 队列名, cid, t: 入队时间戳, q: 号码缓存, f: 定稿标记}
  listTicketsByDay: () => listByDay(CUSTOMER),
  listTickets: () => listAll(CUSTOMER),
  findTicket: sid => find(CUSTOMER, sid),
  saveTicket: t => upsert(CUSTOMER, t),
  removeTicket: sid => remove(CUSTOMER, sid),

  // 老板侧：{s: sid, n: 队列名, t: 创建时间戳, p: 已取号人数峰值}
  listQueuesByDay: () => listByDay(HOST),
  countQueuesToday: () => (wx.getStorageSync(dayKey(HOST, Date.now())) || []).length,
  findQueue: sid => find(HOST, sid),
  saveQueue: q => upsert(HOST, q),
  removeQueue: sid => remove(HOST, sid),
}
