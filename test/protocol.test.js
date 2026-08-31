// 端到端：内存 MQTT broker（retained + 通配符）+ 真实页面代码
// 模拟一台老板设备和三台客人设备，验证「无发号权威」协议。
// broker / wx 桩 / 设备工厂都在 harness.js 里。
const assert = require('assert')
const { P, broker, scan, makeDevice, useStorage, tick } = require('./harness.js')

const sleep = ms => new Promise(r => setTimeout(r, ms))

// 哨兵：Promise 永远不 settle 时 node 会静默退出并返回 0，那样 npm test 会假绿
let finished = false
process.on('exit', code => {
  if (finished || code !== 0) return
  console.error('\n失败: 没跑到结尾就退出了 —— 大概有个 Promise 永远不会 settle')
  process.exitCode = 1
})

;(async () => {
  // ---------- 老板建队列 ----------
  const boss = makeDevice('pages/mine/mine.js')
  boss.onShow()
  boss.setData({ name: '大厅' })
  await boss.create()
  await tick()

  const bossStore = require(P('utils/store.js'))
  const queues = bossStore.listQueuesByDay()
  assert.strictEqual(queues[0].items.length, 1, '应创建 1 个队列')
  const sid = queues[0].items[0].s
  assert.ok(/^[0-9A-F]{32}$/.test(sid), 'sid 应为 32 位大写 hex，实际: ' + sid)
  assert.ok(broker.retained.has(`eq/${sid}/m`), '应广播队列信息')
  assert.ok(broker.retained.has(`eq/${sid}/c`), '应广播初始叫号')
  console.log('OK 老板建队列，sid =', sid.slice(0, 8) + '…')

  // ---------- 二维码内容可被正确解析 ----------
  const qr = require(P('utils/qr.js'))
  const payload = qr.payloadFor(sid)
  assert.strictEqual(qr.parsePayload(payload), sid, '二维码内容应能解回 sid')
  assert.strictEqual(qr.parsePayload('https://example.com'), null, '非 EasyQ 的码应被拒')
  console.log('OK 二维码载荷 =', payload, `(${payload.length} 字符)`)

  // ---------- 三个客人依次扫码取号 ----------
  const customers = []
  for (let i = 0; i < 3; i++) {
    const c = makeDevice('pages/queue/queue.js')
    c.__activate()
    c.onLoad()
    c.onShow()
    scan.result = payload
    await c.scan()
    // 三次入队的 t 必须严格递增：同一毫秒内扫码会平局，平局按随机 cid 决序，
    // 后面的 1/2/3 断言会偶发变成 [1,3,2]
    await sleep(5)
    await tick()
    customers.push(c)
  }

  // 让每个客人各自同步一次（切换 storage 后重新 onShow 触发 watch）
  for (const c of customers) { c.__activate(); c.onShow(); await tick() }
  await tick(10)

  const nums = customers.map(c => {
    c.__activate()
    c.rebuild()
    return c.data.groups[0].items[0].num
  })
  assert.deepStrictEqual(nums, [1, 2, 3], '三个客人应各自算出 1/2/3 号，实际: ' + JSON.stringify(nums))
  console.log('OK 三个客人本地各自算出号码:', nums.join(' / '))

  // 每个客人只看到自己的一张票
  customers.forEach((c, i) => {
    c.__activate()
    assert.strictEqual(c.data.groups[0].items.length, 1, `客人${i + 1} 应只有 1 张票`)
  })

  // ---------- 号码定稿：退订 j/+，并把自己的号上报为「已取号人数」的下界 ----------
  for (const c of customers) { c.__activate(); c.freezeRank(sid); await tick(4) }
  assert.ok(broker.retained.has(`eq/${sid}/n`), '应有一条人数 retained 消息')
  assert.strictEqual(JSON.parse(broker.retained.get(`eq/${sid}/n`)).n, 3, '最后定稿的应是 3 号')
  console.log('OK 三个客人各自上报号码，人数 retained 只有 1 条')

  // ---------- 老板叫号 ----------
  const host = makeDevice('pages/host/host.js', { syncSettleMs: 20 })
  useStorage(boss.__storage) // 老板设备上的队列记录
  host.__activate = () => useStorage(boss.__storage)
  host.onLoad({ s: sid })
  host.onShow()
  await tick(10)
  // 回放到齐之前叫号是锁的。进页面时 called 是 0（它不存本地），waiting 却是拿本地峰值
  // 垫上去的，所以一个已经叫到头的队列在这一瞬间按钮是亮的 —— 点下去发出的 called + 1
  // 会覆盖 `eq/{sid}/c` 那个唯一的 retained 槽位，把进度改小。见 host.js sync()。
  assert.strictEqual(host.data.synced, false, '刚订上、回放还没静默时不该认为已同步')
  await sleep(60)
  assert.strictEqual(host.data.synced, true, '回放静默后应解锁')
  assert.strictEqual(host.data.total, 3, '老板应看到 3 人取号，实际: ' + host.data.total)
  assert.strictEqual(host.data.waiting, 3)
  assert.strictEqual(host.data.next, 1, '下一位应是 1 号')
  // 只订 m/c/n，不拉全量入队消息
  const hostSubs = broker.clients[broker.clients.length - 1].subs
  assert.strictEqual(hostSubs.indexOf(`eq/${sid}/j/+`), -1, '老板端不应订阅 j/+，实际: ' + hostSubs.join(' '))
  assert.ok(hostSubs.indexOf(`eq/${sid}/n`) !== -1, '老板端应订阅人数 topic')
  assert.strictEqual(host.data.total, 3, '人数应来自 n，而不是 joins')

  await host.callNext()
  await tick(10)
  assert.strictEqual(host.data.called, 1)
  assert.strictEqual(host.data.waiting, 2)
  assert.strictEqual(host.data.next, 2)
  console.log('OK 老板叫号：已叫到', host.data.called, '等待', host.data.waiting)

  // ---------- 客人看到状态变化 ----------
  const check = (c, expect) => {
    c.__activate()
    c.rebuild()
    const vm = c.data.groups[0].items[0]
    assert.strictEqual(vm.status, expect, `期望「${expect}」实际「${vm.status}」`)
    return vm
  }
  for (const c of customers) { c.__activate(); c.onShow(); await tick(4) }
  await tick(10)

  const vm1 = check(customers[0], '已到号，请前往')
  assert.strictEqual(vm1.arrived, true)
  check(customers[1], '下一位就是你')
  check(customers[2], '前面还有 1 位')
  assert.strictEqual(customers[2].data.groups[0].items[0].called, 1, '应看到已叫到 1 号')
  console.log('OK 客人状态：1号=已到号  2号=下一位  3号=前面还有1位')

  // ---------- 没有取消排队：入队消息只增不删 ----------
  assert.strictEqual(typeof customers[0].leave, 'undefined', '客人端不应再有取消入口')
  const joinTopics = Array.from(broker.retained.keys()).filter(k => k.indexOf(`eq/${sid}/j/`) === 0)
  assert.strictEqual(joinTopics.length, 3, '三条入队消息应都还在，实际: ' + joinTopics.length)

  // ---------- 老板叫到 2 号，1 号变成过号 ----------
  host.__activate()
  await host.callNext()
  await tick(10)
  assert.strictEqual(host.data.called, 2)
  assert.strictEqual(host.data.total, 3, '过号不减少已取号人数')

  for (const c of customers) { c.__activate(); c.onShow(); await tick(4) }
  await tick(10)

  // 1 号落到「过号」tab，不再出现在「正在排」里
  const c1 = customers[0]
  c1.__activate()
  c1.rebuild()
  assert.strictEqual(c1.data.groups.length, 0, '过号的票不应留在「正在排」')
  assert.strictEqual(c1.data.activeCount, 0)
  assert.strictEqual(c1.data.overCount, 1)
  c1.switchTab({ currentTarget: { dataset: { tab: 'over' } } })
  const over1 = c1.data.groups[0].items[0]
  assert.strictEqual(over1.num, 1, '过号后号码不变，仍是 1 号')
  assert.strictEqual(over1.status, '已过号')
  assert.strictEqual(over1.over, true)
  console.log('OK 1号过号：落到「过号」tab，号码仍是', over1.num, '号')

  // 2 号已到号、3 号下一位，都还在「正在排」
  check(customers[1], '已到号，请前往')
  check(customers[2], '下一位就是你')
  assert.strictEqual(customers[1].data.activeCount, 1)
  assert.strictEqual(customers[1].data.overCount, 0)
  console.log('OK 2号=已到号  3号=下一位，均留在「正在排」')

  // ---------- 人数只涨不回退 ----------
  // 慢半拍的客人（先取号、后定稿）会把更小的号写进同一个 retained 槽位
  host.__activate()
  broker.publish(`eq/${sid}/n`, JSON.stringify({ n: 1, t: 1 }), { retain: true })
  await tick(6)
  assert.strictEqual(host.data.total, 3, '更小的上报值不应让人数回退，实际: ' + host.data.total)

  // 重进页面时 retained 已经是 1 了，靠本地记的峰值 p 兜底
  const host2 = makeDevice('pages/host/host.js')
  useStorage(boss.__storage)
  host2.__activate = () => useStorage(boss.__storage)
  host2.onLoad({ s: sid })
  host2.onShow()
  await tick(10)
  assert.strictEqual(host2.data.total, 3, '重进页面应用本地峰值兜底，实际: ' + host2.data.total)
  host2.onHide()
  console.log('OK 人数只涨不回退：max + 本地峰值兜底')

  // ---------- 定稿后重开只跟 c ----------
  const c3 = customers[2]
  c3.__activate()
  assert.strictEqual(bossStore.findTicket(sid).q, 3, '号码应缓存进本地')
  assert.strictEqual(c3.joinsOn[sid], false, '应已停止订阅 j/+')

  const before = broker.clients.length
  c3.onHide()
  assert.strictEqual(broker.clients.length, before - 1, '切后台应真的断开连接，不只是退订')

  c3.onShow()                              // 重新打开小程序
  await tick(10)
  c3.rebuild()
  const again = c3.data.groups[0].items[0]
  assert.strictEqual(again.num, 3, '重开后号码来自本地缓存，实际: ' + again.num)
  assert.strictEqual(again.status, '下一位就是你')
  const subs = broker.clients[broker.clients.length - 1].subs
  assert.strictEqual(subs.indexOf(`eq/${sid}/j/+`), -1, '重开后不应再订阅 j/+，实际: ' + subs.join(' '))
  assert.ok(subs.indexOf(`eq/${sid}/c`) !== -1, '仍应订阅叫号进度')
  console.log('OK 号码定稿后只订 c：重开仍是', again.num, '号，未回放任何入队消息')

  // ---------- 隔夜的票一条 topic 都不订 ----------
  c3.__activate()
  c3.onHide()
  const oldSid = 'A'.repeat(32)
  bossStore.saveTicket({ s: oldSid, n: '昨天的店', cid: 'B'.repeat(16), t: Date.now() - 25 * 3600 * 1000, q: 4 })
  c3.onShow()
  await tick(10)
  const subs2 = broker.clients[broker.clients.length - 1].subs
  assert.strictEqual(subs2.filter(f => f.indexOf(oldSid) !== -1).length, 0,
    '隔夜的票不应订阅任何 topic，实际: ' + subs2.join(' '))

  c3.rebuild()
  c3.switchTab({ currentTarget: { dataset: { tab: 'over' } } })
  const ended = c3.data.groups[0].items[0]
  assert.strictEqual(ended.status, '已结束')
  assert.strictEqual(ended.num, 4, '号码来自本地缓存 q，实际: ' + ended.num)
  console.log('OK 隔夜的票：不订阅、只用本地缓存显示「已结束」')

  // ---------- 入队消息没落到 broker：重发 3 次后判失败 ----------
  const c4 = makeDevice('pages/queue/queue.js')
  c4.__activate()
  c4.onLoad()
  c4.onShow()
  scan.result = payload
  await c4.scan()
  await tick(10)

  const tk4 = bossStore.findTicket(sid)
  const oldTopic = `eq/${sid}/j/${tk4.cid}`
  const oldPayload = broker.retained.get(oldTopic)
  const joinsBefore4 = Array.from(broker.retained.keys()).filter(k => k.indexOf(`eq/${sid}/j/`) === 0).length

  // 模拟「自己的入队消息没回来」：之后发出去的入队消息照常 retain 在 broker 上，
  // 但一条都不投递回来（就是「PUBACK 回来了、消息在 broker 上、自己却没收到」那种）。
  // 再清掉已经看到的 joins 和缓存的号码，静默期结束就会走重发。
  broker.muteJoins = true
  bossStore.saveTicket({ s: sid, t: tk4.t, q: 0 })
  c4.states[sid].joins = []
  // 线上每轮间隔 5 秒，测试里跑得太快，手动推进时钟
  const realNow = Date.now
  let skew = 5000
  Date.now = () => realNow() + skew

  c4.settle(sid)
  await tick(4)
  assert.strictEqual(c4.tries[sid], 1, '第一次静默应触发重发')
  // 重发必须换新 cid + 新时间戳，也就是纯追加。沿用旧 cid 去覆盖那条消息，等于把一个
  // 可能已经在集合中间的元素挪到末尾：后面每个人 rank 前移一位，已定稿的人不会重算，
  // 于是两个人拿到同一个号。
  const re4 = bossStore.findTicket(sid)
  assert.notStrictEqual(re4.cid, tk4.cid, '重发应换一个新 cid')
  assert.ok(re4.t > tk4.t, '重发应换成新时间戳，实际: ' + re4.t + ' vs ' + tk4.t)
  assert.strictEqual(broker.retained.get(oldTopic), oldPayload, '旧那条入队消息一个字节都不能动')
  assert.strictEqual(bossStore.listTickets().filter(x => x.s === sid).length, 1, '不应留下重复的票')
  c4.rebuild()
  assert.strictEqual(c4.data.groups[0].items[0].status, '取号中…（第 1 次重试）')

  for (let i = 0; i < 3; i++) { skew += 5000; c4.settle(sid); await tick(4) }
  Date.now = realNow
  broker.muteJoins = false
  c4.rebuild()
  const vm4 = c4.data.groups[0].items[0]
  assert.strictEqual(c4.tries[sid], 4, '重发 3 次后不再重发')
  assert.strictEqual(vm4.failed, true)
  assert.strictEqual(vm4.status, '没取到号')
  assert.strictEqual(vm4.num, '—')
  // 每次重发都是一条新 cid 的消息。代价是最坏留下 3 条无主的入队消息（幽灵号），
  // 这正是「不能覆盖旧那条」换来的 —— 幽灵号老板叫到没人应就过了，撞号不会自愈。
  const joinsAfter4 = Array.from(broker.retained.keys()).filter(k => k.indexOf(`eq/${sid}/j/`) === 0)
  assert.strictEqual(joinsAfter4.length, joinsBefore4 + 3, '3 次重发应留下 3 条新的入队消息')
  assert.ok(JSON.parse(broker.retained.get(`eq/${sid}/j/${bossStore.findTicket(sid).cid}`)).t > tk4.t,
    '最后一条广播出去的入队消息应带新时间戳')
  console.log('OK 取不到号：重发 3 次（每次换新 cid + 新时间戳）→ 显示「没取到号」+ 重新扫码入口')

  // 重新扫码：只删本地记录，不动 broker 上的任何入队消息
  const retainedBefore = broker.retained.size
  c4.__activate()
  scan.result = null                        // 让 scanCode 直接失败，只验证清理部分
  await c4.rescan({ currentTarget: { dataset: { sid } } })
  assert.ok(!bossStore.findTicket(sid), '本地票应已删除')
  assert.strictEqual(broker.retained.size, retainedBefore, '不应删除 broker 上的入队消息')
  c4.onHide()
  console.log('OK 重新扫码只清本地，broker 上的消息一条没动')

  // ---------- 叫号计数只涨不回退（多台老板端并发叫号）----------
  // 文档说「发号权可以同时给任意多台设备」，前提就是这里取 max：单个 retained 槽位是
  // 后写覆盖的，慢半拍的那台会把计数打回去，客人端的状态就从「已到号」退回「下一位」。
  host.__activate()
  broker.publish(`eq/${sid}/c`, JSON.stringify({ n: 1, t: 1 }), { retain: true })
  await tick(6)
  assert.strictEqual(host.data.called, 2, '更小的叫号值不应让计数回退，实际: ' + host.data.called)
  assert.strictEqual(host.data.next, 3, '下一位应仍是 3 号')
  console.log('OK 叫号计数只涨不回退：并发叫号时慢半拍的那条不会把进度打回去')

  // ---------- 坏 payload 不能毒化界面 ----------
  // sid 对队列里每个人都可见，共享凭证又在客户端里，所以任何客人都能往这些 topic 发东西。
  // 伪造数字是已接受的缺口，但非数值会顺着 Math.max / aheadOf 变成 NaN，全局界面都得跟着烂。
  // 3.5 会变成「叫下一位（4.5 号）」，1e308 会让全队客人永久显示「已过号」——
  // 既然已经在校验类型，顺手把非整数和荒谬量级也挡掉
  const poison = ['{"n":"abc"}', '{"n":null}', '{"n":{}}', 'not json', '{"n":9e999}',
    '{"n":3.5}', '{"n":1e308}', '{"n":-1}']
  for (const bad of poison) {
    broker.publish(`eq/${sid}/c`, bad, { retain: true })
    broker.publish(`eq/${sid}/n`, bad, { retain: true })
    broker.publish(`eq/${sid}/m`, bad, { retain: true })
    await tick(4)
    assert.strictEqual(host.data.called, 2, `坏 payload ${bad} 改动了 called: ` + host.data.called)
    assert.strictEqual(host.data.total, 3, `坏 payload ${bad} 改动了 total: ` + host.data.total)
    ;['called', 'total', 'waiting', 'next'].forEach(k => {
      assert.ok(Number.isInteger(host.data[k]) && host.data[k] >= 0,
        `坏 payload ${bad} 让 ${k} 变成了 ` + host.data[k])
    })
  }
  // 队列名被伪造成非字符串时只当作没有，不能把对象塞进标题
  broker.publish(`eq/${sid}/m`, JSON.stringify({ n: '大厅', t: 1 }), { retain: true })
  await tick(4)
  console.log('OK 坏 payload 一律丢弃：called/total/waiting/next 只会是非负整数')

  // ---------- 过号的票可以重新取号 ----------
  // 办完事当天想再排一次、一家人共用一台手机，都会走到这里。协议上就是一条新 cid 的
  // 入队消息，追加到总序末尾，谁的号都不受影响。
  const c1r = customers[0]
  c1r.__activate()
  const beforeCid = bossStore.findTicket(sid).cid
  const joinsBefore = Array.from(broker.retained.keys()).filter(k => k.indexOf(`eq/${sid}/j/`) === 0).length
  assert.strictEqual(c1r.viewModel(bossStore.findTicket(sid)).over, true, '1 号应处于过号态')
  scan.result = payload
  await c1r.scan()
  await tick(10)

  const reTk = bossStore.findTicket(sid)
  assert.ok(reTk, '重新取号后应有一张票')
  assert.notStrictEqual(reTk.cid, beforeCid, '重新取号必须换新 cid（新 topic = 追加到队尾）')
  assert.strictEqual(bossStore.listTickets().filter(x => x.s === sid).length, 1, '同一队列本地只留一张票')
  const joinsAfter = Array.from(broker.retained.keys()).filter(k => k.indexOf(`eq/${sid}/j/`) === 0)
  assert.strictEqual(joinsAfter.length, joinsBefore + 1, '旧的入队消息一条都不能删，只能多一条')
  assert.ok(joinsAfter.indexOf(`eq/${sid}/j/${beforeCid}`) !== -1, '旧 cid 的入队消息应仍在 broker 上')
  c1r.rebuild()
  c1r.switchTab({ currentTarget: { dataset: { tab: 'active' } } })   // 上面切到过号 tab 看过了
  const reVm = c1r.data.groups[0].items[0]
  assert.ok(reVm.num > 1, '重新取号应拿到队尾的新号，实际: ' + reVm.num)

  // 正在排的票仍然拦重扫，避免手滑扫两次拿两个号
  await c1r.scan()
  await tick(4)
  assert.strictEqual(bossStore.findTicket(sid).cid, reTk.cid, '正在排队时重扫不应换号')
  console.log('OK 过号的票可以重新取号（新 cid、拿队尾的', reVm.num, '号），正在排的仍被拦住')

  // ---------- 没定稿就切后台：重开要重新订 j/+，把中间号码纠正回来 ----------
  // 号码只有在静默窗口结束（freezeRank）之后才算定稿。回放途中缓存的 q 是个中间值，
  // 拿它当「不必再订 j/+」的依据，会把中间号码永久钉死 —— 扫完码 5 秒内锁屏就会踩到。
  boss.__activate()
  boss.onShow()
  boss.setData({ name: '二店' })
  await boss.create()
  await tick(6)
  const sid2 = bossStore.listQueuesByDay()[0].items.map(q => q.s).filter(x => x !== sid)[0]
  boss.onHide()

  const slow = makeDevice('pages/queue/queue.js', { settleMs: 100000 })  // 静默窗口远没到
  slow.__activate()
  slow.onLoad()
  slow.onShow()
  scan.result = qr.payloadFor(sid2)
  await slow.scan()
  await tick(10)
  await sleep(100)   // 号码缓存挂在 80ms 的合帧上（见 queue.js onState / cacheTicket）

  const slowStore = require(P('utils/store.js'))
  const tkSlow = slowStore.findTicket(sid2)
  assert.strictEqual(tkSlow.q, 1, '回放里只看到自己 → 缓存了一个中间值 1')
  assert.ok(!tkSlow.f, '静默窗口还没到，不该算定稿')

  slow.onHide()                                   // 锁屏 / 切后台，freezeRank 从没跑过
  // 另一位客人的入队消息此刻才落到 broker，而且 t 比我早（两人对着同一块屏幕同时扫，
  // 对方的 PUBACK 慢半拍）
  broker.publish(`eq/${sid2}/j/${'E'.repeat(16)}`, JSON.stringify({ t: tkSlow.t - 1 }), { retain: true })
  slow.onShow()                                   // 重新打开小程序
  await tick(10)
  await sleep(100)
  const slowSubs = broker.clients[broker.clients.length - 1].subs
  assert.ok(slowSubs.indexOf(`eq/${sid2}/j/+`) !== -1, '没定稿的票重开必须重新订 j/+，实际: ' + slowSubs.join(' '))
  slow.rebuild()
  assert.strictEqual(slow.data.groups[0].items[0].num, 2, '重开后要重算：真实 rank 是 2')
  slow.onHide()
  console.log('OK 没定稿就重开：重新订 j/+ 并把中间号码 1 纠正成 2')

  // ---------- 收尾：全部断开（也清掉挂着的静默计时器）----------
  for (const c of customers) { c.__activate(); c.onHide() }
  host.__activate()
  host.onHide()
  boss.__activate()
  boss.onShow()      // 用户回到「我创建的」
  boss.onHide()      // 再切走：这个页面自己不订阅任何东西，离开就该断开
  await tick(6)
  assert.strictEqual(broker.clients.length, 0, '全部切后台后不该剩下连接，实际: ' + broker.clients.length)
  console.log('OK 全部切后台后剩余连接:', broker.clients.length)

  finished = true
  console.log('\n端到端全部通过')
})().catch(e => { console.error('\n失败:', e.stack); process.exit(1) })
