// 老板端队列详情 —— 展示二维码、当前进度、叫下一位
const store = require('../../utils/store.js')
const mqttc = require('../../utils/mqttc.js')
const qr = require('../../utils/qr.js')
const { config } = require('../../utils/config.js')

Page({
  data: {
    sid: '',
    name: '',
    total: 0,      // 已取号人数
    called: 0,     // 已叫到第几号
    waiting: 0,
    next: 1,
    offline: false,
    busy: false,
    counting: false,   // 正在重新统计人数（临时订 j/+）
    ended: false,      // 队列已过 retainTtlSec：broker 上什么都不剩了
    // 回放静默前 called 只是初值 0，这个标志挡「叫下一位」直到同步完（见 sync）
    synced: false,
  },

  onLoad(query) {
    const sid = (query && query.s) || ''
    const q = store.findQueue(sid)
    if (!q) {
      wx.showModal({ title: '队列不存在', content: '可能已过期', showCancel: false,
        complete: () => wx.navigateBack() })
      return
    }
    this.queueT = q.t
    // 本地保留 3 天而 broker 上 retained 只活 24h：点进来的队列可能已失效。失效就不订阅、
    // 不画二维码、不能叫号 —— 否则客人扫进一个 m/c 都已过期、没人叫号的队列干等。
    // 两端用同一个 store.isExpired（见 DESIGN.md「本地存储」）
    const ended = store.isExpired(q.t)
    // p 是本地记过的人数峰值（n 槽位会被慢半拍的客人写回更小值，重进页面时兜底）；
    // waiting 只是上界，called 要等回放，synced 之前界面显示「—」
    this.setData({ sid, name: q.n, ended, total: q.p || 0, waiting: ended ? 0 : q.p || 0 })
    wx.setNavigationBarTitle({ title: q.n })
  },

  // canvas type="2d" 节点要等首屏渲染完才查得稳（onLoad 里真机上可能拿到 null），
  // 二维码是核心功能，查不到就重试，不能只弹 toast
  onReady() {
    if (this.data.sid && !this.data.ended) this.drawQr(config.qrRetries)
  },

  drawQr(tries) {
    // 220 要和 host.wxss 里 .qr 的尺寸一致
    qr.drawTo(this, '#qr', qr.payloadFor(this.data.sid), 220).catch(() => {
      if (tries > 0) {
        this.qrTimer = setTimeout(() => this.drawQr(tries - 1), config.qrRetryMs)
        return
      }
      wx.showToast({ title: '二维码生成失败，请退出重进', icon: 'none' })
    })
  },

  // 只订 m/c/n：老板要的只是「已取号人数」标量，不订 j/+，不拉全量入队消息
  onShow() {
    // 失效判定每次回页面都要重算，不能只在 onLoad：这个页面 setKeepScreenOn 开着，
    // 是唯一可能跨过 24h 边界的页面。只在 onShow 重算、不挂定时器 —— 时间只会往前走，
    // 这个标志只会 false→true，而正在叫号时把按钮抽走比晚一步更糟
    if (!this.data.ended && store.isExpired(this.queueT)) this.setData({ ended: true, waiting: 0 })
    // 失效的队列 broker 上什么都不剩，订了也拿不到；顺手 disconnect（会话分钟按连接时长计费）
    if (!this.data.sid || this.data.ended) {
      this.stopWatch()
      mqttc.disconnect()
      return
    }
    if (this.watcher) return
    // 防息屏：老板要长时间举着屏幕给客人扫；切后台就还回去
    wx.setKeepScreenOn({ keepScreenOn: true })
    // 每次回前台都重新同步：切后台时连接已断开，回来是一次全新回放，且别的老板设备
    // 可能已叫过号（发号权可同时给多台设备），本机的 called 只是「上次看到的值」
    this.setData({ synced: false })
    this.watcher = mqttc.watch(this.data.sid, state => this.apply(state), { joins: false, total: true })
    // 订不上不解锁：callNext 发的是 called + 1，没订上就不知道 called（见 sync）
    this.watcher.ready.then(() => this.armSync(), () => this.setData({ offline: true }))
  },

  onHide() {
    this.stopWatch()
  },

  onUnload() {
    this.stopWatch()
    clearTimeout(this.qrTimer)
  },

  // 断开是安全的：clean:true + retained，重新订阅时 broker 会把队列状态全推回来
  stopWatch() {
    if (!this.watcher) return
    this.watcher.unwatch()
    this.watcher = null
    clearTimeout(this.recountTimer)
    clearTimeout(this.syncTimer)
    this.setData({ counting: false, synced: false })
    wx.setKeepScreenOn({ keepScreenOn: false })
    mqttc.disconnect()
  },

  // 人数只会涨：取上报值、本地峰值、自己叫到的号、重新统计数到的入队消息条数四者的最大值
  apply(state) {
    const total = Math.max(state.total, state.called, state.joins.length, this.data.total)
    if (total > (this.data.total || 0)) store.saveQueue({ s: this.data.sid, t: this.queueT, p: total })
    const next = {
      total,
      called: state.called,
      waiting: Math.max(0, total - state.called),
      next: state.called + 1,
      offline: !state.online,
    }
    // 值没变就不写：平时只有 m/c/n 三条消息无所谓，但「重新统计」会临时订回 j/+，
    // 那一下是每条入队消息一次 setData（大店 N=200）
    if (Object.keys(next).some(k => this.data[k] !== next[k])) this.setData(next)
    // 消息还在来说明回放没完，把静默窗口往后推。这里也是慢重试兜底的入口：ready 已经
    // reject（快重试用尽）但慢重试后来把订阅接回来时，第一条消息到达是唯一的信号
    if (!this.data.synced) this.armSync()
  },

  armSync() {
    if (this.data.synced) return
    clearTimeout(this.syncTimer)
    this.syncTimer = setTimeout(() => this.sync(), config.syncSettleMs)
  },

  // 回放安静了 syncSettleMs 就认为 m/c/n 到齐，解锁叫号。为什么必须有这道闸：called 不
  // 落盘，进页面时是 0 而 waiting 是本地峰值 p —— 一个已叫到头的队列在回放到达前按钮是
  // 亮的，点一下会发出 called+1=1，把 c 槽位覆盖成 1。在线的设备靠 max 合并没事，但之后
  // 才订阅的人（客人重开页面、新客人扫码）回放到的就是 1，要等下次真叫号才被纠正 ——
  // max 合并防的正是这种倒退，这是老板自己的陈旧设备把它捅穿了。和客人端 settle() 一样，
  // 「安静」只在连接活着时算数：断线同样会让消息停下来
  sync() {
    if (!this.watcher || this.data.synced) return
    if (!this.watcher.state.online) return this.armSync()
    this.setData({ synced: true })
  },

  // 「人数不对？重新统计」——「等待中」平时只有一个来源：客人定稿时上报的 n，丢一条就
  // 少算一位，只有一位客人时人数 0 会把叫号锁死。这里临时订回 j/+ 直接数入队消息（所有
  // 客人算号用的同一个集合，也是「一共发出去多少个号」的准确值；没来的幽灵号也算一个）。
  // 代价是一次全量回放，所以只在老板主动点的时候做，统计完就退订
  recount() {
    if (!this.watcher || this.data.counting) return
    this.setData({ counting: true })
    this.watcher.startJoins()
    clearTimeout(this.recountTimer)
    this.recountTimer = setTimeout(() => {
      if (this.watcher) this.watcher.stopJoins()
      this.setData({ counting: false })
    }, config.settleMs)
  },

  async callNext() {
    if (this.data.busy || !this.data.synced || this.data.waiting === 0) return
    const n = this.data.called + 1
    this.setData({ busy: true })
    try {
      await mqttc.publishCalled(this.data.sid, n)
      // 乐观更新；retained 消息回来后 apply 会再确认一次
      this.setData({ called: n, waiting: Math.max(0, this.data.total - n), next: n + 1 })
    } catch (err) {
      this.setData({ offline: true })
      wx.showToast({ title: '叫号失败，请重试', icon: 'none' })
    } finally {
      this.setData({ busy: false })
    }
  },
})
