// 我排过的 —— 客人端：扫码取号 + 实时查看自己的位置
const store = require('../../utils/store.js')
const mqttc = require('../../utils/mqttc.js')
const qr = require('../../utils/qr.js')
const { rankOf, aheadOf } = require('../../utils/rank.js')
const { newCid } = require('../../utils/id.js')
const { config, isConfigured } = require('../../utils/config.js')

// 静默期结束还看不到自己的入队消息，说明它没落到 broker 上，重新入队（见 rejoin）。
// 连试 MAX_TRIES 次仍拿不到号才判失败；同一数字也用作人数上报的重试次数（见 report）。
const MAX_TRIES = 3

// 定稿标记 f 只由 freezeRank() 打，含义是「静默窗口结束、集合已收敛」。缓存的 q 在回放
// 途中就会被改写，不能当定稿用（见 watchAll / DESIGN.md）。
//
// 超过 retainTtlSec 的票：broker 上的 retained 已过期，一条 topic 都不订，号码用 q 展示。
const isEnded = tk => store.isExpired(tk.t)

Page({
  data: {
    tab: 'active',     // active 正在排 / over 过号
    groups: [],        // 当前 tab 要显示的分组
    activeCount: 0,
    overCount: 0,
    configured: true,
    offline: false,
    busy: false,
  },

  onLoad() {
    this.states = {}       // sid -> mqtt 状态
    this.watchers = {}     // sid -> {unwatch, stopJoins}
    this.joinsOn = {}      // sid -> 是否还在订阅 j/+
    this.joinCounts = {}   // sid -> 上次看到的入队人数，用于判断回放是否还在继续
    this.tries = {}        // sid -> 已重发几次入队消息
    this.settleTimers = {}
    this.reportTimers = {} // sid -> 人数上报的重试计时器
    this.renderTimer = null
    this.dirty = {}        // sid -> 号码/队列名待落盘，跟着 renderTimer 一起合帧
    this.online = true     // 上一次看到的连接状态，用于识别「从断到通」那一下
    this.buckets = { active: [], over: [] }
  },

  onShow() {
    this.setData({ configured: isConfigured() })
    this.rebuild()
    if (isConfigured()) this.watchAll()
  },

  // 切到后台就退订并断开：会话分钟按连接时长计费，只退订是不省钱的
  onHide() {
    this.unwatchAll()
  },

  onUnload() {
    this.unwatchAll()
  },

  watchAll() {
    store.listTickets().forEach(tk => {
      if (this.watchers[tk.s]) return
      if (isEnded(tk)) return
      // 定稿的票不再订 j/+，只跟叫号进度。判据是 f 而不是 q：q 会把回放途中的中间值
      // 当成终值钉死（扫完码 5 秒内锁屏就会踩到）
      const needJoins = !tk.f
      const w = mqttc.watch(tk.s, state => this.onState(tk.s, state), { joins: needJoins })
      w.ready.catch(() => this.setData({ offline: true }))
      this.watchers[tk.s] = w
      this.joinsOn[tk.s] = needJoins
    })
  },

  unwatchAll() {
    Object.keys(this.watchers).forEach(sid => this.watchers[sid].unwatch())
    Object.keys(this.settleTimers).forEach(sid => clearTimeout(this.settleTimers[sid]))
    Object.keys(this.reportTimers).forEach(sid => clearTimeout(this.reportTimers[sid]))
    clearTimeout(this.renderTimer)
    this.renderTimer = null
    this.dirty = {}
    this.watchers = {}
    this.joinsOn = {}
    this.joinCounts = {}
    this.tries = {}
    this.settleTimers = {}
    this.reportTimers = {}
    mqttc.disconnect()
  },

  // 入队人数还在变说明 retained 还在回放，重新等一个静默窗口
  scheduleSettle(sid, state) {
    if (!this.joinsOn[sid]) return
    if (this.joinCounts[sid] === state.joins.length) return
    this.joinCounts[sid] = state.joins.length
    this.armSettle(sid)
  },

  armSettle(sid) {
    clearTimeout(this.settleTimers[sid])
    this.settleTimers[sid] = setTimeout(() => this.settle(sid), config.settleMs)
  },

  // 静默窗口到了：要么定稿，要么重新入队再等一轮
  settle(sid) {
    if (!this.joinsOn[sid]) return
    const tk = store.findTicket(sid)
    const state = this.states[sid]
    if (!tk || !state) return
    // 断线时不判定，只把截止时间往后推：这里唯一的判据是「消息不再来了」，而断线也会
    // 让消息不再来。分不清就两头出错 —— 回放中途断线会把偏小的号码永久定稿（撞号），
    // 页面开着时没网则会拿着空集合走上重发阶梯（换 cid 把自己挪到队尾）。见 DESIGN.md
    if (!state.online) return this.armSettle(sid)
    if (rankOf(state.joins, tk.cid) > 0) return this.freezeRank(sid)

    const tries = (this.tries[sid] || 0) + 1
    this.tries[sid] = tries
    if (tries <= MAX_TRIES) this.rejoin(sid, tk)
    this.rebuild()                // 把「第 N 次重试」/「没取到号」显示出来
  },

  // 重新入队：换新 cid + 新时间戳，等于往总序末尾纯追加。不能沿用旧 cid 覆盖原消息 ——
  // 万一它其实已落到 broker（只是本机没收到），覆盖 = 把集合中间的元素挪到末尾，已定稿
  // 的人不会重算，两个人拿到同一个号。换新 cid 最坏留一条无主的号（幽灵号），见 DESIGN.md
  async rejoin(sid, tk) {
    const cid = await newCid()
    if (!this.watchers[sid]) return   // 期间切了后台
    const t = Date.now()
    store.removeTicket(sid)     // cid/t 都变了，先删干净再写
    store.saveTicket({ s: sid, n: tk.n, cid, t, q: 0 })
    mqttc.publishJoin(sid, cid, t).catch(() => {})
    this.joinCounts[sid] = -1   // 让下一条消息一定重置计时
    this.armSettle(sid)
    this.rebuild()
  },

  // 号码定稿：打定稿标记、写本地缓存，然后退订 j/+。`f` 只在这里打。
  freezeRank(sid) {
    if (!this.joinsOn[sid]) return
    const tk = store.findTicket(sid)
    const state = this.states[sid]
    if (!tk || !state) return
    const rank = rankOf(state.joins, tk.cid)
    if (rank <= 0) return   // 还没看到自己的入队消息，继续等
    store.saveTicket({ s: sid, t: tk.t, q: rank, f: 1 })
    this.joinsOn[sid] = false
    if (this.watchers[sid]) this.watchers[sid].stopJoins()
    this.report(sid, rank, MAX_TRIES)
  },

  // 把自己的号上报为「已取号人数」的下界。老板端不订 j/+，这条丢了他就会少算 —— 队列里
  // 只有一位客人时更狠：人数 0 会把「叫下一位」锁死。失败要重试；几次仍不行就交给老板端
  // 的「重新统计」兜底
  report(sid, rank, tries) {
    mqttc.publishTotal(sid, rank).catch(() => {
      if (tries <= 0 || !this.watchers[sid]) return
      this.reportTimers[sid] = setTimeout(() => this.report(sid, rank, tries - 1), config.settleMs)
    })
  },

  onState(sid, state) {
    this.states[sid] = state
    // 离线横幅跟着连接状态走，值没变就不写：回放是一串消息，每条都 setData 一次，
    // 取号那一刻会堆出上百次跨线程调用（大店 N=200）
    const offline = !state.online
    if (this.data.offline !== offline) this.setData({ offline })
    // 从断到通：clean session 会重推全量 retained，重连前的静默计时器可能抢跑，整个重置
    if (state.online && !this.online) this.rearmAll()
    this.online = state.online

    this.scheduleSettle(sid, state)

    // 合并刷新 + 落盘都挂在 80ms 合帧上：store.findTicket 会走全量存储扫描，逐条做太贵
    this.dirty[sid] = true
    if (this.renderTimer) return
    this.renderTimer = setTimeout(() => {
      this.renderTimer = null
      Object.keys(this.dirty).forEach(s => this.cacheTicket(s))
      this.dirty = {}
      this.rebuild()
    }, 80)
  },

  // 把队列名和算出的号码缓存到本地（retained 过期后仍能展示）。注意 q 只是缓存：
  // 回放没结束时它是中间值，定稿由 freezeRank 负责
  cacheTicket(sid) {
    const tk = store.findTicket(sid)
    const state = this.states[sid]
    if (!tk || !state) return
    const rank = rankOf(state.joins, tk.cid)
    const patch = {}
    if (rank > 0 && rank !== tk.q) patch.q = rank
    if (state.name && state.name !== tk.n) patch.n = state.name
    if (Object.keys(patch).length > 0) store.saveTicket(Object.assign({ s: sid, t: tk.t }, patch))
  },

  // 重连后重置全部静默窗口：joinCounts 置 -1 让回放的第一条消息一定重新计时（重连后的
  // 回放内容可能和断线前一模一样，条数不变就不会触发 scheduleSettle）
  rearmAll() {
    Object.keys(this.joinsOn).forEach(sid => {
      if (!this.joinsOn[sid]) return
      this.joinCounts[sid] = -1
      this.armSettle(sid)
    })
  },

  // 按天分组，再按「正在排 / 过号」分桶。只把当前 tab 的那一份塞进 data。
  rebuild() {
    const buckets = { active: [], over: [] }
    store.listTicketsByDay().forEach(g => {
      const split = { active: [], over: [] }
      g.items.forEach(tk => {
        const vm = this.viewModel(tk)
        split[vm.over ? 'over' : 'active'].push(vm)
      })
      Object.keys(split).forEach(k => {
        if (split[k].length > 0) buckets[k].push({ day: g.day, items: split[k] })
      })
    })

    const count = gs => gs.reduce((n, g) => n + g.items.length, 0)
    this.buckets = buckets
    this.setData({
      groups: buckets[this.data.tab],
      activeCount: count(buckets.active),
      overCount: count(buckets.over),
    })
  },

  switchTab(e) {
    const tab = e.currentTarget.dataset.tab
    if (tab === this.data.tab) return
    this.setData({ tab, groups: this.buckets[tab] })
  },

  viewModel(tk) {
    const state = this.states[tk.s]
    // 优先用实时算出的号码，拿不到就退回本地缓存
    const rank = state ? rankOf(state.joins, tk.cid) || tk.q || 0 : tk.q || 0
    const called = state ? state.called : 0
    // 队列的 retained 消息已过期，状态不会再更新了，归档到「过号」tab
    const ended = isEnded(tk)

    let status = '取号中…'
    let arrived = false
    let over = false
    let failed = false
    if (ended) {
      status = '已结束'
      over = true
    } else if (rank <= 0) {
      // 号码还没算出来：等回放 / 重发中 / 彻底没取到
      const tries = this.tries[tk.s] || 0
      if (tries > MAX_TRIES) {
        failed = true
        status = '没取到号'
      } else if (tries > 0) {
        status = `取号中…（第 ${tries} 次重试）`
      }
    } else if (rank > 0) {
      if (called > rank) {
        // 过号不做任何自动处理：客人找店员说明，由店家决定要不要优先安排
        status = '已过号'
        over = true
      } else if (called === rank) {
        arrived = true
        status = '已到号，请前往'
      } else {
        const ahead = aheadOf(rank, called)
        status = ahead === 0 ? '下一位就是你' : `前面还有 ${ahead} 位`
      }
    }

    return {
      s: tk.s,
      name: tk.n || '队列',
      num: rank > 0 ? rank : '—',
      called,
      // 「已叫到 N 号」只在有号可比时才有意义（已结束/没取到号的票没有实时状态）
      showCalled: !ended && rank > 0,
      status,
      arrived,
      over,
      failed,
    }
  },

  // 抹掉一张票在本机的全部痕迹（本地记录 + 订阅 + 计时器）。不碰 broker 上的入队消息 ——
  // 这和「取消排队」不是一回事，消息只增不删（见 DESIGN.md）
  dropTicket(sid) {
    if (this.watchers[sid]) {
      this.watchers[sid].unwatch()
      delete this.watchers[sid]
    }
    clearTimeout(this.settleTimers[sid])
    clearTimeout(this.reportTimers[sid])
    delete this.reportTimers[sid]
    delete this.states[sid]
    delete this.tries[sid]
    delete this.joinsOn[sid]
    delete this.joinCounts[sid]
    delete this.settleTimers[sid]
    store.removeTicket(sid)
  },

  // 没取到号的票：本地记录直接删掉再扫一次
  async rescan(e) {
    const sid = e.currentTarget.dataset.sid
    this.dropTicket(sid)
    this.rebuild()
    await this.scan()
  },

  async scan() {
    if (this.data.busy) return
    let res
    try {
      res = await wx.scanCode({ scanType: ['qrCode'], onlyFromCamera: false })
    } catch (err) {
      return // 用户取消
    }

    const sid = qr.parsePayload(res.result)
    if (!sid) return wx.showToast({ title: '不是 EasyQ 的排队码', icon: 'none' })

    // 只有「正在排」的票拦重扫（防手滑拿两个号）。过号、已结束、没取到号的都放行：
    // 新 cid 是一条全新入队消息，只追加到总序末尾 —— 拦死会咬到「办完事当天再排一次」
    // 和「一家人共用一台手机」（见 DESIGN.md「不提供取消排队」）
    const old = store.findTicket(sid)
    const vm = old ? this.viewModel(old) : null
    if (vm && !vm.over && !vm.failed) {
      wx.showToast({ title: '你已经在这个队列里了', icon: 'none' })
      return
    }
    // 旧票留到新号发出去之后再换，publish 失败就什么都没动过；队列名留着免得卡片闪「队列」
    const keepName = (old && old.n) || ''

    this.setData({ busy: true })
    try {
      const cid = await newCid()
      const t = Date.now()
      // 入队 = 给自己发一条 retained 消息宣告存在，号码由各端本地算出
      await mqttc.publishJoin(sid, cid, t)
      if (old) this.dropTicket(sid)
      store.saveTicket({ s: sid, n: keepName, cid, t, q: 0 })
      this.rebuild()
      this.watchAll()
      wx.showToast({ title: '已入队，正在取号', icon: 'none' })
    } catch (err) {
      wx.showModal({
        title: '取号失败',
        content: '无法连接服务器，请检查网络后重试，或直接找店员。',
        showCancel: false,
      })
    } finally {
      this.setData({ busy: false })
    }
  },
})
