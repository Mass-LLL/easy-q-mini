// 我创建的 —— 老板端队列列表 + 建队列
const store = require('../../utils/store.js')
const mqttc = require('../../utils/mqttc.js')
const { newSid } = require('../../utils/id.js')
const { config, isConfigured } = require('../../utils/config.js')

Page({
  data: {
    groups: [],
    configured: true,
    creating: false,
    name: '',
    busy: false,
    atLimit: false,
    maxPerDay: config.maxQueuesPerDay,
    openSid: '',   // 左滑露出删除按钮的那一行
  },

  onShow() {
    this.navigating = false
    this.setData({ configured: isConfigured() })
    this.refresh()
  },

  // 建队列会开一条连接，而这个页面自己不订阅：离开就断开（disconnect 自带「还有 watcher
  // 就不关」的守卫）。唯一例外是刚建完跳去 host —— 那边马上要用同一条连接
  onHide() {
    if (!this.navigating) mqttc.disconnect()
  },

  refresh() {
    // 本地保留 3 天而 retained 只活 24h，列表里可能有一大半已失效，行上要标出来
    const groups = store.listQueuesByDay().map(g => ({
      day: g.day,
      items: g.items.map(q => Object.assign({}, q, { ended: store.isExpired(q.t) })),
    }))
    this.setData({
      groups,
      openSid: '',
      atLimit: store.countQueuesToday() >= config.maxQueuesPerDay,
      maxPerDay: config.maxQueuesPerDay,
    })
  },

  openCreate() {
    if (this.reachedDailyLimit()) return
    this.setData({ creating: true, name: '' })
  },

  // 每天创建上限，删掉队列会腾出名额（本地计数，仅防手滑）
  reachedDailyLimit() {
    if (store.countQueuesToday() < config.maxQueuesPerDay) return false
    wx.showToast({ title: `每天最多创建 ${config.maxQueuesPerDay} 个队列`, icon: 'none' })
    return true
  },

  cancelCreate() {
    this.setData({ creating: false, name: '' })
  },

  onNameInput(e) {
    this.setData({ name: e.detail.value })
  },

  async create() {
    const name = this.data.name.trim()
    if (!name) return wx.showToast({ title: '请输入队列名', icon: 'none' })
    if (this.data.busy) return
    if (this.reachedDailyLimit()) return this.setData({ creating: false, name: '' })

    this.setData({ busy: true })
    try {
      const sid = await newSid()
      const t = Date.now()
      // 先广播队列信息和初始叫号，客人扫码后才能拿到队列名
      await mqttc.publishMeta(sid, name)
      await mqttc.publishCalled(sid, 0)
      store.saveQueue({ s: sid, n: name, t })
      this.setData({ creating: false, name: '' })
      this.refresh()
      this.navigating = true
      wx.navigateTo({ url: `/pages/host/host?s=${sid}` })
    } catch (err) {
      wx.showModal({
        title: '创建失败',
        content: (err && err.message) || '无法连接服务器，请检查网络',
        showCancel: false,
      })
    } finally {
      this.setData({ busy: false })
    }
  },

  // ---- 左滑露出删除 ----
  // 横向位移超过阈值且明显大于纵向位移才算左滑，避免和列表竖向滚动打架
  onSwipeStart(e) {
    const p = e.touches[0]
    this.swipe = { x: p.clientX, y: p.clientY, sid: e.currentTarget.dataset.sid }
  },

  onSwipeMove(e) {
    if (!this.swipe) return
    const p = e.touches[0]
    const dx = p.clientX - this.swipe.x
    const dy = p.clientY - this.swipe.y
    if (Math.abs(dx) < 20 || Math.abs(dx) < Math.abs(dy)) return

    const next = dx < 0 ? this.swipe.sid : ''
    if (next !== this.data.openSid) this.setData({ openSid: next })
  },

  onSwipeEnd() {
    this.swipe = null
  },

  // 点空白处收起（卡片和按钮的点击也会冒泡到这里）
  closeSwipe() {
    if (this.data.openSid) this.setData({ openSid: '' })
  },

  // 只删本机记录：队列名和叫号进度是 broker 上的 retained 消息，24 小时后自然过期，
  // 已在排队的客人不受影响（他们的号由各端本地算出，见 DESIGN.md）
  async removeQueue(e) {
    const sid = e.currentTarget.dataset.sid
    const q = store.findQueue(sid)
    if (!q) return

    const confirm = await wx.showModal({
      title: `删除「${q.n}」`,
      content: '删除后这个队列不再显示，二维码 24 小时后失效。已取号的客人仍能看到自己的号。',
      confirmText: '删除',
      confirmColor: '#FA5151',
    })
    if (!confirm.confirm) return

    store.removeQueue(sid)
    this.refresh()
    wx.showToast({ title: '已删除', icon: 'none' })
  },

  openQueue(e) {
    // 有行处于左滑打开状态时，这一下点击只用来收起（收起由 closeSwipe 冒泡处理）
    if (this.data.openSid) return
    this.navigating = true
    wx.navigateTo({ url: `/pages/host/host?s=${e.currentTarget.dataset.sid}` })
  },

  // 吃掉弹窗内部的点击，避免冒泡到遮罩把弹窗关掉
  noop() {},
})
