// 客户端生成二维码，画到 Canvas 2D 上，不需要服务端。
const qrcode = require('../libs/qrcode.js')
const { config } = require('./config.js')

// 二维码内容 = qrPrefix + sid，35 字符全在 QR alphanumeric 字符集内 → version 2（25×25），
// 从反光手机屏上扫的成功率高（见 DESIGN.md「二维码」）
const payloadFor = sid => config.qrPrefix + sid

const parsePayload = text => {
  if (typeof text !== 'string') return null
  const s = text.trim().toUpperCase()
  if (s.indexOf(config.qrPrefix) !== 0) return null
  const sid = s.slice(config.qrPrefix.length)
  return /^[0-9A-F]{32}$/.test(sid) ? sid : null
}

const pixelRatio = () =>
  (wx.canIUse('getWindowInfo') ? wx.getWindowInfo() : wx.getSystemInfoSync()).pixelRatio || 2

// selector 形如 '#qr'，sizePx 为逻辑像素边长
const drawTo = (page, selector, text, sizePx) =>
  new Promise((resolve, reject) => {
    const draw = node => {
      const qr = qrcode(0, 'M')
      qr.addData(text, 'Alphanumeric')
      qr.make()
      const count = qr.getModuleCount()

      const dpr = pixelRatio()
      node.width = sizePx * dpr
      node.height = sizePx * dpr
      const ctx = node.getContext('2d')
      ctx.scale(dpr, dpr)

      ctx.fillStyle = '#FFFFFF'
      ctx.fillRect(0, 0, sizePx, sizePx)

      const quiet = 2 // 静默区，太小会扫不出来
      const cell = sizePx / (count + quiet * 2)
      ctx.fillStyle = '#000000'
      for (let r = 0; r < count; r++) {
        for (let c = 0; c < count; c++) {
          if (!qr.isDark(r, c)) continue
          // 多画 0.5px 避免相邻格子之间出现缝隙
          ctx.fillRect((c + quiet) * cell, (r + quiet) * cell, cell + 0.5, cell + 0.5)
        }
      }
    }

    wx.createSelectorQuery()
      .in(page)
      .select(selector)
      .fields({ node: true, size: true })
      .exec(res => {
        const node = res && res[0] && res[0].node
        if (!node) return reject(new Error('canvas 未就绪'))
        // exec 回调里抛异常没人接得住，promise 会永不 settle，必须 try/catch 转 reject
        try {
          draw(node)
          resolve()
        } catch (e) {
          reject(e)
        }
      })
  })

module.exports = { payloadFor, parsePayload, drawTo }
