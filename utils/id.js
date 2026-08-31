// 随机 id 生成。sid 的不可猜性是 v1 唯一的访问控制（见 DESIGN.md），只接受
// wx.getRandomValues 强随机源：Math.random 内部状态只有 128 位，先后生成的 sid 可互推，
// 所以拿不到强随机源就报错、不静默降级。cid 不是秘密（同队列可见），可退弱随机。
const HEX = '0123456789ABCDEF'

const weakHex = nBytes => {
  let s = ''
  for (let i = 0; i < nBytes * 2; i++) s += HEX[Math.floor(Math.random() * 16)]
  return s
}

const toHex = arrayBuffer => {
  const bytes = new Uint8Array(arrayBuffer)
  let s = ''
  for (let i = 0; i < bytes.length; i++) {
    s += HEX[bytes[i] >> 4] + HEX[bytes[i] & 0x0f]
  }
  return s
}

const strongHex = nBytes =>
  new Promise((resolve, reject) => {
    const fail = () => reject(new Error('当前微信版本过低，无法安全生成队列 id，请更新微信'))
    if (!wx.canIUse('getRandomValues')) return fail()
    wx.getRandomValues({
      length: nBytes,
      success: res => resolve(toHex(res.randomValues)),
      fail,
    })
  })

const randomHex = nBytes => strongHex(nBytes).catch(() => weakHex(nBytes))

// 队列 id：128 位，只通过二维码传播
const newSid = () => strongHex(16)
// 客人设备 id：每个队列一个，64 位
const newCid = () => randomHex(8)

module.exports = { newSid, newCid }
