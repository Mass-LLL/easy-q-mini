#!/usr/bin/env node
// 把 mqtt.js 从 node_modules 拷进 libs/，并打上「强制走 wx transport」的补丁。
//
// 为什么需要补丁：mqtt.js 4.3.8 的 lib/connect/index.js 是这么挑 transport 的 ——
//
//   if ((typeof process !== 'undefined' && process.title !== 'browser') ||
//       typeof __webpack_require__ !== 'function') { ...tcp/tls... } else { ...wx/ali... }
//
// 也就是说它只在**认为自己是 webpack 产物**时才注册 `wx` / `wxs`。我们 vendor 的是
// browserify 产物：process 垫片确实把 title 设成了 'browser'（前半段过了），但
// `__webpack_require__` 不存在，后半段把整个条件顶成 true，于是注册的是 node 那套，
// `wxs://` 落到 tcp transport 上 —— 第一次 connect() 就抛
// `net.createConnection is not a function`。
//
// 试过在 require 之前挂 `global.__webpack_require__`：node 里有效，小程序里无效。
// 小程序逻辑层的 `global` 并不是真正的全局对象，往它上面挂属性不会影响 bundle 内部
// 那个裸标识符的解析。任何依赖「全局对象语义」的修法在这里都不可靠，所以直接把条件
// 改掉：小程序里永远只可能走 wx 分支，tcp/tls 那套在这个运行时里本来就没有意义。
//
// 第二个补丁：wx transport 把 Buffer 当 ArrayBuffer 发。
//
//   e._write = function (chunk, enc, cb) { socketTask.send({ data: chunk.buffer, ... }) }
//
// `chunk` 是 Node Buffer（Uint8Array 的子类），`chunk.buffer` 是它背后那整块
// ArrayBuffer —— byteOffset / byteLength 全被丢掉。mqtt-packet 写「剩余长度」这类
// 变长整数时会 `Buffer.allocUnsafe(4)` 再 `slice(0, 1)`，于是一个 1 字节的 chunk
// 会被当成 4 字节发出去，报文里凭空多出 `00 00 00`。CONNECT 变成
// `10 24 00 00 00 00 04 4D 51 54 54 …`，broker 解不出协议名，直接关连接不回 CONNACK，
// 客户端只能等到 connectTimeout —— 表现就是「MQTTX 连得上，小程序里一直连接超时」。
// 浏览器版 transport 没这个问题：它把 Buffer 直接交给 WebSocket.send，而 WebSocket
// 认 TypedArray 的 byteOffset/byteLength。
//
// 用法：node scripts/vendor-mqtt.js  （或 npm run vendor:mqtt）
const fs = require('fs')
const path = require('path')

const ROOT = path.join(__dirname, '..')
const SRC = path.join(ROOT, 'node_modules/mqtt/dist/mqtt.min.js')
const DEST = path.join(ROOT, 'libs/mqtt.js')

// 压缩后要替换的两段。每段在整个 bundle 里都只出现一次；对不上就是上游改了写法，
// 这时候必须人去看一眼对应的源文件，而不是让脚本猜。
const PATCHES = [
  {
    what: '协议表分支（lib/connect/index.js）',
    from: 'void 0!==r&&"browser"!==r.title||"function"!=typeof __webpack_require__?',
    to: '!1?',
  },
  {
    what: 'wx transport 的 send（lib/connect/wx.js）',
    from: 's.send({data:e.buffer,',
    to: 's.send({data:e.buffer.slice(e.byteOffset,e.byteOffset+e.byteLength),',
  },
]

let src = fs.readFileSync(SRC, 'utf8')
PATCHES.forEach(p => {
  const hits = src.split(p.from).length - 1
  if (hits !== 1) {
    console.error(`补丁没打上：期望在「${p.what}」命中 1 次，实际 ${hits} 次。`)
    console.error('上游大概改了写法，去看一眼源文件再更新本脚本里的 PATCHES。')
    process.exit(1)
  }
  src = src.replace(p.from, p.to)
})

fs.writeFileSync(DEST, src)
console.log(`已写入 ${path.relative(ROOT, DEST)}（${(src.length / 1024).toFixed(0)}KB），${PATCHES.length} 个 wx transport 补丁已应用`)
console.log('跑一下 npm test —— logic.test.js 会实际调 mqtt.connect("wxs://…") 并逐字节校验 CONNECT 报文')
