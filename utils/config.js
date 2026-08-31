// MQTT broker 配置 + 全局时间/重试参数。
// broker 域名必须能进小程序「socket 合法域名」白名单 —— EMQX Serverless 的海外域名
// 通常没有 ICP 备案，提审前必须验证（见 DESIGN.md 后续加固清单 P0）。
const PLACEHOLDER = 'REPLACE_ME'

const config = {
  // 形如 wxs://xxxxxxxx.ala.cn-hangzhou.emqxsl.cn:8084/mqtt
  brokerUrl: 'your host',
  username: 'your username',
  password: 'your password',

  topicPrefix: 'eq',
  // retained 消息存活时间，靠 MQTT 5 Message Expiry Interval 自动过期
  retainTtlSec: 24 * 3600,
  keepDays: 3,              // 本地只保留最近几天的队列
  maxQueuesPerDay: 20,      // 单台设备每天创建上限，防手滑
  qrPrefix: 'EQ1',          // 二维码内容前缀，扫码时快速判定

  // 时间/重试旋钮集中放这里，测试才能覆写（见 test/harness.js）
  settleMs: 5000,           // 回放静默多久算「号码已定」（客人端，见 DESIGN.md）
  syncSettleMs: 1200,       // 老板端回放静默多久算「进度已同步」（见 host.js sync）
  publishTimeoutMs: 10000,  // publish（含建连）超时：连接半开时 mqtt.js 要等 keepalive 才报错
  subRetries: 3,            // 订阅被拒（如 ACL 配错）时的快重试
  subRetryMs: 3000,
  subRetryLongMs: 30000,    // 快重试用完后的慢重试间隔，不能彻底放弃（见 mqttc.js watch）
  closeTimeoutMs: 2000,     // end() 回调不来时最多挡住新连接这么久（见 mqttc.js teardown）
  qrRetries: 5,             // canvas 2d 节点未就绪时重画二维码的次数与间隔
  qrRetryMs: 300,
}

// 哨兵必须永远不是真实值，否则 isConfigured() 会反过来自认未配置
const isConfigured = () =>
  [config.brokerUrl, config.username, config.password].every(v => !!v && v.indexOf(PLACEHOLDER) === -1)

module.exports = { config, isConfigured }
