// 队列顺序的本地计算：全序确定的纯函数，给定同一组入队消息，任何设备都算出同一结果，
// 因此不需要发号权威（见 DESIGN.md）。时间戳相同时按 cid 字典序兜底。

const sortJoins = joins =>
  joins.slice().sort((a, b) => (a.t !== b.t ? a.t - b.t : a.cid < b.cid ? -1 : a.cid > b.cid ? 1 : 0))

// cid 对应的号码（从 1 开始），不在队列里返回 0
const rankOf = (joins, cid) => {
  const sorted = sortJoins(joins)
  for (let i = 0; i < sorted.length; i++) {
    if (sorted[i].cid === cid) return i + 1
  }
  return 0
}

// called 表示 1..called 号已被叫过，返回自己前面还剩几位
const aheadOf = (rank, called) => Math.max(0, rank - Math.max(0, called) - 1)

module.exports = { sortJoins, rankOf, aheadOf }
