// src/api/spread.js
import request from '@/utils/request'

// 当天面板
export function getTodayDashboard(params) {
  return request({
    url: '/api/spread/today/dashboard', // dev 环境会被 /dev-api 代理
    method: 'get',
    params
  })
}

// 当天面板：新拆的小接口（并发请求，分别回）
export function getSummary(params, opts = {}) {
  return request({ url: '/spread/today/summary', method: 'get', params, ...opts })
}
export function getHistogram(params, opts = {}) {
  return request({ url: '/spread/today/histogram', method: 'get', params, ...opts })
}
export function getHistogramWeekly(params, opts = {}) {
  return request({ url: '/spread/today/histogram_weekly', method: 'get', params, ...opts })
}
export function getTrend(params, opts = {}) {
  return request({ url: '/spread/today/trend', method: 'get', params, ...opts })
}
export function getOpenTrend(params, opts = {}) {
  return request({ url: '/spread/today/open_trend', method: 'get', params, ...opts })
}


export function getContracts(params) {
  return request({
    url: '/spread/contracts',
    method: 'get',
    params
  })
}

// 历史面板
export function getHistoryDashboard(params) {
  return request({
    url: '/api/spread/history/dashboard', // 后端对应 /spread/history/dashboard
    method: 'get',
    params
  })
}
