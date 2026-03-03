// src/utils/request.js
import axios from 'axios'
import { MessageBox } from 'element-ui'
import store from '@/store'
import { getToken } from '@/utils/auth'

const service = axios.create({
  baseURL: process.env.VUE_APP_BASE_API, // .env.development => /dev-api
  timeout: 120000, // ← 30s，避免重统计接口超时
  headers: { Accept: 'application/json' }
})

// ===== 请求拦截 =====
service.interceptors.request.use(
  config => {
    if (process.env.NODE_ENV !== 'production') {
      // 打印下最终会发出的完整路径（便于排查代理问题）
      // 注意：此时还未拼接 baseURL，仅打印相对路径
      console.log('[REQ]', service.defaults.baseURL, config.method?.toUpperCase(), config.url, config.params || config.data || '')
    }

    // 代理到后端时，如果 baseURL 是 /dev-api，而 url 又以 /api/ 开头，去掉 /api 以兼容你后端两种路由写法
    if (service.defaults.baseURL === '/dev-api' && /^\/api\//.test(config.url)) {
      config.url = config.url.replace(/^\/api/, '')
    }
    // 避免产生 // 双斜杠
    config.url = config.url.replace(/\/{2,}/g, '/')

    // 携带 token
    const token = getToken()
    if (token) {
      config.headers['X-Token'] = token
    }

    return config
  },
  error => Promise.reject(error)
)

// ===== 响应拦截 =====
service.interceptors.response.use(
  response => {
    const res = response.data // 形如 { code, data, message }

    if (process.env.NODE_ENV !== 'production') {
      console.log('[RES]', response.config.url, res)
    }

    // 你后端的成功码是 20000
    if (res.code !== 20000) {
      // 只对 token 相关错误弹框，其它错误交给页面 catch 显示
      if (res.code === 50008 || res.code === 50012 || res.code === 50014) {
        MessageBox.confirm('登录状态已失效，请重新登录', '提示', {
          confirmButtonText: '重新登录',
          cancelButtonText: '取消',
          type: 'warning'
        }).then(() => {
          store.dispatch('user/resetToken').then(() => location.reload())
        })
      }
      return Promise.reject(new Error(res.message || '请求失败'))
    }

    // ✅ 保持返回 { code, data, message }，页面用 res.data 解包
    // 如果你更喜欢直接返回 data，也可以改成：return res.data
    return res
  },
  error => {
    // 尽量把后端 message 透传出来
    const backend = error?.response?.data
    const msg = backend?.message || error.message || '网络错误'
    if (process.env.NODE_ENV !== 'production') {
      console.error('[ERR]', error?.config?.url, msg, backend || '')
    }
    // 透传后端结构，方便页面按需处理
    return Promise.reject(backend || { message: msg })
  }
)

export default service
