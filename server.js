// server.js
const express = require('express')
const cors = require('cors')
const app = express()
app.use(cors())
app.use(express.json())

// 约定的返回结构：{ code: 20000, data: {...}, message?: string }
const USER = { username: 'admin', password: '111111' }  // 先硬编码
const TOKEN = 'mock-admin-token'

// 登录（POST）
app.post('/dev-api/user/login', (req, res) => {
  const { username, password } = req.body || {}
  if (username === USER.username && password === USER.password) {
    return res.json({ code: 20000, data: { token: TOKEN } })
  }
  return res.status(401).json({ code: 50008, message: 'Invalid credentials' })
})

// 用户信息（GET）——从请求头 X-Token 里拿 token
app.get('/dev-api/user/info', (req, res) => {
  const token = req.header('X-Token')
  if (token === TOKEN) {
    return res.json({
      code: 20000,
      data: { name: 'Admin', avatar: '', roles: ['admin'] }
    })
  }
  return res.status(401).json({ code: 50008, message: 'Invalid token' })
})

// 退出（POST）
app.post('/dev-api/user/logout', (req, res) => {
  return res.json({ code: 20000, data: null })
})

app.listen(8080, () => console.log('API listening on http://localhost:8080'))
