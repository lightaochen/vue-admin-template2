// 用 js-cookie 这个库把 token 存在浏览器 Cookie 里
import Cookies from 'js-cookie'
// Cookie 的名字（键名)
const TokenKey = 'vue_admin_template_token'
// 登录凭证 token 的读/写/删的工具函数
// 读 Cookie
export function getToken() {
  return Cookies.get(TokenKey)
}
// 写 Cookie
export function setToken(token) {
  return Cookies.set(TokenKey, token)
}
// 删 Cookie
export function removeToken() {
  return Cookies.remove(TokenKey)
}
