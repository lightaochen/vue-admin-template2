// 引入 Vue 核心库，所有 Vue API 的入口
import Vue from 'vue'
// CSS Reset：统一不同浏览器的默认样式，避免表格、按钮等默认差异
import 'normalize.css/normalize.css' // A modern alternative to CSS resets
// 引入 Element-UI 组件库及其默认主题
import ElementUI from 'element-ui'
import 'element-ui/lib/theme-chalk/index.css'
// import locale from 'element-ui/lib/locale/lang/en' // lang i18n

// 全局样式入口（项目配的主题色、全局布局、工具类都在这）
import '@/styles/index.scss' // global css

// 根组件 App.vue，（里面通常只有布局壳和 <router-view/>）
import App from './App'
// Vuex 的全局状态 store（用户信息、token、UI 状态等）
import store from './store'
// vue-router 的 router（路由表、导航控制）
import router from './router'
// 多为 svg-sprite 注册点，把 src/icons/svg 下的图标全量打包为图标字体，供 meta.icon / <svg-icon/> 使用
import '@/icons' // icon
// 全局路由守卫
// router.beforeEach：检查登录态（token）、拉取用户角色、动态注入路由（菜单权限）
// 进度条（如 nprogress）也常在这里控制
import '@/permission' // permission control

/**
 * If you don't want to use mock-server
 * you want to use MockJs for mock api
 * you can execute: mockXHR()
 *
 * Currently MockJs will be used in the production environment,
 * please remove it before going online ! ! !
 */
// if (process.env.NODE_ENV === 'production') {
//   const { mockXHR } = require('../mock')
//   mockXHR()
// }

// set ElementUI lang to EN
// Vue.use(ElementUI, { locale })
// 如果想要中文版 element-ui，按如下方式声明
// 安装 Element-UI 插件
Vue.use(ElementUI)
// 关闭生产提示，控制台里少一句 “You are running Vue in development mode.” 的警告
Vue.config.productionTip = false

// 挂载根实例，让Vue把App.vue 渲染到public/index.html的div里的 #app 节点。
new Vue({
  el: '#app',
  router,
  store,
  render: h => h(App)
})
