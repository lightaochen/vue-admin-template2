import Vue from 'vue'
import Vuex from 'vuex'
import getters from './getters'
import app from './modules/app'
import settings from './modules/settings'
import user from './modules/user'

Vue.use(Vuex)
// 注册三个模块：app / settings / user
// 合并全局 getters
// 导出 store 供 main.js 挂载
const store = new Vuex.Store({
  modules: {
    app,
    settings,
    user
  },
  getters
})

export default store
