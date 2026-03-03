// setting.hs 是一个Vuex 模块，专门用来管理“全局外观配置项”
// 比如“是否固定头部”“侧边栏是否显示 Logo”“界面里要不要显示设置面板入口”。

// 1. 管理了哪些开关？
import defaultSettings from '@/settings'

const { showSettings, fixedHeader, sidebarLogo } = defaultSettings

const state = {
  showSettings: showSettings,   // 是否显示‘设置’入口按钮/面板
  fixedHeader: fixedHeader,     // 头部是否固定在页面顶端（滚动时不跟着走）
  sidebarLogo: sidebarLogo      // 侧边栏顶部是否显示 Logo
}

const mutations = {
  CHANGE_SETTING: (state, { key, value }) => {
    // eslint-disable-next-line no-prototype-builtins
    if (state.hasOwnProperty(key)) {
      state[key] = value
    }
  }
}

const actions = {
  changeSetting({ commit }, data) {
    commit('CHANGE_SETTING', data)
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions
}
