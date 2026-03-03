import Cookies from 'js-cookie'

// 全局 UI 状态
const state = {
  sidebar: {
    // 侧边栏是否展开
    opened: Cookies.get('sidebarStatus') ? !!+Cookies.get('sidebarStatus') : true,
    // 切换时要不要过渡动画（有些场景需要“瞬间”变化）
    withoutAnimation: false
  },
  // 当前设备类型,默认 'desktop'，移动端布局时会用到
  device: 'desktop'
}

// 修改状态
// mutation 是 Vuex 里修改状态的唯一正规入口（同步修改）
const mutations = {
  TOGGLE_SIDEBAR: state => {
    // 取反：开→关，关→开
    state.sidebar.opened = !state.sidebar.opened
    state.sidebar.withoutAnimation = false
    // 同步写入 Cookie，记住你的选择
    // 目的是“下次刷新页面还能记住你的侧栏开/关”
    if (state.sidebar.opened) {
      Cookies.set('sidebarStatus', 1)
    } else {
      Cookies.set('sidebarStatus', 0)
    }
  },
  CLOSE_SIDEBAR: (state, withoutAnimation) => {
    Cookies.set('sidebarStatus', 0)
    // 强制关闭
    state.sidebar.opened = false
    // 可选：是否无动画
    state.sidebar.withoutAnimation = withoutAnimation
  },
  TOGGLE_DEVICE: (state, device) => {
    // 'desktop' 或 'mobile'
    state.device = device
  }
}

// 组件里怎么用？
// actions 只是把调用 mutation 的流程包一层，方便组件里直接派发
const actions = {
  toggleSideBar({ commit }) {
    commit('TOGGLE_SIDEBAR')
  },
  closeSideBar({ commit }, { withoutAnimation }) {
    commit('CLOSE_SIDEBAR', withoutAnimation)
  },
  toggleDevice({ commit }, device) {
    commit('TOGGLE_DEVICE', device)
  }
}

export default {
  namespaced: true,
  state,
  mutations,
  actions
}
