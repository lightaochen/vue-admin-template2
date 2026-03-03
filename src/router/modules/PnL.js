// src/router/modules/spread.js
import Layout from '@/layout'

const PnLRouter = {
  // 父级路由
  path: '/spread',
  component: Layout,
  // 默认子路由
  redirect: '/spread/today', // 进入价差分析默认到“当天”
  name: 'Spread',
  alwaysShow: true,
  meta: { title: '价差分析', icon: 'dashboard' }, // 这个 'dashboard' 是 svg 图标名
  // 子页面
  children: [
    {
      path: 'today',
      name: 'SpreadToday',
      component: () =>
        import(/* webpackChunkName: "spread-today" */ '@/views/dashboard_sp/Dashboard_today.vue'),
      meta: { title: '当天统计面板' }, // 子菜单一般不要放 el-icon-xxx
      // 通过路由 props 把模式传给组件
      props: route => ({ mode: 'today' })
    },
    {
      path: 'history',
      name: 'SpreadHistory',
      component: () =>
        import(/* webpackChunkName: "spread-history" */ '@/views/dashboard_sp/Dashboard_history.vue'),
      meta: { title: '历史统计面板' },
      props: route => ({ mode: 'history' })
    }
  ]
}

export default spreadRouter
