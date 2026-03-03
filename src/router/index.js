// Vue.use(Router)：给 Vue 安装“路由功能”
import Vue from 'vue'

import Router from 'vue-router'
import spreadRouter from './modules/spread'

Vue.use(Router)

/* Layout */
// Layout：项目的“壳”（顶栏+侧边栏+内容区）。很多页面都会作为它的 children 出现，从而继承统一布局。
import Layout from '@/layout'

/**
 * Note: sub-menu only appear when route children.length >= 1
 * Detail see: https://panjiachen.github.io/vue-element-admin-site/guide/essentials/router-and-nav.html
 *
 * hidden: true                   if set true, item will not show in the sidebar(default is false)
 * alwaysShow: true               if set true, will always show the root menu
 *                                if not set alwaysShow, when item has more than one children route,
 *                                it will becomes nested mode, otherwise not show the root menu
 * redirect: noRedirect           if set noRedirect will no redirect in the breadcrumb
 * name:'router-name'             the name is used by <keep-alive> (must set!!!)
 * meta : {
    roles: ['admin','editor']    control the page roles (you can set multiple roles)
    title: 'title'               the name show in sidebar and breadcrumb (recommend set)
    icon: 'svg-name'/'el-icon-x' the icon show in the sidebar
    breadcrumb: false            if set false, the item will hidden in breadcrumb(default is true)
    activeMenu: '/example/list'  if set path, the sidebar will highlight the path you set
  }
 */
// hidden: true：不在侧边栏显示（但路由能访问，比如编辑详情页）。
// alwaysShow: true：即使只有一个子路由，也强制显示父菜单（默认一个子路由会把父级合并隐藏）。
// redirect：点父级菜单跳到哪个子页面。设为 noRedirect 可让面包屑里这个层级不可点。
// name：配合 <keep-alive> 缓存页面时必须有，且全局唯一。
// meta 里最常用：
// roles: ['admin','editor']：控制哪些角色可见/可访问（通常配合 permission.js 动态过滤）。
// title：侧边栏/面包屑显示的文案。
// icon：侧边栏小图标（svg-name 或 el-icon-*）。
// breadcrumb: false：不出现在面包屑。
// activeMenu: '/example/list'：高亮别的菜单（常用于详情页高亮列表页）。

/**
 * constantRoutes
 * a base page that does not have permission requirements
 * all roles can be accessed
 * 所有人都能访问的“基础路由”,这些路由立即就生效，创建 Router 时就挂上了。
 * 通常只放：登录、注册、404、首页重定向、完全公开的页面等。
 * 项目里有 带 roles 的“结算 PnL” 放在 constantRoutes 中，这代表它不经过权限过滤就已存在；
 * 如果你想登录前看不到菜单或无权限直接 404/跳转，最好把它移动到“动态路由”，见“最佳实践”。
 */
export const constantRoutes = [
  {
    path: '/login',
    component: () => import('@/views/login/index'),
    hidden: true
  },

  { path: '/', redirect: '/spread/today' },

  // 价差
  spreadRouter,

  // // 价差分析（所有用户一样）
  // {
  //   path: '/dashboard_sp',
  //   component: Layout,
  //   children: [{
  //     path: 'index',
  //     name: 'DashboardSP',
  //     component: () => import('@/views/dashboard_sp/index'),
  //     meta: { title: '价差分析', icon: 'dashboard' }   // 不限角色
  //   }]
  // },
  {
    path: '/example',
    component: Layout,
    redirect: '/example/table',
    name: 'Example',
    meta: { title: 'Example', icon: 'el-icon-s-help' },
    children: [
      {
        path: 'table',
        name: 'Table',
        component: () => import('@/views/table/index'),
        meta: { title: 'Table', icon: 'table' }
      },
      {
        path: 'tree',
        name: 'Tree',
        component: () => import('@/views/tree/index'),
        meta: { title: 'Tree', icon: 'tree' }
      }
    ]
  },

  {
    path: '/form',
    component: Layout,
    children: [
      {
        path: 'index',
        name: 'Form',
        component: () => import('@/views/form/index'),
        meta: { title: 'Form', icon: 'form' }
      }
    ]
  },

  {
    path: '/nested',
    component: Layout,
    redirect: '/nested/menu1',
    name: 'Nested',
    meta: {
      title: 'Nested',
      icon: 'nested'
    },
    children: [
      {
        path: 'menu1',
        component: () => import('@/views/nested/menu1/index'), // Parent router-view
        name: 'Menu1',
        meta: { title: 'Menu1' },
        children: [
          {
            path: 'menu1-1',
            component: () => import('@/views/nested/menu1/menu1-1'),
            name: 'Menu1-1',
            meta: { title: 'Menu1-1' }
          },
          {
            path: 'menu1-2',
            component: () => import('@/views/nested/menu1/menu1-2'),
            name: 'Menu1-2',
            meta: { title: 'Menu1-2' },
            children: [
              {
                path: 'menu1-2-1',
                component: () => import('@/views/nested/menu1/menu1-2/menu1-2-1'),
                name: 'Menu1-2-1',
                meta: { title: 'Menu1-2-1' }
              },
              {
                path: 'menu1-2-2',
                component: () => import('@/views/nested/menu1/menu1-2/menu1-2-2'),
                name: 'Menu1-2-2',
                meta: { title: 'Menu1-2-2' }
              }
            ]
          },
          {
            path: 'menu1-3',
            component: () => import('@/views/nested/menu1/menu1-3'),
            name: 'Menu1-3',
            meta: { title: 'Menu1-3' }
          }
        ]
      },
      {
        path: 'menu2',
        component: () => import('@/views/nested/menu2/index'),
        name: 'Menu2',
        meta: { title: 'menu2' }
      }
    ]
  },

  {
    path: 'external-link',
    component: Layout,
    children: [
      {
        path: 'https://panjiachen.github.io/vue-element-admin-site/#/',
        meta: { title: 'External Link', icon: 'link' }
      }
    ]
  },

  // 404 page must be placed at the end !!!
  { path: '*', redirect: '/404', hidden: true }
]

// 需要鉴权的路由
export const asyncRoutes = [
  {
    path: '/dashboard_pnl',
    component: Layout,
    children: [{
      path: 'index',
      name: 'DashboardPnL',
      component: () => import('@/views/dashboard_pnl/index'),
      meta: { title: '结算', icon: 'pnl', roles: ['admin', 'trader'] }
    }]
  }
]



const createRouter = () => new Router({
  // mode: 'history', // require service support
  scrollBehavior: () => ({ y: 0 }),
  routes: constantRoutes
})


// 创建一个新的 Router 实例，初始路由是 constantRoutes
const router = createRouter()

// 重置路由，常用于 退出登录 时清空权限路由
// Detail see: https://github.com/vuejs/vue-router/issues/1234#issuecomment-357941465
export function resetRouter() {
  const newRouter = createRouter()
  router.matcher = newRouter.matcher // reset router
}

export default router
