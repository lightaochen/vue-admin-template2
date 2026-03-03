// src/router/modules/volume.js
import Layout from '@/layout'

const volumeRouter = {
  path: '/volume',
  component: Layout,
  redirect: '/volume/overview',
  name: 'Volume',
  meta: { title: '成交量面板', icon: 'el-icon-data-analysis' },
  children: [
    {
      path: 'overview',
      name: 'VolumeOverview',
      component: () => import('@/views/dashboard_sp/volume/Overview'),
      meta: { title: '成交量统计' }
    }
  ]
}
export default volumeRouter
