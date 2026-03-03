<template>
  <div class="page">
    <h2>结算（PnL）</h2>

    <!-- 管理员视图：总体盈亏 -->
    <div v-if="isAdmin">
      <el-card shadow="hover" style="margin-bottom:12px">总体盈亏：{{ totalPnL }}</el-card>
      <el-table :data="teamPnL" size="small" stripe>
        <el-table-column prop="trader" label="Trader"/>
        <el-table-column prop="pnl" label="PnL"/>
      </el-table>
    </div>

    <!-- Trader 视图：个人或组内 -->
    <div v-else>
      <el-card shadow="hover" style="margin-bottom:12px">我的盈亏：{{ myPnL }}</el-card>
      <el-table :data="groupPnL" size="small" stripe>
        <el-table-column prop="member" label="成员"/>
        <el-table-column prop="pnl" label="PnL"/>
      </el-table>
    </div>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
import request from '@/utils/request'

export default {
  name: 'DashboardPnL',
  data() {
    return {
      totalPnL: 0,
      teamPnL: [],
      myPnL: 0,
      groupPnL: []
    }
  },
  computed: {
    ...mapGetters(['roles', 'name']),
    isAdmin() { return Array.isArray(this.roles) && this.roles.includes('admin') }
  },
  async created() {
    // 一个接口由后端按角色返回不同数据（推荐）
    const res = await request({ url: '/pnl', method: 'get' })
    // 假设后端返回 data 里可能包含这些字段
    this.totalPnL = res.data.totalPnL || 0
    this.teamPnL  = res.data.teamPnL  || []
    this.myPnL    = res.data.myPnL    || 0
    this.groupPnL = res.data.groupPnL || []
  }
}
</script>

<style scoped>.page{margin:24px}</style>