<template>
  <div>
    <h2>管理员仪表盘</h2>
    <el-row :gutter="20">
      <el-col :span="6"><el-card shadow="hover">用户总数：1024</el-card></el-col>
      <el-col :span="6"><el-card shadow="hover">今日访问：3,560</el-card></el-col>
      <el-col :span="6"><el-card shadow="hover">错误告警：2</el-card></el-col>
      <el-col :span="6"><el-card shadow="hover">系统健康：<el-progress :percentage="90"/></el-card></el-col>
    </el-row>
  </div>
</template>
<script>
export default { name: 'AdminDashboard' }
</script>