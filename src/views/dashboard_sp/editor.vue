<template>
  <div>
    <h2>编辑者仪表盘</h2>
    <el-card shadow="hover" style="margin-top:10px">
      <div>我的任务</div>
      <el-table :data="rows" size="small" stripe style="margin-top:10px">
        <el-table-column prop="title" label="标题"/>
        <el-table-column prop="status" label="状态" width="120"/>
        <el-table-column prop="updatedAt" label="更新时间" width="180"/>
      </el-table>
    </el-card>
  </div>
</template>
<script>
export default {
  name: 'EditorDashboard',
  data(){ return { rows: [
    { title:'撰写发布说明', status:'进行中', updatedAt:'2025-08-28 10:00' },
    { title:'检查草稿',   status:'待处理', updatedAt:'2025-08-27 16:30' }
  ]}}
}
</script>
