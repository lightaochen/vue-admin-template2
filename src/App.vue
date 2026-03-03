<template>
  <div id="app">
    <router-view />
    <!-- 这是一条用于验证的提示条，看到它就说明 App.vue 已经被正确渲染 -->
    <div style="margin:12px; padding:10px; border:1px dashed #f0c36d; background:#fffbea;">
      ✅ App 根组件已渲染（来自 App.vue）
    </div>
  </div>
</template>

<script>
export default {
  name: 'App'
}
</script>
