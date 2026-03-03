<template><div class="tab">价差相关性（占位）</div></template>
<script>
export default {
  name: 'CorrelationTab',
  props: { params: { type: Object, default: () => ({}) }},
  watch: { params: { deep: true, handler() { /* TODO: 拉取 /api/spread/correlation */ } }}
}
</script>
