<template><div class="tab">波动率（占位）</div></template>
<script>
export default {
  name: 'VolatilityTab',
  props: { params: { type: Object, default: () => ({}) }},
  watch: { params: { deep: true, handler() { /* TODO: 拉取 /api/spread/volatility */ } }}
}
</script>
