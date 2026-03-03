<template><div class="tab">价格变化量 ΔP（占位）</div></template>
<script>
export default {
  name: 'DeltaPTab',
  props: { params: { type: Object, default: () => ({}) }},
  watch: { params: { deep: true, handler() { /* TODO: 拉取 /api/spread/deltap */ } }}
}
</script>
