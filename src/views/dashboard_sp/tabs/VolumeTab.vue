<script>
export default {
  name: 'VolumeTab',
  props: { params: { type: Object, default: () => ({}) }},
  watch: { params: { deep: true, handler() { /* TODO: 拉取 /api/spread/volume */ } }}
}
</script>
