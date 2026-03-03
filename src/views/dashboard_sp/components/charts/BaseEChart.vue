<template>
  <div ref="el" class="echart-container" />
</template>

<script>
import * as echarts from 'echarts'

export default {
  name: 'BaseEChart',
  props: {
    option: { type: Object, default: () => ({}) },
    autoresize: { type: Boolean, default: true }
  },
  data() {
    return { chart: null, ro: null }
  },
  watch: {
    option: {
      deep: true,
      handler(val) {
        if (!this.chart || !val) return
        this.chart.setOption(val, true) // notMerge 覆盖
      }
    }
  },
  mounted() {
    this.chart = echarts.init(this.$refs.el)
    if (this.option) this.chart.setOption(this.option, true)

    if (this.autoresize) {
      if (window.ResizeObserver) {
        this.ro = new ResizeObserver(() => this.chart && this.chart.resize())
        this.ro.observe(this.$refs.el)
      } else {
        this._onResize = () => this.chart && this.chart.resize()
        window.addEventListener('resize', this._onResize)
      }
      this.$nextTick(() => this.chart && this.chart.resize())
    }
  },
  beforeDestroy() {
    if (this.ro) this.ro.disconnect()
    if (this._onResize) window.removeEventListener('resize', this._onResize)
    if (this.chart) this.chart.dispose()
    this.chart = null
  }
}
</script>

<style scoped>
.echart-container { width: 100%; height: 100%; }
</style>
