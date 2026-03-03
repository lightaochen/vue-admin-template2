<template>
  <BaseEChart class="w-full h-full" :option="option" />
</template>

<script>
import BaseEChart from './BaseEChart.vue'

export default {
  name: 'HistogramChart',
  components: { BaseEChart },
  props: {
    bins: { type: Array, default: () => [] },           // x 类目（中心点）
    countsToday: { type: Array, default: () => [] },    // 当天频数
    countsWeekly: { type: Array, default: null },       // 近5日频数（可空）
    tradingDayLabel: { type: String, default: '当天' }  // 图例名
  },
  computed: {
    option() {
      const xData = (this.bins || []).map(v => Number(v).toFixed(2))
      const series = []

      if (Array.isArray(this.countsWeekly) && this.countsWeekly.length) {
        series.push({
          name: '近5个交易日',
          type: 'bar',
          data: this.countsWeekly,
          barGap: '-100%',              // 与当天柱同类目重叠，做背景
          itemStyle: { opacity: 0.35 },
          emphasis: { focus: 'series' },
          z: 1
        })
      }

      series.push({
        name: this.tradingDayLabel,
        type: 'bar',
        data: this.countsToday || [],
        itemStyle: { borderColor: '#333', borderWidth: 0.7 },
        z: 10
      })

      return {
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        legend: { top: 2 },
        grid: { left: 40, right: 20, top: 28, bottom: 40 },
        xAxis: { type: 'category', data: xData, axisLabel: { rotate: 45 } },
        yAxis: { type: 'value' },
        series
      }
    }
  }
}
</script>
