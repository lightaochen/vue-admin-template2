<template>
  <div class="app-container">
    <!-- 过滤条 -->
    <el-form :inline="true" size="small" class="filter-bar">
      <el-form-item label="品种">
        <el-select v-model="product" filterable placeholder="选择品种" style="width: 140px">
          <el-option v-for="p in productOptions" :key="p" :label="p.toUpperCase()" :value="p" />
        </el-select>
      </el-form-item>

      <el-form-item label="交易日">
        <el-date-picker v-model="tradingDay" type="date" value-format="yyyy-MM-dd" style="width: 160px" />
      </el-form-item>

      <el-form-item label="会话">
        <el-select v-model="sessions" style="width: 120px">
          <el-option label="全部" value="all" />
          <el-option label="日盘" value="day" />
          <el-option label="夜盘" value="night" />
        </el-select>
      </el-form-item>

      <el-form-item label="趋势粒度">
        <el-select v-model="groupSize" style="width: 120px">
          <el-option v-for="m in [5, 10, 15, 30]" :key="m" :label="m + ' 分钟'" :value="m" />
        </el-select>
      </el-form-item>

      <el-form-item label="合约模式">
        <el-radio-group v-model="contractMode">
          <el-radio label="auto">自动主次</el-radio>
          <el-radio label="manual">自选固定</el-radio>
        </el-radio-group>
      </el-form-item>

      <el-form-item v-if="contractMode==='manual'" label="近月">
        <el-select
          v-model="nearContract"
          filterable
          clearable
          placeholder="选择近月"
          style="width: 160px"
          @change="onContractChange"
        >
          <el-option
            v-for="opt in contractOptions"
            :key="opt.value"
            :label="opt.label"
            :value="opt.value"
            :disabled="opt.value===farContract"
          />
        </el-select>
      </el-form-item>

      <el-form-item v-if="contractMode==='manual'" label="远月">
        <el-select
          v-model="farContract"
          filterable
          clearable
          placeholder="选择远月"
          style="width: 160px"
          @change="onContractChange"
        >
          <el-option
            v-for="opt in contractOptions"
            :key="opt.value"
            :label="opt.label"
            :value="opt.value"
            :disabled="opt.value===nearContract"
          />
        </el-select>
      </el-form-item>

      <el-button type="primary" @click="loadData">查询</el-button>
    </el-form>

    <!-- 主/次合约指标卡片 -->
    <el-row v-if="summary" :gutter="12">
      <el-col :md="12" :sm="24">
        <el-card shadow="hover">
          <div class="card-title">
            主力合约：<b>{{ summary.main_contract }}</b>
            <el-tag v-if="summary.main_limit_flag" type="success" size="mini">触及涨跌停</el-tag>
          </div>
          <el-descriptions :column="2" size="small" border>
            <el-descriptions-item label="当日成交量">{{ formatInt(summary.vol_main) }}</el-descriptions-item>
            <el-descriptions-item label="月均成交量">{{ formatInt(summary.main_avg_month_volume) }}</el-descriptions-item>
            <el-descriptions-item label="价格变化量">{{ formatNum(summary.leg1_total_change, 2) }}</el-descriptions-item>
            <el-descriptions-item label="价格波动率">{{ formatNum(summary.leg1_volatility, 4) }}</el-descriptions-item>
            <el-descriptions-item label="与价差相关性">{{ formatNum(summary.corr_spread_leg1, 3) }}</el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>

      <el-col :md="12" :sm="24" style="margin-top: 12px">
        <el-card shadow="hover">
          <div class="card-title">
            次主力合约：<b>{{ summary.sub_contract }}</b>
            <el-tag v-if="summary.sub_limit_flag" type="success" size="mini">触及涨跌停</el-tag>
          </div>
          <el-descriptions :column="2" size="small" border>
            <el-descriptions-item label="当日成交量">{{ formatInt(summary.vol_sub) }}</el-descriptions-item>
            <el-descriptions-item label="月均成交量">{{ formatInt(summary.sub_avg_month_volume) }}</el-descriptions-item>
            <el-descriptions-item label="价格变化量">{{ formatNum(summary.leg2_total_change, 2) }}</el-descriptions-item>
            <el-descriptions-item label="价格波动率">{{ formatNum(summary.leg2_volatility, 4) }}</el-descriptions-item>
            <el-descriptions-item label="与价差相关性">{{ formatNum(summary.corr_spread_leg2, 3) }}</el-descriptions-item>
          </el-descriptions>
        </el-card>
      </el-col>
    </el-row>

    <!-- 汇总（价差） -->
    <el-card v-if="summary" style="margin-top: 12px" shadow="never">
      <el-descriptions :column="4" size="small" border>
        <el-descriptions-item label="展示口径">{{ summary.near_contract }} - {{ summary.far_contract }}</el-descriptions-item>
        <el-descriptions-item label="价差总变化">{{ formatNum(summary.spread_total_change_display, 2) }}</el-descriptions-item>
        <el-descriptions-item label="价差波动率">{{ formatNum(summary.spread_volatility, 4) }}</el-descriptions-item>
        <el-descriptions-item label="腿间相关性">{{ formatNum(summary.corr_leg1_leg2, 3) }}</el-descriptions-item>
      </el-descriptions>
    </el-card>

    <!-- 图表：分布 & 趋势 -->
    <el-row v-show="summary" v-loading="loading" :gutter="12" style="margin-top: 12px">
      <el-col :md="12" :sm="24">
        <el-card shadow="never" :body-style="{ padding: '8px' }">
          <div class="chart-title">价差分布直方图（近月 - 远月）</div>
          <div ref="histChart" class="chart"></div>
        </el-card>
      </el-col>
      <el-col :md="12" :sm="24" style="margin-top: 12px">
        <el-card shadow="never" :body-style="{ padding: '8px' }">
          <div class="chart-title">价差变化趋势（每 {{ groupSize }} 分钟聚合）</div>
          <div ref="trendChart" class="chart"></div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 开盘窗口（仅当 openTrendData 存在） -->
    <el-row v-show="summary && openTrendData" :gutter="12" style="margin-top: 12px">
      <el-col :md="24" :sm="24">
        <el-card shadow="never" :body-style="{ padding: '8px' }">
          <div class="chart-title">
            开盘窗口价差变化（每分钟，
            {{ (openTrendData && openTrendData.window && openTrendData.window.start) || '' }}
            –
            {{ (openTrendData && openTrendData.window && openTrendData.window.end) || '' }}
            ，近月-远月）
          </div>
          <div ref="openChart" class="chart"></div>
        </el-card>
      </el-col>
    </el-row>

    <el-empty v-if="!loading && !summary" description="请选择条件并查询" />
  </div>
</template>

<script>
import * as echarts from 'echarts'
import { getTodayDashboard, getContracts } from '@/api/spread'

export default {
  name: 'SpreadDashboard',
  props: {
    mode: { type: String, default: 'today' }
  },
  data() {
    const today = new Date()
    const fmt = d => [d.getFullYear(), String(d.getMonth() + 1).padStart(2, '0'), String(d.getDate()).padStart(2, '0')].join('-')
    return {
      productOptions: ['cu', 'zn', 'al', 'ni', 'i', 'au', 'ih', 'if', 'ic', 'im'],
      product: 'cu',
      tradingDay: fmt(today),
      sessions: 'all',
      groupSize: 5,

      contractMode: 'auto',
      contractOptions: [],
      nearContract: '',
      farContract: '',

      loading: false,
      summary: null,
      histData: null,
      histWeeklyData: null, // ← 新增：近 5 日
      trendData: null,
      histChart: null,
      trendChart: null,
      openTrendData: null, // { x:[], y:[], limit_ts:[], window:{start,end}, is_index:bool } 或 null
      openChart: null, // echarts 实例
      yRangeTrendOpen: null, // {min, max} 或 null
      // 并发 & 定时器控制（不以 _ 开头，避免保留名）
      reqSeq: 0,
      isPending: false,
      chartRetryTimer: null
    }
  },
  watch: {
    contractMode(val) {
      if (val === 'manual') {
        this.fetchContractOptions().then(() => {
          // 只清数据，别在隐藏态清/画图
          this.nearContract = ''
          this.farContract = ''
          this.summary = this.histData = this.trendData = null
          if (this.chartRetryTimer) {
            clearTimeout(this.chartRetryTimer)
            this.chartRetryTimer = null
          }
        })
      } else {
        this.loadData()
      }
    },
    product() { this.loadData() },
    tradingDay() { this.loadData() },
    sessions() { this.loadData() },
    groupSize() { this.loadData() }
  },
  mounted() {
    if (this.mode === 'today') {
      this.$nextTick(() => {
        this.initCharts()
        this.loadData()
        window.addEventListener('resize', this.handleResize)
        setTimeout(this.handleResize, 0)
      })
    }
  },
  beforeDestroy() { // 如果是 Vue3 改为 beforeUnmount
    window.removeEventListener('resize', this.handleResize)
    if (this.chartRetryTimer) {
      clearTimeout(this.chartRetryTimer)
      this.chartRetryTimer = null
    }
    if (this.histChart) this.histChart.dispose()
    if (this.trendChart) this.trendChart.dispose()
    if (this.openChart) this.openChart.dispose() // NEW
  },
  methods: {
    async fetchContractOptions() {
      try {
        const resp = await getContracts({ product: this.product, on: this.tradingDay })
        if (resp.code !== 20000) {
          this.$message.error(resp.message || '加载合约失败')
          this.contractOptions = []
          this.nearContract = ''
          this.farContract = ''
          return
        }
        this.contractOptions = (resp.data?.options || []).map(code => ({
          label: String(code).toUpperCase(),
          value: code
        }))
      } catch (e) {
        console.error(e)
        this.$message.error('加载合约失败')
        this.contractOptions = []
        this.nearContract = ''
        this.farContract = ''
      }
    },
    initCharts() {
      // 只在实例未创建时 init；并保证拿到最新 DOM
      if (this.$refs.histChart && (!this.histChart || this.histChart.getDom() !== this.$refs.histChart)) {
        this.histChart = echarts.init(this.$refs.histChart)
      }
      if (this.$refs.trendChart && (!this.trendChart || this.trendChart.getDom() !== this.$refs.trendChart)) {
        this.trendChart = echarts.init(this.$refs.trendChart)
      }
      // NEW
      if (!this.openChart && this.$refs.openChart) {
        this.openChart = echarts.init(this.$refs.openChart)
      }
    },
    onContractChange() {
      // 手动模式：两个合约都选齐再查，避免两次 @change 竞态
      if (this.contractMode === 'manual' && this.nearContract && this.farContract) {
        this.loadData()
      }
    },
    async loadData() {
      if (this.mode !== 'today') return

      // 手动模式：未选齐就不请求，也不动图
      if (this.contractMode === 'manual' && (!this.nearContract || !this.farContract)) {
        this.summary = this.histData = this.trendData = null
        return
      }

      if (this.isPending) return

      try {
        this.loading = true
        this.isPending = true
        const reqId = ++this.reqSeq

        const params = {
          product: this.product,
          trading_day: this.tradingDay,
          sessions: this.sessions,
          group_size: this.groupSize,
          contract_mode: this.contractMode
        }
        if (this.contractMode === 'manual') {
          params.near_contract = this.nearContract
          params.far_contract = this.farContract
        }

        const resp = await getTodayDashboard(params)
        if (reqId !== this.reqSeq) return // 丢弃过期响应

        if (resp.code !== 20000 || !resp.data || !resp.data.summary) {
          this.$message[resp.code === 20000 ? 'info' : 'error'](resp.message || (resp.code === 20000 ? '暂无数据' : '获取数据失败'))
          this.summary = this.histData = this.trendData = null
          // 不在隐藏态清图，直接返回
          return
        }

        const payload = resp.data || {}
        this.summary = payload.summary || null
        this.histData = payload.histogram || null
        this.histWeeklyData = payload.histogram_weekly || null // 近 5 日
        this.trendData = payload.trend || null
        // NEW
        this.openTrendData = payload.open_trend || null
        this.yRangeTrendOpen = payload.y_range_trend_open || null

        // 等 v-show 生效、容器有宽度后再画；并清理任何残留重试
        if (this.chartRetryTimer) {
          clearTimeout(this.chartRetryTimer)
          this.chartRetryTimer = null
        }
        this.$nextTick(() => {
          this.renderCharts()
          this.handleResize()
        })
      } catch (e) {
        this.$message.error('请求失败')
        console.error(e)
      } finally {
        this.loading = false
        this.isPending = false
      }
    },
    renderCharts(clearOnly = false) {
      this.initCharts()

      // 只保留一次重试：进入就清掉旧的
      if (this.chartRetryTimer) {
        clearTimeout(this.chartRetryTimer)
        this.chartRetryTimer = null
      }

      // 容器若未完成布局（宽度为 0）→ 延迟重试（仅挂一个）
      const hw = this.$refs.histChart ? this.$refs.histChart.getBoundingClientRect().width : 0
      const tw = this.$refs.trendChart ? this.$refs.trendChart.getBoundingClientRect().width : 0
      if (!hw || !tw) {
        this.chartRetryTimer = setTimeout(() => {
          this.chartRetryTimer = null
          this.renderCharts(clearOnly)
        }, 80)
        return
      }

      // === 直方图（当天 + 近5日背景）===
      if (clearOnly || !this.histData) {
        this.histChart && this.histChart.clear()
      } else if (this.histChart) {
        // x 轴类别取当天 bins；若当天无而 weekly 有，则取 weekly
        const bins = (this.histData.bins && this.histData.bins.length)
          ? this.histData.bins
          : (this.histWeeklyData && this.histWeeklyData.bins) || []

        const series = []

        // 背景：近 5 日（在后面、淡色、与当天共边界）
        if (this.histWeeklyData && this.histWeeklyData.counts && this.histWeeklyData.counts.length) {
          series.push({
            name: `近${this.histWeeklyData.window_trading_days || 5}个交易日`,
            type: 'bar',
            data: this.histWeeklyData.counts,
            // 让两条柱子“同一类目重叠”，形成背景效果
            barGap: '-100%',
            itemStyle: { opacity: 0.35 },
            emphasis: { focus: 'series' },
            z: 1
          })
        }

        // 前景：当天（不透明，描边）
        series.push({
          name: this.summary ? this.summary.trading_day : '当天',
          type: 'bar',
          data: this.histData.counts || [],
          itemStyle: { borderColor: '#333', borderWidth: 0.7 },
          z: 10
        })

        this.histChart.setOption({
          tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }},
          legend: { top: 2 }, // 显示图例，便于区分
          grid: { left: 40, right: 20, top: 28, bottom: 40 },
          xAxis: {
            type: 'category',
            data: (bins || []).map(v => Number(v).toFixed(2)),
            axisLabel: { rotate: 45 }
          },
          yAxis: { type: 'value' },
          series
        }, true)
        this.histChart.resize()
      }

      // 趋势图
      if (clearOnly || !this.trendData) {
        this.trendChart && this.trendChart.clear()
      } else if (this.trendChart) {
        this.trendChart.setOption({
          tooltip: { trigger: 'axis' },
          grid: { left: 40, right: 20, top: 20, bottom: 40 },
          xAxis: { type: 'category', data: this.trendData.x || [], axisLabel: { rotate: 45 }},
          yAxis: { type: 'value' },
          series: [{ type: 'bar', data: this.trendData.y || [] }]
        }, true)
        this.trendChart.resize()
      }

      // NEW: 开盘窗口（每分钟）
      if (clearOnly || !this.openTrendData) {
        this.openChart && this.openChart.clear()
      } else if (this.openChart) {
        // 若后端给了统一范围，两张图共用
        const yRange = this.yRangeTrendOpen || null
        this.openChart.setOption({
          tooltip: { trigger: 'axis' },
          grid: { left: 40, right: 20, top: 20, bottom: 40 },
          xAxis: { type: 'category', data: this.openTrendData.x || [], axisLabel: { rotate: 45 }},
          yAxis: Object.assign({ type: 'value' }, yRange ? { min: yRange.min, max: yRange.max } : {}),
          series: [
            { type: 'bar', data: this.openTrendData.y || [] },
            // 若想标注开盘窗口里的涨跌停点：用散点覆盖（可选）
            ...(Array.isArray(this.openTrendData.limit_ts) && this.openTrendData.limit_ts.length
              ? [{
                type: 'scatter',
                data: (this.openTrendData.limit_ts || []).map(ts => {
                  const idx = (this.openTrendData.x || []).findIndex(x => x === ts.slice(11, 16))
                  if (idx >= 0) return [this.openTrendData.x[idx], this.openTrendData.y[idx]]
                  return null
                }).filter(Boolean),
                symbolSize: 8
              }]
              : [])
          ]
        }, true)
        this.openChart.resize()
      }

      // 若需要让“主趋势”和“开盘窗口”共用同一 y 轴范围，也把趋势图 y 轴按 yRangeTrendOpen 覆盖掉
      if (this.trendChart && this.yRangeTrendOpen) {
        this.trendChart.setOption({
          yAxis: { type: 'value', min: this.yRangeTrendOpen.min, max: this.yRangeTrendOpen.max }
        })
      }
    },
    handleResize() {
      if (this.histChart) this.histChart.resize()
      if (this.trendChart) this.trendChart.resize()
      if (this.openChart) this.openChart.resize() // NEW
    },
    formatInt(v) {
      if (v === null || v === undefined) return '-'
      const n = Number(v)
      if (Number.isNaN(n)) return String(v)
      return n.toLocaleString()
    },
    formatNum(v, d = 2) {
      if (v === null || v === undefined) return '-'
      const n = Number(v)
      if (Number.isNaN(n)) return String(v)
      return n.toFixed(d)
    }
  }
}
</script>

<style scoped>
.filter-bar { margin-bottom: 10px; }
.card-title { margin-bottom: 6px; font-size: 14px; }
.chart-title { font-size: 13px; margin: 4px 0 8px; }
.chart { width: 100%; height: 340px; }
</style>
