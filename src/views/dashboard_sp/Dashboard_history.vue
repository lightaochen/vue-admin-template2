<!-- template 页面结构（表单 + 卡片 + 图表容器）。 -->
<template>
  <!-- 给整页加 loading 蒙层 -->
  <div
    class="app-container"
  >
    <!-- 条件栏 -->
    <el-form :inline="true" size="small" class="filter-bar">
      <el-form-item label="品种">
        <el-select v-model="product" filterable style="width: 140px">
          <el-option v-for="p in productOptions" :key="p" :label="p.toUpperCase()" :value="p" />
        </el-select>
      </el-form-item>

      <el-form-item label="日期范围">
        <el-date-picker
          v-model="range"
          type="daterange"
          value-format="yyyy-MM-dd"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          :picker-options="pickerOptions"
          style="width: 280px;"
        />
      </el-form-item>

      <el-form-item label="会话">
        <el-select v-model="sessions" style="width: 120px">
          <el-option label="全部" value="all" />
          <el-option label="日盘" value="day" />
          <el-option label="夜盘" value="night" />
        </el-select>
      </el-form-item>

      <!-- <el-form-item label="合约模式">
        <el-radio-group v-model="contractMode">
          <el-radio label="auto">自动主/次</el-radio>
          <el-radio label="manual">自选固定</el-radio>
        </el-radio-group>
      </el-form-item>

      <el-form-item v-if="contractMode==='manual'" label="近月">
        <el-input v-model="nearContract" placeholder="例如 cu2509" style="width: 130px" />
      </el-form-item>
      <el-form-item v-if="contractMode==='manual'" label="远月">
        <el-input v-model="farContract" placeholder="例如 cu2510" style="width: 130px" />
      </el-form-item> -->

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
          @change="loadData"
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
          @change="loadData"
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

    <!-- 顶部汇总 -->
    <el-card v-if="summary" v-loading="loading" shadow="never">
      <el-descriptions :column="5" size="small" border>
        <el-descriptions-item label="交易日数">{{ summary.days }}</el-descriptions-item>
        <el-descriptions-item label="跳过天数">{{ summary.skipped }}</el-descriptions-item>
        <el-descriptions-item label="近月均成交量">{{ fmtInt(summary.avg_vol_near) }}</el-descriptions-item>
        <el-descriptions-item label="远月均成交量">{{ fmtInt(summary.avg_vol_far) }}</el-descriptions-item>
        <el-descriptions-item label="价差均波动率">{{ fmtNum(summary.avg_volat_spread, 4) }}</el-descriptions-item>
      </el-descriptions>
    </el-card>

    <!-- 指标图 -->
    <el-row :gutter="12"  style="margin-top: 12px">
      <el-col v-loading="loading" :md="12" :sm="24">
        <el-card shadow="never" :body-style="{padding:'8px'}">
          <div class="chart-title">成交量（近月 / 远月）</div>
          <div ref="volChart" class="chart" />
        </el-card>
      </el-col>
      <el-col v-loading="loading" :md="12" :sm="24" style="margin-top: 12px">
        <el-card shadow="never" :body-style="{padding:'8px'}">
          <div class="chart-title">价格变化量（近月 / 远月）</div>
          <div ref="deltaChart" class="chart" />
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="12" style="margin-top: 12px">
      <el-col v-loading="loading" :md="12" :sm="24">
        <el-card shadow="never" :body-style="{padding:'8px'}">
          <div class="chart-title">波动率（近月 / 远月）</div>
          <div ref="volatChart" class="chart" />
        </el-card>
      </el-col>
      <el-col  :md="12" :sm="24" style="margin-top: 12px">
        <el-card v-loading="loading" shadow="never" :body-style="{padding:'8px'}">
          <div class="chart-title">与价差相关性（近月 / 远月）</div>
          <div ref="corrChart" class="chart" />
        </el-card>
      </el-col>
    </el-row>

    <!-- 合约轨迹 -->
    <!-- <el-card v-if="contracts && contracts.length" style="margin-top: 12px" shadow="never">
      <div class="chart-title">每日使用的合约对（用于验证 “自动主/次” 选择）</div>
      <el-table :data="contracts" size="small" border>
        <el-table-column prop="trading_day" label="交易日" width="120" />
        <el-table-column prop="near" label="近月" />
        <el-table-column prop="far" label="远月" />
      </el-table>
    </el-card> -->

    <div v-if="!loading && (!x.length)" class="empty-hint">暂无数据，请调整筛选条件</div>
  </div>
</template>



<!-- script 数据、生命周期、请求后端、把数据塞进图表。 -->
<script>
import * as echarts from 'echarts'
import { getHistoryDashboard } from '@/api/spread'
import { getContracts } from '@/api/spread'

export default {
  name: 'SpreadHistory',
  props: { mode: { type: String, default: 'history' }},
  data() {
    const today = new Date()
    const fmt = d => [d.getFullYear(), String(d.getMonth() + 1).padStart(2, '0'), String(d.getDate()).padStart(2, '0')].join('-')
    const end = fmt(today)
    const start = fmt(new Date(today.getTime() - 29 * 86400000)) // 默认近一月
    return {
      productOptions: ['cu', 'zn', 'al', 'ni', 'i', 'au', 'ih', 'if', 'ic', 'im'],
      product: 'cu',
      range: [start, end],
      sessions: 'all',
      contractMode: 'auto',
      contractOptions: [], // [{value,label}]
      nearContract: '',
      farContract: '',

      pickerOptions: {
        shortcuts: [
          { text: '近7天', onClick: picker => {
            const e = new Date(); const s = new Date(e.getTime() - 6 * 86400000)
            picker.$emit('pick', [fmt(s), fmt(e)])
          } },
          { text: '近15天', onClick: picker => {
            const e = new Date(); const s = new Date(e.getTime() - 14 * 86400000)
            picker.$emit('pick', [fmt(s), fmt(e)])
          } },
          { text: '近1个月', onClick: picker => {
            const e = new Date(); const s = new Date(e.getTime() - 29 * 86400000)
            picker.$emit('pick', [fmt(s), fmt(e)])
          } },
          { text: '近2个月', onClick: picker => {
            const e = new Date(); const s = new Date(e.getTime() - 59 * 86400000)
            picker.$emit('pick', [fmt(s), fmt(e)])
          } }
        ]
      },

      loading: false,
      x: [],
      metrics: null,
      contracts: [],
      summary: null,

      volChart: null,
      deltaChart: null,
      volatChart: null,
      corrChart: null
    }
  },
  watch: {
    contractMode(val) {
      if (val === 'manual') {
        this.fetchContractOptions().then(() => this.loadData())
      } else {
        this.loadData()
      }
    },
    product() {
      if (this.contractMode === 'manual') {
        this.fetchContractOptions().then(() => this.loadData())
      } else {
        this.loadData()
      }
    },
    tradingDay() {
      if (this.contractMode === 'manual') {
        this.fetchContractOptions().then(() => this.loadData())
      } else {
        this.loadData()
      }
    },
    sessions() { this.loadData() },
    groupSize() { this.loadData() }
  },
  mounted() {
    if (this.mode === 'today') {
      this.$nextTick(() => {
        this.initCharts()
        // 手动模式时，需要先拉合约列表
        if (this.contractMode === 'manual') {
          this.fetchContractOptions().then(() => this.loadData())
        } else {
          this.loadData()
        }
        window.addEventListener('resize', this.handleResize)
      })
    }
  },
  beforeDestroy() {
    window.removeEventListener('resize', this.onResize)
    ;['volChart', 'deltaChart', 'volatChart', 'corrChart'].forEach(k => {
      if (this[k]) { this[k].dispose(); this[k] = null }
    })
  },
  methods: {
    initCharts() {
      if (!this.volChart && this.$refs.volChart) this.volChart = echarts.init(this.$refs.volChart)
      if (!this.deltaChart && this.$refs.deltaChart) this.deltaChart = echarts.init(this.$refs.deltaChart)
      if (!this.volatChart && this.$refs.volatChart) this.volatChart = echarts.init(this.$refs.volatChart)
      if (!this.corrChart && this.$refs.corrChart) this.corrChart = echarts.init(this.$refs.corrChart)
    },
    async fetchContractOptions() {
      try {
        // 用区间的“结束日”作为 on（后端会按这个日子过滤 last_trade_date）
        const on = this.range && this.range.length === 2 ? this.range[1] : undefined
        const resp = await getContracts({ product: this.product, on })
        if (resp.code !== 20000) {
          this.$message.error(resp.message || '加载合约失败')
          this.contractOptions = []
          // 强制清空选择
          this.nearContract = ''
          this.farContract = ''
          return
        }

        const opt = resp.data || {}
        // 统一成 {label, value}
        const list = Array.isArray(opt.options) ? opt.options : []
        this.contractOptions = list.map(o => typeof o === 'string' ? ({ label: o, value: o }) : o)

        // ✅ 关键：无论如何都置空，页面显示占位符“选择近月/远月”
        this.nearContract = ''
        this.farContract = ''
      } catch (e) {
        console.error(e)
        this.$message.error('加载合约失败')
        this.contractOptions = []
        this.nearContract = ''
        this.farContract = ''
      }
    },
    async loadData() {
      if (!this.range || this.range.length !== 2) return
      try {
        this.loading = true
        const params = {
          product: this.product,
          start: this.range[0],
          end: this.range[1],
          sessions: this.sessions
        }
        if (this.contractMode === 'manual') {
          params.near_contract = this.nearContract.trim()
          params.far_contract = this.farContract.trim()
        }
        const resp = await getHistoryDashboard(params)
        if (resp.code !== 20000) {
          this.$message.error(resp.message || '获取数据失败')
          this.x = []; this.metrics = null; this.contracts = []; this.summary = null
          this.render(true)
          return
        }
        const payload = resp.data || {}
        this.x = payload.x || []
        this.metrics = payload.metrics || null
        this.contracts = payload.contracts || []
        this.summary = payload.summary || null
        if (!this.x.length) {
          this.$message.info(resp.message || '暂无数据')
        }
        this.render(false)
      } catch (e) {
        console.error(e)
        this.$message.error('请求失败')
      } finally {
        this.loading = false
      }
    },
    render(clearOnly) {
      this.initCharts()
      const grid = { left: 50, right: 20, top: 20, bottom: 60 }
      const axisLabel = { rotate: 45 }

      // 成交量
      if (clearOnly || !this.metrics) {
        this.volChart && this.volChart.clear()
      } else if (this.volChart) {
        this.volChart.setOption({
          tooltip: { trigger: 'axis' },
          legend: { data: ['近月', '远月'] },
          grid, xAxis: { type: 'category', data: this.x, axisLabel }, yAxis: { type: 'value' },
          series: [
            { name: '近月', type: 'line', data: this.metrics.vol_near || [] },
            { name: '远月', type: 'line', data: this.metrics.vol_far || [] }
          ]
        }, true)
      }

      // 价格变化量
      if (clearOnly || !this.metrics) {
        this.deltaChart && this.deltaChart.clear()
      } else if (this.deltaChart) {
        this.deltaChart.setOption({
          tooltip: { trigger: 'axis' },
          legend: { data: ['近月', '远月'] },
          grid, xAxis: { type: 'category', data: this.x, axisLabel }, yAxis: { type: 'value' },
          series: [
            { name: '近月', type: 'line', data: this.metrics.dP_near || [] },
            { name: '远月', type: 'line', data: this.metrics.dP_far || [] }
          ]
        }, true)
      }

      // 波动率
      if (clearOnly || !this.metrics) {
        this.volatChart && this.volatChart.clear()
      } else if (this.volatChart) {
        this.volatChart.setOption({
          tooltip: { trigger: 'axis' },
          legend: { data: ['近月', '远月'] },
          grid, xAxis: { type: 'category', data: this.x, axisLabel }, yAxis: { type: 'value' },
          series: [
            { name: '近月', type: 'line', data: this.metrics.volat_near || [] },
            { name: '远月', type: 'line', data: this.metrics.volat_far || [] }
          ]
        }, true)
      }

      // 与价差相关性
      if (clearOnly || !this.metrics) {
        this.corrChart && this.corrChart.clear()
      } else if (this.corrChart) {
        this.corrChart.setOption({
          tooltip: { trigger: 'axis' },
          legend: { data: ['近月', '远月'] },
          grid,
          xAxis: { type: 'category', data: this.x, axisLabel },
          yAxis: { type: 'value', min: -1, max: 1 },
          series: [
            { name: '近月', type: 'bar', data: this.metrics.corr_spread_near || [] },
            { name: '远月', type: 'bar', data: this.metrics.corr_spread_far || [] }
          ]
        }, true)
      }
    },
    onResize() {
      this.volChart && this.volChart.resize()
      this.deltaChart && this.deltaChart.resize()
      this.volatChart && this.volatChart.resize()
      this.corrChart && this.corrChart.resize()
    },
    fmtInt(v) {
      if (v === null || v === undefined) return '-'
      const n = Number(v); if (Number.isNaN(n)) return String(v)
      return n.toLocaleString()
    },
    fmtNum(v, d = 2) {
      if (v === null || v === undefined) return '-'
      const n = Number(v); if (Number.isNaN(n)) return String(v)
      return n.toFixed(d)
    }
  }
}
</script>



<!-- 当前文件私有样式 -->
<style scoped>
.filter-bar { margin-bottom: 10px; }
.chart { width: 100%; height: 320px; }
.chart-title { font-size: 13px; margin: 4px 0 8px; }
.empty-hint { color: #909399; padding: 12px 0; }
</style>
