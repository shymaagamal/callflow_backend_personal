import { Component, Input } from '@angular/core';
import {
  ApexAxisChartSeries,
  ApexChart,
  ApexStroke,
  ApexFill,
  ApexTooltip,
  ApexXAxis,
  ApexYAxis
} from 'ng-apexcharts';

export interface KpiItem {
  kpiName: string;        // "Availability"
  value: number;         // 96.3
  // unit: string;          // "%"
  // delta: number;         // -1.7
  anomalies: string;     // 2
  // baseline: number;      // 98.0
  series: number[];      // 🔥 line chart data
  timestamps?: string[];
}

export type KpiChartOptions = {
  series: ApexAxisChartSeries;
  chart: ApexChart;
  stroke: ApexStroke;
  fill: ApexFill;
  tooltip: ApexTooltip;
  xaxis: ApexXAxis;
  yaxis: ApexYAxis;
  colors: string[];
};

@Component({
  selector: 'app-kpi-line-chart',
  templateUrl: './kpi-line-chart.component.html',
  styleUrl: './kpi-line-chart.component.css'
})
export class KpiLineChartComponent {

  @Input() series: number[] = [];
  @Input() showXAxis: boolean = false;  // NEW: Show x-axis labels
  @Input() showYAxis: boolean = false;  // NEW: Show y-axis labels
  @Input() timestamps?: string[];
  @Input() index: number = 0;

  private readonly colors = [
  '#ef4444', // red
  '#10b981', // green
  '#3b82f6', // blue
  '#f59e0b',  // orange
  '#8B5CF6',
  '#EC4899',
  '#06B6D4'
];
  public chartOptions!: Partial<KpiChartOptions>;

  ngOnInit() {
    this.initializeChart();
  }

  ngOnChanges() {
    if (this.series?.length) {
      this.initializeChart();
      console.log('Time', this.timestamps)
    }
  }



  initializeChart() {
    // Map color name to hex code
    const colorMap = {
      blue: '#3B82F6',
      green: '#10B981',
      orange: '#F59E0B',
      purple: '#8B5CF6'
    };

    const hexColor = this.colors[this.index % this.colors.length];

    const chartData = this.timestamps?.length
      ? this.series.map((val, i) => ({
        x: this.timestamps![i],  // timestamp
        y: val                   // value
      }))
      : this.series;

    this.chartOptions = {
      series: [{
        name: 'Value',
        data: chartData
      }],
      chart: {
        type: 'area',
        height: 60,
        sparkline: {
          enabled: !this.showXAxis && !this.showYAxis
        },

        animations: {
          enabled: true,
          speed: 400
        }
      },
      stroke: {
        curve: 'smooth',
        width: 2
      },
      fill: {
        type: 'gradient',
        gradient: {
    shade: 'light',
    type: 'vertical',
    shadeIntensity: 0.8,
    inverseColors: false,
    opacityFrom: 0.45,
    opacityTo: 0.05,
    stops: [10, 200, 150]
  }
      },
      colors: [hexColor],
tooltip: {
  enabled: true,
  x: {
    formatter: (_: any, opts) => {
      const index = opts.dataPointIndex;
      return this.timestamps?.[index] ?? '';
    }
  },
  // y: {
  //   formatter: (val: number) => `Value: ${val}`
  // }
},
      xaxis: this.timestamps ? {
        type: 'category',
        labels: {
          show: this.showXAxis
        }
      } : undefined,

      yaxis: {
        show: this.showYAxis
      },

    };
  }
}