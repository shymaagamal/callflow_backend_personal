import { Component, OnChanges, SimpleChanges } from '@angular/core';
import { Input, OnInit, ViewChild } from '@angular/core';
import {
  ChartComponent,
  ApexAxisChartSeries,
  ApexChart,
  ApexXAxis,
  ApexYAxis,
  ApexDataLabels,
  ApexPlotOptions,
  ApexLegend,
  ApexGrid,
  ApexTooltip
} from 'ng-apexcharts';

/**
 * REUSABLE BAR CHART COMPONENT - Using ApexCharts
 * 
 * Purpose: Display bar charts with variance indicators
 * 
 * How to use:
 * <app-bar-chart 
 *   [title]="'Abnormal KPIs'"
 *   [data]="chartData"
 *   [showLegend]="true">
 * </app-bar-chart>
 * 
 * Where chartData is an array like:
 * [
 *   { label: 'KPI Name', value: 250, variance: 'high' },
 *   { label: 'KPI Name', value: 200, variance: 'medium' }
 * ]
 */

// Type for chart options
export type ChartOptions = {
  series: ApexAxisChartSeries;
  chart: ApexChart;
  xaxis: ApexXAxis;
  yaxis: ApexYAxis;
  dataLabels: ApexDataLabels;
  plotOptions: ApexPlotOptions;
  legend: ApexLegend;
  grid: ApexGrid;
  tooltip: ApexTooltip;
  colors: string[];
};

@Component({
  selector: 'app-bar-chart',
  templateUrl: './bar-chart.component.html',
  styleUrl: './bar-chart.component.css'
})
export class BarChartComponent {
  @ViewChild('chart') chart!: ChartComponent;

  // Input properties
  @Input() title: string = '';
  @Input() data: any[] = [];
  @Input() showLegend: boolean = false;
  @Input() Abnormal_KPIs_Legend: boolean = true;
  @Input() yAxisLabel: string = '';
  @Input() Colwidth: string = '14px';
  @Input() barHeight: string = '24px';

  @Input() horizontal_option: boolean = false;
  @Input() height: number = 300;
  @Input() DEFAULT_BAR_COLOR = '#007C92';
  @Input() barRadius: number = 8;          // Border radius
  @Input() showValueAtEnd: boolean = false; // Show value at end of bar
  @Input() valueOffset: number = 10;       // Distance from bar end
  @Input() enableAbnormalTooltip = false;
  @Input() RemedyTooltip = false;

  // ApexCharts configuration
  public chartOptions!: Partial<ChartOptions>;

  ngOnChanges(changes: SimpleChanges) {
    if (changes['data'] && this.data?.length) {
      console.log('BarChart received data:', this.data);
      this.prepareChartData();
    }
  }
  ngOnInit() {
    this.prepareChartData();
  }


  /**
   * Prepares data in ApexCharts format
   * 
   * ApexCharts expects data in this structure:
   * {
   *   series: [{ data: [value1, value2, ...] }],
   *   xaxis: { categories: ['Label1', 'Label2', ...] },
   *   colors: [color1, color2, ...]
   * }
   */

  prepareChartData() {
    // Extract labels and values from data array
    const labels = this.data.map(item => item.label || '');
    const values = this.data.map(item => item.value || 0);

    // Assign colors based on variance level
    const colors = this.data.map(item => this.getBarColor(item.variance));

    // Configure ApexCharts options
    this.chartOptions = {
      series: [
        {
          name: this.yAxisLabel || 'Value',
          data: values
        }
      ],
      chart: {
        type: 'bar',
        height: this.horizontal_option ? 350 : 300,
        toolbar: {
          show: false  // Hide toolbar for cleaner look
        },
        animations: {
          enabled: true,
          speed: 800,
          animateGradually: {
            enabled: true,
            delay: 150
          }
        }
      },
      plotOptions: {
        bar: {
          horizontal: this.horizontal_option,  // This makes bars horizontal!
          borderRadius: this.barRadius,
          columnWidth: this.Colwidth,
          barHeight: this.horizontal_option ? this.barHeight : '50px',
          distributed: true,  // Each bar gets its own color
          // dataLabels: {
          //   position: this.horizontal_option ? 'right' : 'top'
          // }
        }

      },
      dataLabels: {
        enabled: this.horizontal_option ? true : false  // Hide values on top of bars
      },
      xaxis: {
        categories: labels,
        labels: {
          style: {
            colors: '#666',
            fontSize: '12px'
          }
        }
      },
      yaxis: {
        title: {
          text: this.yAxisLabel,
          style: {
            color: '#666',
            fontSize: '12px',
            fontWeight: 500
          }
        },
        labels: {
          style: {
            colors: '#666'
          }
        }
      },
      colors: colors,  // Different color for each bar
      legend: {
        show: false  // We'll create custom legend
      },
      grid: {
        borderColor: '#f0f0f0',
        strokeDashArray: 8
      },
      tooltip: {
        enabled: true,
        custom: ({ series, seriesIndex, dataPointIndex, w }) => {
          const cell = this.data[dataPointIndex];
          console.log('Cell------------')
          console.log(cell)
          if (this.enableAbnormalTooltip) {
            console.log('weeeee are hereeeee')
            let tooltipBody = '';
            if (cell.tooltipData?.length) {
              tooltipBody = cell.tooltipData.map((kpi: any) => `
                    <div style="margin-bottom:4px;">
                      <strong>${kpi.name}:</strong><br/>
                      <span>Mean:  ${kpi.current_value}</span>
                    </div>
                    <div style="margin-bottom:4px;">
                      <strong style="color:#007C92;"=>${kpi.message}</strong><br/>
                    </div>
                  `).join('');
            }

            return `
                  <div style="
                    background: rgba(0,0,0,0.85);
                    color: white;
                    padding: 14px;
                    border-radius: 6px;
                    max-width: 380px;
                    font-size: 13px;
                    
                    white-space: normal;
                    word-wrap: break-word;
                    overflow-wrap: break-word;
                  ">
                    <strong>${cell.label}</strong><br/>
                    <span>No. Customers: ${cell.value}</span>
                    ${tooltipBody ? `
                      <hr style="border:0.5px solid #666; margin:6px 0;" />
                      ${tooltipBody}
                    ` : ''}
                  </div>
                `;
          }
          if (cell.tooltipData?.length && !this.enableAbnormalTooltip) {
            let tooltipBody = '';
            if (cell.tooltipData?.length) {
              tooltipBody = cell.tooltipData.map((kpi: any) => `
                    <div style="margin-bottom:4px;">
                      <strong>Status: ${kpi.name}</strong><br/>
                      <span>Count:  ${kpi.current_value}</span>
                    </div>
                  `).join('');
            }

            return `
                  <div style="
                    background: rgba(0,0,0,0.85);
                    color: white;
                    padding: 14px;
                    border-radius: 6px;
                    max-width: 380px;
                    font-size: 13px;
                  ">
                    ${tooltipBody ? `
                      ${tooltipBody}
                    ` : ''}
                  </div>
                `;

          }
          if (!cell.tooltip && !this.enableAbnormalTooltip) {

            return `
        <div style="
          background: rgba(0,0,0,0.85);
          color: white;
          padding: 20px;
          border-radius: 6px;
          max-width: 360px;
          white-space: normal;
          font-size: 14px;
          line-height: 1.5;
        ">
          <strong>${cell.label}</strong><br/>
          <strong> No.Customer: ${cell.value} </strong>
        </div>
      `;
          }


          return `
        <div style="
          background: rgba(0,0,0,0.85);
          color: white;
          padding: 20px;
          border-radius: 6px;
          max-width: 360px;
          white-space: normal;
          font-size: 14px;
          line-height: 1.3;
        ">
          <strong>${cell.label} --- ${cell.value} Customer</strong>
          <hr style="border:0.5px solid #ccc; margin:5px 0;" />
        
          ${cell.tooltip}
        </div>
      `;
        }
      },

    };

  }

  /**
   * Returns color based on variance level
   * 
   * Variance categories:
   * - 'low' or 'less': Yellow/lime (< 30% variance)
   * - 'medium': Orange (30-100% variance)  
   * - 'high' or 'more': Red (> 100% variance)
  */

  getBarColor(variance: string): string {
    if (!variance) {
      return this.DEFAULT_BAR_COLOR;
    }
    const varMap: { [key: string]: string } = {
      'low': '#D7E51C',      // Yellow
      'medium': '#EB9700',   // Orange
      'high': '#E7000B'      // Red
    };
    return varMap[variance?.toLowerCase()] || '#dc2626';
  }

}
