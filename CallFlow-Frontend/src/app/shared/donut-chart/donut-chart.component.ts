import { Component, Input, OnInit, ViewChild } from '@angular/core';
import {
  ChartComponent,
  ApexNonAxisChartSeries,
  ApexChart,
  ApexPlotOptions,
  ApexStroke,
  ApexFill
} from 'ng-apexcharts';

/**
 * REUSABLE DONUT CHART COMPONENT - Using ApexCharts
 * 
 * Purpose: Display circular/donut progress charts
 * 
 * How to use:
 * <app-donut-chart 
 *   [title]="'Cause1'"
 *   [percentage]="75"
 *   [color]="'#9333ea'"
 *   [details]="['4 cells', 'HO SR drop']">
 * </app-donut-chart>
 */

export type DonutChartOptions = {
  series: ApexNonAxisChartSeries;
  chart: ApexChart;
  plotOptions: ApexPlotOptions;
  stroke: ApexStroke;
  fill: ApexFill;
  colors: string[];
};
@Component({
  selector: 'app-donut-chart',
  templateUrl: './donut-chart.component.html',
  styleUrl: './donut-chart.component.css'
})
export class DonutChartComponent {
@ViewChild('chart') chart!: ChartComponent;
  
  // Input properties from parent
  @Input() title: string = '';           // Chart title (e.g., "Cause1")
  @Input() percentage: number = 0;       // Percentage value (0-100)
  @Input() centerText: string = '';      // Text in center (e.g., "Customers Impacted")
  @Input() centerValue: string = '';     // Value in center (e.g., "77")
  @Input() color: string = '#9333ea';    // Chart color
  @Input() details: string[] = [];       // Array of detail strings below chart
  @Input() cellCount: string = '';       // Cell count text (e.g., "4 cells")

  // ApexCharts configuration
  public chartOptions!: Partial<DonutChartOptions>;

  ngOnInit() {
    this.prepareChartData();
  }

  /**
   * Prepares chart configuration for ApexCharts
   * 
   * ApexCharts radialBar creates beautiful circular progress charts
   * Much better than manual SVG circles!
   */
  prepareChartData() {
    this.chartOptions = {
      series: [this.percentage],  // The percentage to show
      chart: {
        type: 'radialBar',
        height: 400,
        animations: {
          enabled: true,
          speed: 800,
          animateGradually: {
            enabled: true,
            delay: 200
          }
        }
      },
      plotOptions: {
        radialBar: {
          hollow: {
            size: '50%',  // Size of the hollow center
            background: 'transparent'
          },
          track: {
            background: '#e5e7eb',  // Light gray background track
            strokeWidth: '100%'
          },
          dataLabels: {
            show: true,
            name: {
              show: true,
              fontSize: '12px',
              color: '#666',
              offsetY: -10
            },
            value: {
              show: true,
              fontSize: '24px',
              fontWeight: 700,
              color: '#333',
              offsetY: 5,
              formatter: (val) => {
                return this.centerValue || val.toString();
              }
            }
          }
        }
      },
      stroke: {
        lineCap: 'round'  // Rounded ends on the progress bar
      },
      colors: [this.color],  // Chart color
      fill: {
        type: 'solid'
      }
    };
  }
}