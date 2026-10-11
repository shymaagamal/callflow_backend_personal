import { Component, Input } from '@angular/core';
import { KpiItem } from '../shared/kpi-line-chart/kpi-line-chart.component';

@Component({
  selector: 'app-kpis-per-cell',
  templateUrl: './kpis-per-cell.component.html',
  styleUrl: './kpis-per-cell.component.css'
})
export class KpisPerCellComponent {
  // @Input() kpis: KpiItem[] = [];

  // kpisData: KpiItem[] = [
  // {
  //   kpiName: 'Availability',
  //   value: 96.3,
  //   unit: '%',
  //   delta: -1.7,
  //   anomalies: 2,
  //   baseline: 98.0,
  //   series: [96.5, 96.6, 96.4, 96.7, 96.8, 96.6, 95.9, 96.3],
  //   color: 'green' // blue
  // },
  // {
  //   kpiName: 'Throughput',
  //   value: 76.3,
  //   unit: ' Mbps',
  //   delta: 17.4,
  //   anomalies: 7,
  //   baseline: 65.0,
  //   series: [60, 68, 72, 66, 70, 69, 75, 76.3],
  //   color: 'blue' // green
  // },
  // {
  //   kpiName: 'Latency',
  //   value: 40.7,
  //   unit: ' ms',
  //   delta: -9.5,
  //   anomalies: 10,
  //   baseline: 45.0,
  //   series: [42, 41, 43, 44, 42, 41, 39, 40.7],
  //   color: 'orange' // orange
  // },
  // {
  //   kpiName: 'Call Drop Rate',
  //   value: 1.3,
  //   unit: '%',
  //   delta: -34.1,
  //   anomalies: 20,
  //   baseline: 2.0,
  //   series: [1.1, 1.4, 1.2, 1.6, 1.3, 1.5, 1.4, 1.3],
  //   color: 'blue' // red
  // },
  // {
  //   kpiName: 'HO Success Rate',
  //   value: 97.5,
  //   unit: '%',
  //   delta: 2.6,
  //   anomalies: 2,
  //   baseline: 95.0,
  //   series: [96.8, 97.0, 97.2, 97.1, 97.3, 97.4, 97.5],
  //   color: 'green' // indigo
  // },
  // {
  //   kpiName: 'RLF Spikes',
  //   value: 2.7,
  //   unit: '',
  //   delta: 170.1,
  //   anomalies: 22,
  //   baseline: 1.0,
  //   series: [1.2, 1.8, 1.5, 2.2, 1.9, 2.4, 2.7],
  //   color: 'red' // pink
  // }]

}
