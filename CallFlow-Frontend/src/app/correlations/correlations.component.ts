import { Component, Input } from '@angular/core';
import { Router } from '@angular/router';
import { ApiService } from '../services/api.service';
import { NodeItem } from '../nodes-per-cell/nodes-per-cell.component';
import { KpiItem } from '../shared/kpi-line-chart/kpi-line-chart.component';
import { OnInit, OnChanges, SimpleChanges } from '@angular/core';
@Component({
  selector: 'app-correlations',
  templateUrl: './correlations.component.html',
  styleUrl: './correlations.component.css'
})
export class CorrelationsComponent   implements OnInit, OnChanges{
  constructor(private router: Router, private apiService: ApiService) { }

  // Top Cards Data
  // totalComplaints = 13;
  // topCluster = 'L09_L1597_02';
  // topHandset = 'Samsung';

  // Failure Rate by Cluster Chart Data
  failureRateChartData: any[] = [];

  // Failure Rate Cards Data (NEW)
  failureRateData: any[] = [];

  // Top Nodes by Impact Chart Data
  topNodesChartData: any[] = [];

  kpisData: KpiItem[] = [];

  @Input() totalComplaints: number;
  @Input() topCell: string;
  @Input() topHandset: string;
  @Input() Abnormal_Kpis_RawData: any;

  // Rendering cap: each KPI card renders its own chart. With many affected cells
  // (the orchestration streams every affected cell x degraded KPI) rendering
  // hundreds of charts synchronously freezes the tab. We reuse the already-loaded
  // orchestration data (no re-fetch) but cap how many cards we render at once.
  readonly MAX_KPI_CARDS = 24;
  totalKpiCount = 0;
  truncatedKpiCount = 0;

  ngOnInit(): void {
    this.initializeFailureRateCards();
    this.initializeTopNodesChart();
    this.initializeKPIs()
    console.log('Abnormal_Kpis_in correlations')
    console.log(this.Abnormal_Kpis_RawData)
  }
  ngOnChanges(changes: SimpleChanges) {
    if (changes['Abnormal_Kpis_RawData']) {
      this.initializeKPIs();
    }
  }
  initializeFailureRateCards(): void {
    this.failureRateData = [
      {
        clusterName: 'Downtown Metro',
        location: 'Cairo',
        failureRate: 20,
        users: 89,
        cells: 28,
        tacs: 2,
        anomaly: 85,
        highlighted: false
      },
      {
        clusterName: 'Industrial Zone',
        location: 'Alex',
        failureRate: 13,
        users: 89,
        cells: 28,
        tacs: 2,
        anomaly: 35,
        highlighted: false
      },
      {
        clusterName: 'Downtown Metro',
        location: 'Cairo',
        failureRate: 12,
        users: 89,
        cells: 28,
        tacs: 2,
        anomaly: 85,
        highlighted: false
      }
    ];
  }
  initializeTopNodesChart(): void {
    this.topNodesChartData = [
      { label: 'eNodeB1', value: 320 },
      { label: 'eNodeB2', value: 280 },
      { label: 'eNodeB3', value: 245 },
      { label: 'eNodeB4', value: 210 },
      { label: 'eNodeB5', value: 185 },
      { label: 'eNodeB6', value: 165 }

    ];
  }
  initializeNodesperCell(): void {
    // nodesPerClusterData: NodeItem[] = [
    //   {
    //     nodeName: 'eNodeB_6',
    //     band: 'B20-800MHz',
    //     tac: 'TAC_1006',
    //     cellsCount: 3,
    //     anomaly: 73,
    //     users: 25
    //   },
    //   ...
    // ];
  }

  nodesPerCellData: NodeItem[] = [
    {
      nodeName: 'eNodeB_6',
      band: 'B20-800MHz',
      tac: 'TAC_1006',
      cellsCount: 3,
      anomaly: 73,
      users: 25
    },
    {
      nodeName: 'eNodeB_6',
      band: 'B20-800MHz',
      tac: 'TAC_1006',
      cellsCount: 3,
      anomaly: 73,
      users: 25
    },
    {
      nodeName: 'eNodeB_6',
      band: 'B20-800MHz',
      tac: 'TAC_1006',
      cellsCount: 3,
      anomaly: 73,
      users: 25
    },
    {
      nodeName: 'eNodeB_6',
      band: 'B20-800MHz',
      tac: 'TAC_1006',
      cellsCount: 3,
      anomaly: 73,
      users: 25
    },
    {
      nodeName: 'eNodeB_6',
      band: 'B20-800MHz',
      tac: 'TAC_1006',
      cellsCount: 3,
      anomaly: 73,
      users: 25
    },
    {
      nodeName: 'eNodeB_6',
      band: 'B20-800MHz',
      tac: 'TAC_1006',
      cellsCount: 3,
      anomaly: 73,
      users: 25
    },
  ]
  Select_Abnormal_System: any = 'Npp'
  Abnormal_System = [
    { label: 'Splunk', value: 'Splunk' },
    { label: 'NPP', value: 'NPP' },
    { label: 'Transmission', value: 'Viavi' },
  ];
  updateAbnormalChart() { }
  /**
     * Initialize KPI metrics data
     * this.kpisData = [
        {
          kpiName: 'Availability',
          value: 96.3,
          unit: '%',
          delta: -1.7,
          anomalies: 2,
          baseline: 98.0,
          series: [96.5, 96.6, 96.4, 96.7, 96.8, 96.6, 95.9, 96.3],
          timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35']
        }]
     * 
     */
  initializeKPIs(): void {
    this.Select_Abnormal_System = 'NPP'
    this.kpisData = this.prepareAbnormalKpiChartData(
      this.Abnormal_Kpis_RawData
    );
    // this.kpisData = [
    //   {
    //     kpiName: 'Availability',
    //     value: 96.3,
    //     // unit: '%',
    //     // delta: -1.7,
    //     anomalies: 2,
    //     // baseline: 98.0,
    //     series: [96.5, 96.6, 96.4, 96.7, 96.8, 96.6, 95.9, 96.3],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35']
    //   },
    //   {
    //     kpiName: 'Throughput',
    //     value: 76.3,
    //     // unit: ' Mbps',
    //     // delta: 17.4,
    //     anomalies: 7,
    //     // baseline: 65.0,
    //     series: [60, 68, 72, 66, 70, 69, 75, 76.3],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35'],

    //   },
    //   {
    //     kpiName: 'Latency',
    //     value: 40.7,
    //     // unit: ' ms',
    //     // delta: -9.5,
    //     anomalies: 10,
    //     // baseline: 45.0,
    //     series: [42, 41, 43, 44, 42, 41, 39, 40.7],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35'],

    //   },
    //   {
    //     kpiName: 'Call Drop Rate',
    //     value: 1.3,
    //     // unit: '%',
    //     // delta: -34.1,
    //     anomalies: 20,
    //     // baseline: 2.0,
    //     series: [1.1, 1.4, 1.2, 1.6, 1.3, 1.5, 1.4, 1.3],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35'],


    //   },
    //   {
    //     kpiName: 'HO Success Rate',
    //     value: 97.5,
    //     // unit: '%',
    //     // delta: 2.6,
    //     anomalies: 2,
    //     // baseline: 95.0,
    //     series: [96.8, 97.0, 97.2, 97.1, 97.3, 97.4, 97.5],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35'],


    //   },
    //   {
    //     kpiName: 'RLF Spikes',
    //     value: 2.7,
    //     // unit: '',
    //     // delta: 170.1,
    //     anomalies: 22,
    //     // baseline: 1.0,
    //     series: [1.2, 1.8, 1.5, 2.2, 1.9, 2.4, 2.7],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35'],

    //   },
    //         {
    //     kpiName: 'Availability',
    //     value: 96.3,
    //     // unit: '%',
    //     // delta: -1.7,
    //     anomalies: 2,
    //     // baseline: 98.0,
    //     series: [96.5, 96.6, 96.4, 96.7, 96.8, 96.6, 95.9, 96.3],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35']
    //   },
    //         {
    //     kpiName: 'Availability',
    //     value: 96.3,
    //     // unit: '%',
    //     // delta: -1.7,
    //     anomalies: 2,
    //     // baseline: 98.0,
    //     series: [96.5, 96.6, 96.4, 96.7, 96.8, 96.6, 95.9, 96.3],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35']
    //   },
    //         {
    //     kpiName: 'Availability',
    //     value: 96.3,
    //     // unit: '%',
    //     // delta: -1.7,
    //     anomalies: 2,
    //     // baseline: 98.0,
    //     series: [96.5, 96.6, 96.4, 96.7, 96.8, 96.6, 95.9, 96.3],
    //     timestamps: ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:30', '13:35']
    //   },
    // ];
  }

  onSystemChange() {
    this.kpisData = this.prepareAbnormalKpiChartData(
      this.Abnormal_Kpis_RawData
    );
  }
  
  prepareAbnormalKpiChartData(rawData: any) {

  if (!rawData) return [];

  const systemKey = this.Select_Abnormal_System?.toLowerCase();
  const systemData = rawData?.[systemKey] || [];

  const result: any[] = [];

  systemData.forEach((item: any) => {

    const degraded = item.degraded_kpis || [];

    degraded.forEach((kpi: any) => {

      const hourlyMap = new Map<string, number[]>();

      // 🔹 GROUP BY HOUR
      (kpi.kpi_data || []).forEach((x: any) => {

        const date = new Date(x.datetime);

        // Normalize to start of hour (UTC)
        date.setUTCMinutes(0, 0, 0);

        const key = date.toISOString();

        if (!hourlyMap.has(key)) {
          hourlyMap.set(key, []);
        }

        hourlyMap.get(key)!.push(Number(x.value || 0));
      });

      // 🔹 BUILD AGGREGATED ARRAYS
      const timestamps: string[] = [];
      const series: number[] = [];

      hourlyMap.forEach((values, key) => {

        // 🔸 Choose correct aggregation:
        const aggregatedValue =
          values.reduce((a, b) => a + b, 0) / values.length; // mean()

        timestamps.push(
          new Date(key).toLocaleString('en-CA', {
            timeZone: 'UTC',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            hour12: true
          }).replace(',', '')
        );

        series.push(aggregatedValue);
      });

      result.push({
        kpiName: kpi.name,
        value: Number(kpi.current_value || 0),
        anomalies: item.cell_name || item.site_name || item.Kpi_Column,
        series,
        timestamps
      });

    });

  });

  // Cap rendering to avoid freezing the tab when there are many affected cells.
  // We keep the highest-value entries first (most severe/abnormal), reusing the
  // already-retrieved orchestration data — no extra API calls.
  this.totalKpiCount = result.length;
  result.sort((a, b) => Number(b.value || 0) - Number(a.value || 0));
  const capped = result.slice(0, this.MAX_KPI_CARDS);
  this.truncatedKpiCount = Math.max(0, this.totalKpiCount - capped.length);

  console.log('result in abnormal', capped.length, 'of', this.totalKpiCount);

  return capped;
}
  // prepareAbnormalKpiChartData(rawData: any) {

  //   if (!rawData) return [];

  //   const systemKey = this.Select_Abnormal_System?.toLowerCase();
  //   const systemData = rawData?.[systemKey] || [];

  //   const result: any[] = [];

  //   systemData.forEach((item: any) => {
  //     // result.push({
  //     //     anomalies: item.cell_name || item.site_name,
  //     //   });
  //     const degraded = item.degraded_kpis || [];

  //     degraded.forEach((kpi: any) => {
  //       const hourlyMap = new Map<string, number[]>();

  //       result.push({
  //         kpiName: kpi.name,
  //         value: Number(kpi.current_value || 0),
  //         anomalies: item.cell_name || item.site_name,

  //         series: (kpi.kpi_data || []).map((x: any) =>
  //           Number(x.value || 0)
  //         ),

  //         timestamps: (kpi.kpi_data || []).map((x: any) =>
  //           new Date(x.datetime).toLocaleString('en-CA', {
  //             timeZone: 'UTC',
  //             year: 'numeric',
  //             month: '2-digit',
  //             day: '2-digit',
  //             hour: '2-digit',
  //             minute: '2-digit',
  //             hour12: true
  //           }).replace(',', '')
  //         )

  //       });

  //     });
  //    console.log('result in abnormal',result)
  //   });

  //   return result;
  // }
}