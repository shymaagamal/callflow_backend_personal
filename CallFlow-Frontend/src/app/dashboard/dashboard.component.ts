// src/app/dashboard/dashboard.component.ts
/**
 * What changed vs. your version:
 *
 *   - Every API call now goes through ApiService's run*Job() methods
 *     (job-polling), matching every other component in the app. The old
 *     trigger*Backend methods are gone from ApiService -- those endpoints
 *     return {job_id} now, not data directly.
 *
 *   - PARKED per your decision, NOT touched beyond making them compile:
 *       - onSubmit() (Smartcare file-upload) -- the backend expects a JSON
 *         msisdn_list, not an uploaded file. Confirmed not currently wired.
 *       - selectTab('Probs') -- no backend endpoint exists yet for
 *         enterprise_kpis/quality_kpis/location. Confirmed not implemented.
 *     Both now show a clear message instead of silently doing nothing.
 *     IMPORTANT: every Network sub-tab below depends on
 *     suspected_sites/suspected_sgw/suspected_cells_raw, which only ever
 *     get populated from a successful Smartcare response. Until onSubmit
 *     is unblocked, those tabs have nothing to query with -- that's
 *     expected right now, not a new bug.
 *
 *   - Found and fixed a real bug while rewriting: the original MSC branch
 *     checked `subtab === 'IMS'` (a duplicate of the IMS branch above it,
 *     so it could never run, and MSC could never be reached). Changed to
 *     `subtab === 'MSC'`.
 *
 *   - Backend addition needed for this to actually render charts: the
 *     Splunk/NPP job results now include a `raw_data` field (raw
 *     per-timestamp rows as a JSON string) alongside the aggregated
 *     alarms/metrics that Summary/abnormal-KPIs use. See the backend
 *     patch notes -- apply that before testing this component, or these
 *     charts will get an empty raw_data and render nothing.
 *
 *   - Replaced ~7 nearly-identical Splunk-node blocks and 2 nearly-identical
 *     NPP blocks with small shared helpers (runSplunkNodeJob,
 *     buildChartConfigs) so adding an 8th node later is a 3-line addition,
 *     not another 35-line copy-paste block. Every individual
 *     `xKpis_chartdata` / `xKpis_chartConfigs` property your template
 *     binds to is still set exactly as before -- only HOW they get
 *     populated changed.
 *
 *   - Remedy: removed `JSON.parse()` around the ticket data. Your old
 *     backend apparently returned that field as a JSON-encoded STRING;
 *     this backend returns a plain object (DRF serializes it once, Angular's
 *     HttpClient parses the whole response body once) -- calling
 *     JSON.parse() on an object that already arrived parsed throws at
 *     runtime. FLAGGED, NOT FULLY VERIFIED: the shape of remedy_tickets
 *     from THIS backend is a dict keyed by site/cluster
 *     (e.g. { siteA: { total: N, ... } }), not necessarily a flat array of
 *     Incident rows the way displayedRemedyColumns expects. I used
 *     Object.values() as a best guess -- check this against your actual
 *     API response and adjust if each value isn't already an Incident-shaped
 *     object.
 */

import { Component, ViewChild } from '@angular/core';
import { ApiService } from '../services/api.service';
import { JobStatus } from '../services/job-polling.service';

import { MatPaginator } from '@angular/material/paginator';
import { MatTableDataSource } from '@angular/material/table';
import { MatSort } from '@angular/material/sort';
export interface Incident {
  incident_number: string;
  description: string;
  type: string;
  vendor: string;
  impact: string;
  outage: string;
  status: string;
  assigment_status: string;
  submit_date: string;
  actual_fault_time: string;
  actual_resolution_time: string;
  operational_categorization_3: string;
}

type ChartSeriesMap = Record<string, Record<string, { x: string; y: number }[]>>;
type ChartConfig = { chartTitle: string; chartData: { name: string; data: { x: Date; y: number }[] }[] };

// NOTE: class name kept as "HeaderComponent" even though this file is the
// dashboard -- that mismatch existed in the file you gave me. Renaming it
// safely means updating every place it's imported (app.module.ts etc.),
// which I can't see, so left as-is to avoid breaking something I can't
// verify. Worth a dedicated rename pass later if you want.
@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrl: './dashboard.component.css',
})
export class HeaderComponent {
  smartcareLoaded = false;
  smartcareLoading = false;
  smartcareError: string | null = null;
  start_date: any;
  end_date: any;
  msisdn: any;
  imsi: any = '';
  selectedFile: File | null = null;
  inputs_submitted = false;

  raw_data: any = {};
  smartcare_data: any = {};
  smartcare_enterprise_kpis_data: any[] = [];
  smartcare_quality_kpis_data: any[] = [];
  location_info: any;
  viavi_data: any[] = [];
  splunk_data: any[] = [];
  npp_data: any[] = [];
  npp_hourly_data: any = [];
  remedy_data: any[] = [];
  remedy_status_count: any;

  suspected_cells_raw: any[] = [];
  suspected_sgw: any[] = [];
  suspected_sites: any[] = [];

  npp_chartdata: any;
  npp_hourly_chartdata: any;

  ggsnKpis_chartdata: any;
  gifwKpis_chartdata: any;
  dnsKpis_chartdata: any;
  imsKpis_chartdata: any;
  sbgKpis_chartdata: any;
  bgfKpis_chartdata: any;
  mscKpis_chartdata: any;
  mmeKpis_chartdata: any;
  totalKpis_chartdata: any;

  viavi_chartdata: any;

  smartcare_enterprise_kpis_chartConfigs: any[] = [];
  smartcare_quality_kpis_chartConfigs: any[] = [];

  npp_chartConfigs: ChartConfig[] = [];
  npp_hourly_chartConfigs: ChartConfig[] = [];

  ggsnKpis_chartConfigs: ChartConfig[] = [];
  gifwKpis_chartConfigs: ChartConfig[] = [];
  dnsKpis_chartConfigs: ChartConfig[] = [];
  imsKpis_chartConfigs: ChartConfig[] = [];
  sbgKpis_chartConfigs: ChartConfig[] = [];
  bgfKpis_chartConfigs: ChartConfig[] = [];
  mscKpis_chartConfigs: ChartConfig[] = [];
  mmeKpis_chartConfigs: ChartConfig[] = [];
  totalKpis_chartConfigs: ChartConfig[] = [];

  viavi_chartConfigs: ChartConfig[] = [];

  selectedCategoryTab = 'Network';
  selectedCategorySubTab = 'Total';

  displayedRemedyColumns: string[] = [
    'incident_number', 'site', 'vendor', 'type', 'impact', 'outage', 'status',
    'assigment_status', 'submit_date', 'actual_fault_time', 'actual_resolution_time',
    'operational_categorization_3', 'description',
  ];

  dataSource = new MatTableDataSource<Incident>();

  @ViewChild(MatPaginator) paginator!: MatPaginator;
  @ViewChild(MatSort) sort!: MatSort;

  constructor(private apiService: ApiService) {}

  // ============================================================
  // Chart data formatters -- UNCHANGED from your original file
  // ============================================================

  formatChartData(categoryData: any[]): ChartSeriesMap {
    const kpiMap: ChartSeriesMap = {};
    for (const entry of categoryData) {
      const timestamp = entry.timestamp;
      for (const [key, value] of Object.entries(entry)) {
        if (key === 'timestamp') continue;
        const [prefix, kpi, site] = key.split('_');
        if (!kpiMap[kpi]) kpiMap[kpi] = {};
        if (!kpiMap[kpi][site]) kpiMap[kpi][site] = [];
        if (value !== null && value !== undefined) {
          kpiMap[kpi][site].push({ x: timestamp, y: value as number });
        }
      }
    }
    return kpiMap;
  }

  formatChartData2(Data: any[]): ChartSeriesMap {
    const kpiMap: ChartSeriesMap = {};
    for (const entry of Data) {
      const cell = entry['Cell'];
      const rawTimestamp = entry['DATETIME_ID'];
      if (!cell || !rawTimestamp) continue;
      const timestamp = new Date(rawTimestamp).toISOString().slice(0, 16);
      for (const [key, value] of Object.entries(entry)) {
        if (['Cell', 'DATETIME_ID', 'Vendor'].includes(key)) continue;
        if (!kpiMap[key]) kpiMap[key] = {};
        if (!kpiMap[key][cell]) kpiMap[key][cell] = [];
        if (value !== null && value !== undefined) {
          kpiMap[key][cell].push({ x: timestamp, y: Number(value) });
        }
      }
    }
    return kpiMap;
  }

  formatChartData5(Data: any[]): ChartSeriesMap {
    const kpiMap: ChartSeriesMap = {};
    for (const entry of Data) {
      const flwid = entry['FLWID'];
      const rawTimestamp = entry['PERIOD_TIMESTAMP'];
      if (!flwid || !rawTimestamp) continue;
      const timestamp = new Date(rawTimestamp).toISOString().slice(0, 16);
      for (const [key, value] of Object.entries(entry)) {
        if (['FLWID', 'PERIOD_TIMESTAMP'].includes(key)) continue;
        if (!kpiMap[key]) kpiMap[key] = {};
        if (!kpiMap[key][flwid]) kpiMap[key][flwid] = [];
        if (value !== null && value !== undefined) {
          kpiMap[key][flwid].push({ x: timestamp, y: Number(value) });
        }
      }
    }
    return kpiMap;
  }

  /** Shared transform used after every formatChartData* call -- was duplicated 9 times originally. */
  private buildChartConfigs(kpiMap: ChartSeriesMap): ChartConfig[] {
    return Object.entries(kpiMap).map(([kpiName, series]) => ({
      chartTitle: kpiName,
      chartData: Object.entries(series).map(([seriesName, points]) => ({
        name: seriesName,
        data: points.map((p) => ({ x: new Date(p.x), y: p.y })),
      })),
    }));
  }

  getCountedItems(array: string[]): { item: string; count: number }[] {
    if (!array) return [];
    const counts: { [key: string]: number } = {};
    array.forEach((x) => (counts[x] = (counts[x] || 0) + 1));
    return Object.keys(counts).map((key) => ({ item: key, count: counts[key] }));
  }

  // ============================================================
  // Smartcare submission -- PARKED (confirmed not wired to backend)
  // ============================================================

  onFileSelected(event: any) {
    this.selectedFile = event.target.files[0];
  }

  onSubmit() {
    console.warn(
      'Smartcare file-upload submission is parked: backend expects a JSON msisdn_list, ' +
      'not an uploaded file. Confirmed not currently wired -- skipping per decision.'
    );
    this.smartcareError = 'Smartcare submission is temporarily unavailable on this dashboard.';
    this.smartcareLoading = false;
    this.smartcareLoaded = false;
  }

  // ============================================================
  // Tabs
  // ============================================================

  selectTab(tab: string): void {
    this.selectedCategoryTab = tab;

    if (tab === 'Tickets') {
      this.apiService
        .runRemedyJob({
          start_date: this.start_date,
          end_date: this.end_date,
          suspected_sites: this.suspected_sites,
        })
        .subscribe({
          next: (job: JobStatus) => {
            if (job.status === 'DONE') {
              const remedyResult = job.result?.remedy_tickets || {};
              this.remedy_status_count = job.result?.status;
              // See file header note -- verify this mapping against the real response.
              this.remedy_data = Object.values(remedyResult) as Incident[];
              this.dataSource = new MatTableDataSource(this.remedy_data);
              if (this.paginator) this.dataSource.paginator = this.paginator;
              if (this.sort) this.dataSource.sort = this.sort;
            } else if (job.status === 'FAILED') {
              console.error('Remedy job failed:', job.error);
            }
          },
          error: (err) => console.error('Remedy request error:', err),
        });
    }

    if (tab === 'Probs') {
      console.warn(
        'Probs tab is parked: no backend endpoint for enterprise_kpis/quality_kpis/location yet. ' +
        'Confirmed not implemented -- skipping per decision.'
      );
    }
  }

  // ============================================================
  // Network sub-tabs
  // ============================================================

  selectSubTab(subtab: string): void {
    this.selectedCategorySubTab = subtab;
    this.selectedCategoryTab = 'Network';

    if (subtab === 'Radio (15 min)') {
      this.runNppJob(false, (rows) => {
        this.npp_data = rows;
        this.npp_chartdata = this.formatChartData2(rows);
        this.npp_chartConfigs = this.buildChartConfigs(this.npp_chartdata);
      });
    } else if (subtab === 'Radio (Hourly)') {
      this.runNppJob(true, (rows) => {
        this.npp_hourly_data = rows;
        this.npp_hourly_chartdata = this.formatChartData2(rows);
        this.npp_hourly_chartConfigs = this.buildChartConfigs(this.npp_hourly_chartdata);
      });
    } else if (subtab === 'GGSN') {
      this.runSplunkNodeJob('GGSN', (rows) => {
        this.splunk_data = rows;
        this.ggsnKpis_chartdata = this.formatChartData(rows);
        this.ggsnKpis_chartConfigs = this.buildChartConfigs(this.ggsnKpis_chartdata);
      });
    } else if (subtab === 'DNS') {
      this.runSplunkNodeJob('DNS', (rows) => {
        this.splunk_data = rows;
        this.dnsKpis_chartdata = this.formatChartData(rows);
        this.dnsKpis_chartConfigs = this.buildChartConfigs(this.dnsKpis_chartdata);
      });
    } else if (subtab === 'GIFW') {
      this.runSplunkNodeJob('GiFW', (rows) => {
        this.splunk_data = rows;
        this.gifwKpis_chartdata = this.formatChartData(rows);
        this.gifwKpis_chartConfigs = this.buildChartConfigs(this.gifwKpis_chartdata);
      });
    } else if (subtab === 'IMS') {
      this.runSplunkNodeJob('IMS', (rows) => {
        this.splunk_data = rows;
        this.imsKpis_chartdata = this.formatChartData(rows);
        this.imsKpis_chartConfigs = this.buildChartConfigs(this.imsKpis_chartdata);
      });
    } else if (subtab === 'MSC') {
      // FIXED BUG: this branch originally checked `subtab === 'IMS'` again
      // (a duplicate of the branch above), so MSC could never be reached.
      this.runSplunkNodeJob('MSC', (rows) => {
        this.splunk_data = rows;
        this.mscKpis_chartdata = this.formatChartData(rows);
        this.mscKpis_chartConfigs = this.buildChartConfigs(this.mscKpis_chartdata);
      });
    } else if (subtab === 'MME') {
      this.runSplunkNodeJob('MME', (rows) => {
        if (rows.length === 0) {
          console.log('No MME data available for the selected parameters.');
          return;
        }
        this.splunk_data = rows;
        this.mmeKpis_chartdata = this.formatChartData(rows);
        this.mmeKpis_chartConfigs = this.buildChartConfigs(this.mmeKpis_chartdata);
      });
    } else if (subtab === 'SBG') {
      this.runSplunkNodeJob('SBG', (rows) => {
        this.splunk_data = rows;
        this.sbgKpis_chartdata = this.formatChartData(rows);
        this.sbgKpis_chartConfigs = this.buildChartConfigs(this.sbgKpis_chartdata);
      });
    } else if (subtab === 'BGF') {
      this.runSplunkNodeJob('BGF', (rows) => {
        this.splunk_data = rows;
        this.bgfKpis_chartdata = this.formatChartData(rows);
        this.bgfKpis_chartConfigs = this.buildChartConfigs(this.bgfKpis_chartdata);
      });
    } else if (subtab === 'Transmission') {
      this.apiService
        .runViaviJob({
          start_date: this.start_date,
          end_date: this.end_date,
          suspected_sites: this.suspected_sites,
        })
        .subscribe({
          next: (job: JobStatus) => {
            if (job.status === 'DONE' && job.result?.viavi) {
              this.viavi_data = JSON.parse(job.result.viavi);
              this.viavi_chartdata = this.formatChartData5(this.viavi_data);
              this.viavi_chartConfigs = this.buildChartConfigs(this.viavi_chartdata);
            } else if (job.status === 'FAILED') {
              console.error('Viavi job failed:', job.error);
            }
          },
          error: (err) => console.error('Viavi request error:', err),
        });
    }
  }

  /** Shared runner for every Splunk-node tab -- replaces 7 near-identical copy-pasted blocks. */
  private runSplunkNodeJob(node: string, onRows: (rows: any[]) => void) {
    this.apiService
      .runSplunkJob({ start_date: this.start_date, end_date: this.end_date, node })
      .subscribe({
        next: (job: JobStatus) => {
          if (job.status === 'DONE') {
            const rows = job.result?.raw_data ? JSON.parse(job.result.raw_data) : [];
            onRows(rows);
          } else if (job.status === 'FAILED') {
            console.error(`Splunk ${node} job failed:`, job.error);
          }
        },
        error: (err) => console.error(`Splunk ${node} request error:`, err),
      });
  }

  /** Shared runner for both NPP tabs -- replaces 2 near-identical copy-pasted blocks. */
  private runNppJob(hourly: boolean, onRows: (rows: any[]) => void) {
    const payload = {
      start_date: this.start_date,
      end_date: this.end_date,
      suspected_cells_raw: this.suspected_cells_raw,
    };
    const job$ = hourly ? this.apiService.runNppHourlyJob(payload) : this.apiService.runNppMinutesJob(payload);

    job$.subscribe({
      next: (job: JobStatus) => {
        if (job.status === 'DONE') {
          const rows = job.result?.raw_data ? JSON.parse(job.result.raw_data) : [];
          onRows(rows);
        } else if (job.status === 'FAILED') {
          console.error(`NPP ${hourly ? 'hourly' : '15min'} job failed:`, job.error);
        }
      },
      error: (err) => console.error('NPP request error:', err),
    });
  }

  applyTicketsSearchFilter(event: Event) {
    const filterValue = (event.target as HTMLInputElement).value;
    this.dataSource.filter = filterValue.trim().toLowerCase();
  }
}