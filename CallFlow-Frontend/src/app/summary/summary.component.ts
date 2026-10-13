import { ChangeDetectorRef, Component, OnInit, OnDestroy } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { ApiService } from '../services/api.service';
import { JobStatus } from '../services/job-polling.service';
import { Subscription } from 'rxjs';
import {
  OrchestrationService, OrchestratedJob, SystemStatus, RcaReadiness, DashboardValues,
  SummaryCards, SummaryCard,
} from '../services/orchestration.service';

// notAvailable: the card's data source does not exist in this context (NCI mode).
type CardState = 'loading' | 'loaded' | 'unavailable' | 'notAvailable';
interface AbnormalJobState {
  status: 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED' | 'NOT_AVAILABLE';
  step: string;
  progress: number;
  error: string | null;
}

interface SystemRow {
  key: string;
  label: string;
  status: SystemStatus;
}
@Component({
  selector: 'app-summary',
  templateUrl: './summary.component.html',
  styleUrl: './summary.component.css'
})
export class SummaryComponent implements OnInit, OnDestroy {
  // ── Orchestrated analysis lifecycle ────────────────────────────────
  request_id: string | null = null;
  // NCI context: Summary opened from an NCI portal_url (?mode=nci&sites=...).
  // Same page and components; NCI provides NPP / Viavi / Remedy only, so
  // everything else is shown in the "Not Available" state (never loading).
  nciMode = false;
  nciSites: string[] = [];
  nciError: string | null = null;
  private readonly NCI_UNAVAILABLE_SYSTEMS = ['trino', 'smartcare', 'splunk'];
  private readonly NCI_UNAVAILABLE_CARDS = [
    'totalComplaints', 'citiesSpanned', 'handsetTypes',
    'hubsAffected', 'tacsLacsAffected', 'packetCoreNodes',
  ];
  private readonly NCI_UNAVAILABLE_MODALS = ['cities', 'handsets', 'hubs', 'nodes', 'tac/lac'];
  modalNotAvailable = false;
  jobStatus: 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED' = 'PENDING';
  overallStatus: 'COMPLETED' | 'PARTIAL_SUCCESS' | 'FAILED' | null = null;
  processing = true;

  systemRows: SystemRow[] = [
    { key: 'trino', label: 'Cell information (Trino)', status: 'PENDING' },
    { key: 'smartcare', label: 'Smartcare', status: 'PENDING' },
    { key: 'npp', label: 'NPP', status: 'PENDING' },
    { key: 'splunk', label: 'Splunk', status: 'PENDING' },
    { key: 'viavi', label: 'Viavi (Transmission)', status: 'PENDING' },
    { key: 'remedy', label: 'Remedy', status: 'PENDING' },
  ];

  rcaReadiness: RcaReadiness = 'NOT_STARTED';
  rcaReady = false;                 // true only when files are actually READY
  rcaFilesUrl: string | null = null;
  rcaFileStatuses: { [system: string]: string } = {};

  // Live dashboard values pushed by the backend as each system finishes.
  dashboard: DashboardValues = {};
  // Per-card progressive readiness computed by the backend (one source of truth
  // with RCA). Drives the cards below independently of overall job completion.
  summaryCards: SummaryCards = {};
  // Dashboard card key -> backend summary card key (only cards that have an
  // explicit per-card readiness with a real EMPTY/PENDING/FAILED distinction).
  private readonly CARD_SUMMARY_KEY: { [key: string]: string } = {
    cellsAffected: 'affected_cells',
    remedyAffected: 'tickets',
  };
  // Explicit per-system analysis states (SUCCESS_ALL_CLEAN / NO_DATA / etc.).
  systemStates: { [system: string]: any } = {};
  // Which backend system each card's data depends on (null == available from input).
  private readonly CARD_SYSTEM: { [key: string]: string | null } = {
    totalComplaints: null,
    cellsAffected: 'npp',
    citiesSpanned: 'npp',
    handsetTypes: 'smartcare',
    remedyAffected: 'remedy',
    hubsAffected: 'npp',
    tacsLacsAffected: 'npp',
    packetCoreNodes: 'smartcare',
  };

  private jobSub?: Subscription;
  private rcaSub?: Subscription;

  // Active tab (Summary, Root Causes, or Correlations)
  activeTab: string = 'summary';

  msisdn_list: any;
  start_date: any;
  end_date: any;
  complain_type: string = 'Data';
  // Modal control
  showModal: boolean = false;
  modalTitle: string = '';
  modalData: any[] = [];
  horizontal_option: any;

  showTacLac = false;
  show_Packet_Nodes = false;
  show_Remedy_Details_btn =false;
  showRemedyTableModal: boolean = false;
  remedyTableData: any[] = [];
  modalDataTac: any[] = [];
  modalDataLac: any[] = [];

  modalColor: string = '#3b82f6';
    modalChartReady: boolean = false;  // New flag to control chart rendering
  modalNoData: boolean = false;        // true -> show "No detailed data available"
  showCellDetails: boolean = false;    // cells popup: show the per-cell KPI table
  cellDetailRows: any[] = [];          // [{cell, kpi, current, previous, deviation, severity, message}]


  // Data received from the form page
  receivedData: any = null;
  formData: any;
  smartcare_data_metrics: any;
  npp_data: any;

  topcell : any
  tophandset : any

  Total_Complaints: number;
  customer_per_cell: any;

  Select_Abnormal_System: any = 'Splunk'
  // True once the user picks a system in the Abnormal KPIs dropdown; until then
  // the dropdown follows the first system whose analysis is ready.
  private abnormalSystemUserPicked = false;
  Abnormal_Kpis_Data: any
  Abnormal_Kpis_chart_Loading = false
  No_Abnormal_Data_Message: string = '';
 
  // --- Progress bar values 

  systemStatus = {
  npp: 'loading',     // loading | success | failed
  splunk: 'loading',
  viavi: 'loading'
};
  abnormalJobs: { [key: string]: AbnormalJobState } = {
  npp: {
    status: 'PENDING',
    step: '',
    progress: 0,
    error: null
  },
  splunk: {
    status: 'PENDING',
    step: '',
    progress: 0,
    error: null
  },
  viavi: {
    status: 'PENDING',
    step: '',
    progress: 0,
    error: null
  }
};
  
  // ------------------
  Select_Packet_Node: any = 'GGSN'

  RootCause_Payload: any;

   Abnormal_Kpis_RawData: any;
  private Packet_Nodes_Data: any;

  // Statistics for the cards
  // In a real app, this would come from an API based on form data
  stats = {
    totalComplaints: 156,
    CellsAffected: 2,
    total_cells: 45,
    citiesSpanned: 3,
    handsetTypes: 2,
    hubsAffected: 4,
    tacsLacsAffected: 4,
    packetCoreNodes: 7,
    remedyAffected: 1,
  };

  // Root causes data for donut charts
  rootCauses = [
    {
      title: '1.Cause1',
      percentage: 49,
      centerText: 'Impacted',  // Shorter text for better display
      centerValue: '77',
      color: '#5E2750',
      cellCount: '4 cells',
      details: []
    },
    {
      title: '2.Cause2',
      percentage: 40,  // 62/156 = ~40%
      centerText: 'Impacted',
      centerValue: '62',
      color: '#00697C',
      cellCount: '7 cells',
      details: []
    },
    {
      title: '3.Cause3',
      percentage: 28,  // 43/156 = ~28%
      centerText: 'Impacted',
      centerValue: '43',
      color: '#EB9700',
      cellCount: '4 cells',
      details: []
    }
  ];

  Abnormal_System = [
    { label: 'Splunk', value: 'Splunk' },
    { label: 'NPP', value: 'NPP' },
    { label: 'Transmission', value: 'Viavi' },
  ];

  Packt_Nodes = [
    { label: 'GGSN', value: 'GGSN' },
    { label: 'DSC', value: 'DSC' },
    { label: 'HSS', value: 'HSS' },
    { label: 'MME', value: 'MME' },
    { label: 'OCC', value: 'OCC' },
    { label: 'PCRF', value: 'PCRF' },
    { label: 'SGSN', value: 'SGSN' },
  ];
  // KPI data for bar chart
  kpiData = [
    { label: 'KPI Name', value: 120, variance: 'high' },
    { label: 'KPI Name', value: 100, variance: 'medium' },
    { label: 'KPI Name', value: 120, variance: 'high' },
    { label: 'KPI Name', value: 80, variance: 'low' },
    { label: 'KPI Name', value: 100, variance: 'medium' },

  ];

  constructor(
    private router: Router,
    private apiService: ApiService,
    private orchestration: OrchestrationService,
    private cdr: ChangeDetectorRef,
    private route?: ActivatedRoute,
  ) {}

  ngOnInit() {
    console.log("summary component loaded")
    const nciSites = this.readNciContext();
    if (nciSites !== null) {
      this.enterNciMode(nciSites);
      return;
    }
    const state = history.state || {};
    this.request_id = state.request_id || null;
    this.Total_Complaints = state.Total_Complaints;
    this.msisdn_list = state.msisdn_list;
    this.start_date = state.start_date;
    this.end_date = state.end_date;
    this.complain_type = state.payload?.Complain_Type || state.Complain_Type || 'Data';
    if (this.Total_Complaints != null) {
      // Known immediately from the input -> seed so the card isn't stuck loading.
      this.dashboard = { ...this.dashboard, totalComplaints: this.Total_Complaints };
    }

    // RCA must open the files for THIS request only (never another request's).
    this.RootCause_Payload = {
      ...(state.payload || {}),
      request_id: this.request_id,
      start_date: this.start_date,
      end_date: this.end_date,
    };

    if (!this.request_id) {
      // No job to track (e.g. deep-linked). Leave the page idle rather than crash.
      this.processing = false;
      this.Abnormal_Kpis_chart_Loading = false;
      return;
    }

    this.startOrchestration(this.request_id);
  }

  ngOnDestroy() {
    // section 20: stop polling / unsubscribe / no leaks when leaving the page.
    this.jobSub?.unsubscribe();
    this.rcaSub?.unsubscribe();
  }

  /** NCI portal_url query: mode, sites and (from the NCI API) nci_id. */
  private nciQuery(): { mode: string | null; sites: string | null; nciId: string | null } | null {
    // Route query params first: correct even when the app navigates here in
    // code (e.g. back from login), before the browser address bar is updated.
    let get: (k: string) => string | null;
    const qp = this.route?.snapshot?.queryParamMap;
    if (qp) {
      get = (k) => qp.get(k);
    } else {
      try {
        const params = new URLSearchParams(window.location.search);
        get = (k) => params.get(k);
      } catch {
        return null;
      }
    }
    let mode = get('mode');
    let sites = get('sites');
    let nciId = get('nci_id');
    // A portal_url copied from escaped JSON (e.g. PowerShell ConvertTo-Json)
    // carries "\u0026" instead of "&": mode = "nci\u0026nci_id=..\u0026sites=..".
    if (mode && /\\u0026|%5Cu0026/i.test(mode)) {
      const inner = new URLSearchParams('mode=' + mode.replace(/\\u0026|%5Cu0026/gi, '&'));
      mode = inner.get('mode');
      sites = sites || inner.get('sites');
      nciId = nciId || inner.get('nci_id');
    }
    return { mode, sites, nciId };
  }

  /** NCI portal_url -> the sites, or null when not in NCI mode. */
  private readNciContext(): string[] | null {
    const q = this.nciQuery();
    if (!q || (q.mode || '').toLowerCase() !== 'nci') return null;
    return (q.sites || '').split(',').map((s) => s.trim()).filter(Boolean);
  }

  /** Id of the NCI API result the link points to (shown as-is, no re-run). */
  private readNciResultId(): string | null {
    return this.nciQuery()?.nciId || null;
  }

  /** Same Summary page; systems NCI does not provide start as NOT_AVAILABLE
   * (the backend never reports them, so they stay that way). */
  private enterNciMode(sites: string[]) {
    this.nciMode = true;
    this.nciSites = sites;
    // Link from the NCI API: show THAT result (stored by the API), no new run.
    const resultId = this.readNciResultId();
    if (resultId) {
      this.setNciSystems('PROCESSING');
      this.request_id = resultId;
      this.startOrchestration(resultId, this.orchestration.nciJobsUrl);
      return;
    }
    // NCI analyses NPP / Viavi / Remedy (PROCESSING from the start); the rest
    // are NOT_AVAILABLE and never change, since the NCI job does not report them.
    this.setNciSystems(sites.length ? 'PROCESSING' : 'FAILED');
    if (!sites.length) {
      this.processing = false;
      this.Abnormal_Kpis_chart_Loading = false;
      this.overallStatus = 'FAILED';
      return;
    }
    this.processing = true;
    this.orchestration.submitNci(sites).subscribe({
      next: (res) => {
        this.request_id = res.job_id;
        this.startOrchestration(res.job_id, this.orchestration.nciJobsUrl);
      },
      error: () => {
        this.setNciSystems('FAILED');
        this.processing = false;
        this.Abnormal_Kpis_chart_Loading = false;
        this.overallStatus = 'FAILED';
      },
    });
  }

  /** NCI job failed / result not found: no system may stay PROCESSING. */
  private failNciSystems(error?: string | null) {
    this.nciError = error || 'NCI analysis failed';
    this.systemRows = this.systemRows.map((r) =>
      r.status === 'PROCESSING' || r.status === 'PENDING' ? { ...r, status: 'FAILED' as SystemStatus } : r);
    this.updateAbnormalJobs();
  }

  private setNciSystems(available: SystemStatus) {
    this.systemRows = this.systemRows.map((r) => ({
      ...r,
      status: this.NCI_UNAVAILABLE_SYSTEMS.includes(r.key) ? ('NOT_AVAILABLE' as SystemStatus) : available,
    }));
    this.updateAbnormalJobs();
  }

  /** Track the data-availability job: per-system progress, then RCA readiness. */
  private startOrchestration(requestId: string, jobsUrl?: string) {
    this.processing = true;
    this.Abnormal_Kpis_chart_Loading = true;

    this.jobSub = this.orchestration.pollJob(requestId, jobsUrl).subscribe({
      next: (job) => this.applyJob(job),
      error: (err) => {
        if (this.nciMode) {
          this.failNciSystems(err?.status === 404
            ? 'NCI result not found or expired' : (err?.error?.error || null));
        }
        this.processing = false;
        this.Abnormal_Kpis_chart_Loading = false;
        this.overallStatus = 'FAILED';
      },
    });
  }

  private applyJob(job: OrchestratedJob) {
    this.jobStatus = job.status;

    // Update per-system rows from real backend status (never faked).
    const systems = job.systems || job.result?.systems;
    if (systems) {
      this.systemRows = this.systemRows.map((row) => ({
        ...row,
        status: (systems[row.key] as SystemStatus) || row.status,
      }));
    }

    // Merge live dashboard values as systems finish (independent per card).
    const dash = job.dashboard || job.result?.dashboard;
    if (dash) {
      this.dashboard = { ...this.dashboard, ...dash };
    }

    // Per-card progressive readiness (backend, one source of truth with RCA).
    const summary = job.summary || job.result?.summary;
    if (summary) {
      this.summaryCards = summary;
    }

    // Consume detailed structures (popups + abnormal chart) as they stream in.
    this.applyDetails(job);

    if (job.rca) {
      this.rcaReadiness = job.rca.status;
      this.rcaFileStatuses = job.rca.files || {};
    }

    if (job.status === 'DONE' || job.status === 'FAILED') {
      this.processing = false;
      this.Abnormal_Kpis_chart_Loading = false;
      this.overallStatus = job.result?.overall_status || (job.status === 'DONE' ? 'COMPLETED' : 'FAILED');
      this.hydrateDashboardFromResult(job);
      if (this.nciMode && job.status === 'FAILED') this.failNciSystems((job as any).error);
      // Now confirm RCA file readiness on its own lifecycle (section 15/18).
      // NCI jobs produce no RCA files: RCA is not applicable there.
      if (!this.nciMode) {
        this.trackRcaReadiness(job.result?.request_id || this.request_id!);
      }
    }
    this.cdr.detectChanges();
  }

  /** Poll RCA readiness until files are actually READY (no manual refresh). */
  private trackRcaReadiness(requestId: string) {
    this.rcaSub?.unsubscribe();
    this.rcaSub = this.orchestration.pollRcaStatus(requestId).subscribe({
      next: (rca) => {
        this.rcaReadiness = rca.status;
        this.rcaFileStatuses = rca.files || {};
        this.rcaReady = rca.status === 'READY' || rca.status === 'PARTIAL';
        this.rcaFilesUrl = rca.rca_files_url || this.orchestration.rcaFilesUrl(requestId);
        if (this.rcaReady) {
          // Give the RCA iframe the request-scoped file URL so it loads the
          // right request's files, only once they exist.
          this.RootCause_Payload = {
            ...this.RootCause_Payload,
            request_id: requestId,
            rca_files_url: this.rcaFilesUrl,
          };
        }
        this.cdr.detectChanges();
      },
      error: () => {
        this.rcaReadiness = 'FAILED';
        this.rcaReady = false;
      },
    });
  }

  /** Map a per-system orchestration status to the abnormal-panel status. */
  private abnormalSystemStatus(system: string): string {
    const s = this.systemStatusOf(system);
    if (s === 'NOT_AVAILABLE') return 'notAvailable';
    if (s === 'COMPLETED') return 'success';
    if (s === 'FAILED' || s === 'PARTIAL') return 'failed';
    return 'loading';
  }

  /** Consume the detailed structures the backend streams (npp_data,
   * smartcare_data_metrics, abnormal arrays) so popups and the abnormal chart
   * get REAL data instead of empty defaults. Runs on every poll so details
   * appear as each system finishes. */
  private applyDetails(job: OrchestratedJob) {
    const d = job.details || job.result?.details;

    // Abnormal-panel system statuses come from the real per-system state.
    this.systemStatus = {
      npp: this.abnormalSystemStatus('npp'),
      splunk: this.abnormalSystemStatus('splunk'),
      viavi: this.abnormalSystemStatus('viavi'),
    };

    if (d) {
      if (d.smartcare_data_metrics) {
        this.smartcare_data_metrics = d.smartcare_data_metrics;
        const metrics = d.smartcare_data_metrics.metrics || {};
        this.customer_per_cell = metrics.subs_per_cell || this.customer_per_cell || [];
        if (d.smartcare_data_metrics.packetNodes) {
          this.Packet_Nodes_Data = d.smartcare_data_metrics.packetNodes.GGSN;
        }
      }
      if (d.npp_data) {
        this.npp_data = d.npp_data;
        this.topcell = this.getTopCellName(this.npp_data.affected_cells_details);
      }
      if (d.abnormal) {
        this.Abnormal_Kpis_RawData = {
          npp: d.abnormal.npp || [],
          splunk: d.abnormal.splunk || [],
          viavi: d.abnormal.viavi || [],
        };
      }
      if (d.states) {
        this.systemStates = d.states;
      }
    }
    // Per-system Abnormal KPI status cards, each driven by its OWN analysis
    // status (never by the overall job status).
    this.updateAbnormalJobs();
    this.autoSelectReadyAbnormalSystem();
    this.updateAbnormalChart();
    this.logAbnormalDiagnostics();
  }

  /** Map the real per-system status onto the Abnormal KPI status cards. */
  private updateAbnormalJobs() {
    const labels: { [k: string]: string } = { npp: 'NPP', splunk: 'Splunk', viavi: 'Viavi' };
    for (const sys of ['npp', 'splunk', 'viavi']) {
      const s = this.systemStatusOf(sys);
      const count = (this.Abnormal_Kpis_RawData?.[sys] || []).length;
      if (s === 'NOT_AVAILABLE') {
        this.abnormalJobs[sys] = { status: 'NOT_AVAILABLE', step: 'Not Available', progress: 0, error: null };
      } else if (s === 'COMPLETED') {
        this.abnormalJobs[sys] = { status: 'DONE', step: `${count} abnormal KPI result(s)`, progress: 100, error: null };
      } else if (s === 'FAILED' || s === 'PARTIAL') {
        const row = this.systemRows.find((r) => r.key === sys);
        this.abnormalJobs[sys] = {
          status: 'FAILED', step: '', progress: 100,
          error: (row && this.systemMessage(row)) || `${labels[sys]} analysis failed.`,
        };
      } else if (s === 'PROCESSING') {
        this.abnormalJobs[sys] = { status: 'RUNNING', step: `${labels[sys]} analysis in progress...`, progress: 50, error: null };
      } else {
        this.abnormalJobs[sys] = { status: 'PENDING', step: `Waiting for ${labels[sys]} analysis...`, progress: 0, error: null };
      }
    }
  }

  /** True while at least one abnormal-KPI system has not finished. */
  get abnormalAnyPending(): boolean {
    return ['npp', 'splunk', 'viavi'].some((s) => {
      const st = this.abnormalJobs[s]?.status;
      return st !== 'DONE' && st !== 'FAILED' && st !== 'NOT_AVAILABLE';
    });
  }

  /** Until the user picks a system, show the first one whose analysis is ready.
   * If several are ready at once, any of them is fine (NPP, Viavi, Splunk order). */
  private autoSelectReadyAbnormalSystem() {
    if (this.abnormalSystemUserPicked) return;
    const current = String(this.Select_Abnormal_System || '').toLowerCase();
    if ((this.systemStatus as any)[current] === 'success') return;   // already showing a ready one
    const values: { [k: string]: string } = { npp: 'NPP', viavi: 'Viavi', splunk: 'Splunk' };
    const ready = ['npp', 'viavi', 'splunk'].find((s) => (this.systemStatus as any)[s] === 'success');
    if (ready) this.Select_Abnormal_System = values[ready];
  }

  /** Dropdown change: the user's choice wins from now on. */
  onAbnormalSystemChange() {
    this.abnormalSystemUserPicked = true;
    this.updateAbnormalChart();
  }

  /** Compact per-poll diagnostics: where abnormal KPI data is (or stops). */
  private logAbnormalDiagnostics() {
    const parts = ['npp', 'viavi', 'splunk'].map((s) =>
      `${s}: status=${this.systemStatusOf(s)} received=${(this.Abnormal_Kpis_RawData?.[s] || []).length} ` +
      `card=${this.abnormalJobs[s]?.status}`);
    console.debug(`[ABNORMAL] request_id=${this.request_id} selected=${this.Select_Abnormal_System} | ${parts.join(' | ')}`);
  }

  /** Retained name for the terminal call; now delegates to applyDetails. */
  private hydrateDashboardFromResult(job: OrchestratedJob) {
    this.applyDetails(job);
  }

  /** Human-readable, system-specific failure reason (section 10). */
  systemMessage(row: SystemRow): string {
    if (row.status !== 'FAILED' && row.status !== 'PARTIAL') return '';
    const partial = row.status === 'PARTIAL';
    switch (row.key) {
      case 'trino':
        return 'Unable to retrieve cell information. Some dependent checks could not be completed.';
      case 'smartcare':
        return 'Smartcare data could not be retrieved. Some analysis results are unavailable.';
      case 'npp':
        return partial ? 'NPP data retrieved; analysis unavailable (Smartcare missing).' : 'NPP processing failed.';
      case 'viavi':
        return partial ? 'Viavi data retrieved; analysis unavailable (Smartcare missing).' : 'Viavi processing failed.';
      case 'remedy':
        return partial ? 'Remedy data retrieved; analysis unavailable (Smartcare missing).' : 'Remedy processing failed.';
      case 'splunk':
        return partial ? 'Splunk data retrieved; analysis unavailable (Smartcare missing).' : 'Splunk processing failed.';
      default:
        return 'Processing failed.';
    }
  }

  // ── Dashboard card state (data-availability driven, no dummy values) ──
  private systemStatusOf(system: string): SystemStatus {
    const row = this.systemRows.find((r) => r.key === system);
    return (row?.status || 'PENDING') as SystemStatus;
  }

  /** Backend per-card readiness for a dashboard card, if one exists. */
  summaryCardFor(key: string): SummaryCard | undefined {
    const scKey = this.CARD_SUMMARY_KEY[key];
    return scKey ? this.summaryCards?.[scKey] : undefined;
  }

  cardState(key: string): CardState {
    if (this.nciMode && this.NCI_UNAVAILABLE_CARDS.includes(key)) return 'notAvailable';
    // Prefer the backend's explicit per-card readiness where available: it
    // distinguishes a REAL empty result (0) from "still pending" and "failed",
    // and it is derived from the SAME analysis result RCA uses.
    const card = this.summaryCardFor(key);
    if (card) {
      switch (card.status) {
        case 'PENDING': return 'loading';       // spinner, never a 0
        case 'FAILED': return 'unavailable';
        case 'READY_WITH_DATA':
        case 'READY_EMPTY': return 'loaded';    // show the real value, incl. 0
      }
    }

    const value = this.dashboard ? this.dashboard[key] : undefined;
    const hasValue = value !== undefined && value !== null && value !== '';
    const system = this.CARD_SYSTEM[key] ?? null;

    // Cards not tied to a backend system (e.g. total complaints from input).
    if (system === null) {
      return hasValue ? 'loaded' : (this.request_id ? 'loading' : 'unavailable');
    }

    const status = this.systemStatusOf(system);
    if (status === 'COMPLETED') return hasValue ? 'loaded' : 'unavailable';
    if (status === 'FAILED' || status === 'PARTIAL') return 'unavailable';
    return 'loading'; // PENDING / PROCESSING
  }

  cardLoading(key: string): boolean { return this.cardState(key) === 'loading'; }
  cardUnavailable(key: string): boolean { return this.cardState(key) === 'unavailable'; }

  cardValue(key: string): string | number {
    if (this.cardState(key) === 'notAvailable') return 'Not Available';
    const v = this.dashboard ? this.dashboard[key] : undefined;
    if (v !== undefined && v !== null) return v;
    // Fall back to the backend per-card value so a real 0 (READY_EMPTY) shows.
    const card = this.summaryCardFor(key);
    if (card && (card.status === 'READY_WITH_DATA' || card.status === 'READY_EMPTY')
        && card.value !== undefined && card.value !== null) {
      return card.value;
    }
    return '';
  }

  get cellsAffectedText(): string {
    const card = this.summaryCardFor('cellsAffected');
    // Affected count: prefer the backend per-card value (real 0 when clean).
    let a = this.dashboard?.['cellsAffected'];
    if ((a === undefined || a === null) && card &&
        (card.status === 'READY_WITH_DATA' || card.status === 'READY_EMPTY')) {
      a = card.value ?? undefined;
    }
    const t = this.dashboard?.['totalCells'];
    return `${a ?? '—'}/${t ?? '—'}`;
  }

  get rcaMessage(): string {
    if (this.nciMode) return 'Not Available';   // NCI produces no RCA files
    switch (this.rcaReadiness) {
      case 'READY': return 'RCA ready';
      case 'PARTIAL': return 'RCA ready (some system files are unavailable)';
      case 'GENERATING': return 'Preparing RCA files...';
      case 'FAILED': return 'RCA unavailable — required files could not be generated.';
      default: return 'Waiting for analysis...';
    }
  }

  /** Text shown next to a system row. */
  statusText(status: SystemStatus): string {
    return status === 'NOT_AVAILABLE' ? 'NOT AVAILABLE' : status;
  }

  openModal(cardType: string) {

    this.modalData = []
    this.modalDataTac = []
    this.modalDataLac = []
    this.showTacLac = false
    this.showCellDetails = false
    this.cellDetailRows = []
    this.modalNotAvailable = this.nciMode && this.NCI_UNAVAILABLE_MODALS.includes(cardType);
    this.showModal = true;

    // All accessors are guarded: the popup shows real data when present, else a
    // clean "No detailed data available" state (never a fake/empty chart).
    switch (cardType) {
      case 'cells':
        this.modalTitle = 'Cells Affected - Detailed Breakdown';
        this.modalColor = '#0891b2';
        this.horizontal_option = false
        this.modalData = Object.keys(this.npp_data?.affected_cells_details || {}).map(cellName => {
          const cell = this.npp_data.affected_cells_details[cellName] || {};
          let tooltipText = '';
          tooltipText += `
            <div style="margin-bottom: 8px;">
              <strong> Site OR Cell : </strong>
              ${cell.site_name}<br/>
            </div>
          `;
          Object.keys(cell.kpis || {}).forEach(kpiName => {
            const kpi = cell.kpis[kpiName];
            tooltipText += `
      <div style="margin-bottom: 8px;">
        <strong>${kpiName}:</strong><br/>
        Current_Value: ${kpi.current_value}<br/>
        Previous_Value: ${kpi.previous_value}<br/>
        Deviation: ${kpi.deviation_pct}% (${kpi.deviation_direction})
      </div>
    `;
          });

          return {
            label: cellName,
            // Same per-cell count the details table and abnormal chart use
            // (backend impacted_customers), so all three always agree.
            // NCI has no customer data: plot the number of degraded KPIs.
            value: this.nciMode ? Object.keys(cell.kpis || {}).length : (cell.impacted_customers ?? 0),
            tooltip: tooltipText,
          };
        }).sort((a, b) => b.value - a.value).slice(0, 10);
        // Per-cell KPI details table (real values/insights from affected_cells_details).
        this.showCellDetails = true;
        this.cellDetailRows = this.buildCellDetailRows();
        break;

      case 'cities':
        this.modalTitle = 'Cities Spanned - Distribution';
        this.modalColor = '#0891b2';
        this.horizontal_option = true
        this.modalData = this.npp_data?.affected_metrics?.subs_per_region || [];
        break;

      case 'handsets':
        this.modalTitle = 'Handset Types - Breakdown';
        this.modalColor = '#dc2626';
        this.horizontal_option = true
        this.modalData = this.smartcare_data_metrics?.metrics?.subs_per_handset_type || [];
        break;

      case 'hubs':
        this.modalTitle = 'Hubs - Breakdown';
        this.modalColor = '#dc2626';
        this.horizontal_option = true
        this.modalData = this.npp_data?.affected_metrics?.subs_per_hub || [];
        break;

      case 'nodes':
        this.modalTitle = 'Packet core Nodes';
        this.modalColor = '#dc2626';
        this.horizontal_option = true
        this.show_Packet_Nodes = true
        this.Select_Packet_Node = 'GGSN'
        this.modalData = this.smartcare_data_metrics?.packetNodes?.GGSN || [];
        break;
      case 'tac/lac':
        this.modalTitle = 'TAC-LAC - Breakdown';
        this.modalColor = '#dc2626';
        this.showTacLac = true;
        this.horizontal_option = true
        this.modalDataTac = this.npp_data?.affected_metrics?.subs_per_tac || [];
        this.modalDataLac = this.npp_data?.affected_metrics?.subs_per_lac || [];
        break;
      case 'remedy':
        this.modalTitle = 'Remedy Tickets';
        this.modalColor = '#dc2626';
        this.horizontal_option = true
        this.show_Remedy_Details_btn = true
        this.modalData = this.smartcare_data_metrics?.remedy
          ? this.prepareRemedyChartData(this.smartcare_data_metrics.remedy)
          : [];
        break;
      default:
        this.modalTitle = 'Detailed Information';
        this.modalColor = '#3b82f6';
        this.modalData = [];
    }

    // Empty detail data -> explicit "No detailed data available" (no fake chart).
    if (this.showTacLac) {
      this.modalNoData =
        (!this.modalDataTac || this.modalDataTac.length === 0) &&
        (!this.modalDataLac || this.modalDataLac.length === 0);
    } else if (this.showCellDetails) {
      this.modalNoData =
        (!this.modalData || this.modalData.length === 0) &&
        (!this.cellDetailRows || this.cellDetailRows.length === 0);
    } else {
      this.modalNoData = !this.modalData ||
        (Array.isArray(this.modalData) && this.modalData.length === 0);
    }
    if (this.modalNotAvailable) {
      // NCI mode: this card's data source does not exist -> "Not Available".
      this.modalData = [];
      this.modalDataTac = [];
      this.modalDataLac = [];
      this.modalNoData = true;
    }

    this.showModal = true;
  }

  /** Flatten affected_cells_details into per-KPI rows for the Cells popup table.
   * Uses only data already produced by the NPP analysis (no invented values). */
  buildCellDetailRows(): any[] {
    const details = this.npp_data?.affected_cells_details || {};
    const rows: any[] = [];
    Object.keys(details).forEach((cellName) => {
      const cell = details[cellName] || {};
      const kpis = cell.kpis || {};
      // Customer count per affected cell = backend impacted_customers (unique
      // complainant MSISDNs per cell, from the Trino cell rows).
      // Null/undefined -> shown as N/A (never a fake 0).
      const customers = (cell.impacted_customers === undefined || cell.impacted_customers === null)
        ? 'N/A' : cell.impacted_customers;
      Object.keys(kpis).forEach((kpiName) => {
        const k = kpis[kpiName] || {};
        rows.push({
          cell: cellName,
          site: cell.site_name || '',
          kpi: kpiName,
          current: k.current_value,
          previous: k.previous_value,
          deviation: k.deviation_pct,
          severity: k.severity,
          customers: customers,
          message: k.message || k.insight_message || '',
        });
      });
    });
    return rows;
  }

  /**
   * Closes the modal
   */
  closeModal() {
    if (this.showTacLac) {
      this.showTacLac = false
    }
    this.showModal = false;
    this.show_Packet_Nodes = false
    this.show_Remedy_Details_btn= false
    this.showCellDetails = false
    this.cellDetailRows = []
  }
  closeRemedyTableModal(){
  this.showRemedyTableModal = false;
  this.show_Remedy_Details_btn= false

  }

  


// NOTE: The old Fetch_Abnormal_KPI()/handleAbnormalJob() flow (three separate
// Smartcare-first abnormal jobs) has been REPLACED by the single orchestrated
// job tracked in startOrchestration()/applyJob(). See orchestration.service.ts.

  updateAbnormalChart() {
      this.No_Abnormal_Data_Message = '';

    if (!this.Abnormal_Kpis_RawData || !this.Select_Abnormal_System) {
      this.Abnormal_Kpis_Data = [];
      return;
    }

    const systemKey = this.Select_Abnormal_System.toLowerCase();
    const systemData = this.Abnormal_Kpis_RawData[systemKey] || [];

 const status = this.systemStatus[systemKey]; 

  
  if (status === 'notAvailable') {
    this.Abnormal_Kpis_Data = [];
    this.No_Abnormal_Data_Message = 'Not Available';
    return;
  }

  if (status === 'failed') {
    this.Abnormal_Kpis_Data = [];
    this.No_Abnormal_Data_Message = '❌This system is currently unavailable';
    return;
  }

  
  if (status === 'success' && systemData.length === 0) {
    this.Abnormal_Kpis_Data = [];
    // Distinguish "analysis ran, all clean" from "no usable data" (never conflate
    // an empty abnormal list with 'analysis did not run').
    const st = this.systemStates[systemKey];
    if (st && (st.all_clean || st.state === 'SUCCESS_ALL_CLEAN')) {
      this.No_Abnormal_Data_Message =
        `✓ Analysis completed — all ${st.entities_analyzed ?? ''} analyzed ` +
        `entities are within the configured thresholds. No abnormal KPIs detected.`;
    } else if (st && st.state === 'NO_DATA') {
      this.No_Abnormal_Data_Message =
        'Analysis ran but returned no usable data for this system.';
    } else {
      this.No_Abnormal_Data_Message = 'There is no abnormal KPI in this system';
    }
    return;
  }

 
  if (status === 'loading') {
    this.Abnormal_Kpis_Data = [];
    return;
  }
    if (systemKey === 'npp') {
      this.Abnormal_Kpis_Data = this.prepare_Npp_ChartData(systemData);
    } else if (systemKey === 'splunk') {
      this.Abnormal_Kpis_Data = this.prepareSplunkChartData(systemData);
    } else if (systemKey === 'viavi') {
      this.Abnormal_Kpis_Data = this.prepare_Viavi_ChartData(systemData);
    } else {
      this.Abnormal_Kpis_Data = [];
    }

    console.log('Prepared chart data:', this.Abnormal_Kpis_Data);
  }

  update_packct_nodes() {
    this.Packet_Nodes_Data = this.smartcare_data_metrics.packetNodes

    if (!this.Packet_Nodes_Data || !this.Select_Packet_Node) {
      this.Packet_Nodes_Data = [];
      return;
    }

    const systemKey = this.Select_Packet_Node

    if (systemKey === 'GGSN') {
      this.modalData = this.Packet_Nodes_Data.GGSN
    } else if (systemKey === 'DSC') {
      this.modalData = this.Packet_Nodes_Data.DSC
    } else if (systemKey === 'HSS') {
      this.modalData = this.Packet_Nodes_Data.HSS
    }
    else if (systemKey === 'MME') {
      this.modalData = this.Packet_Nodes_Data.MME
    }
    else if (systemKey === 'OCC') {
      this.modalData = this.Packet_Nodes_Data.OCC
    }
    else if (systemKey === 'PCRF') {
      this.modalData = this.Packet_Nodes_Data.PCRF
    }
    else if (systemKey === 'SGSN') {
      this.modalData = this.Packet_Nodes_Data.SGSN
    }

    else {
      this.Packet_Nodes_Data = [];
    }

    console.log('Prepared chart data:', this.Packet_Nodes_Data);
  }
  /** Bar value: impacted customers normally; degraded-KPI count in NCI mode
   * (NCI analyses sites, not customers). */
  private abnormalValue(item: any): number {
    return this.nciMode ? (item.degraded_kpis || []).length : item.impacted_customers;
  }

  get abnormalYAxisLabel(): string {
    return this.nciMode ? 'No. of degraded KPIs' : 'No. of Customers affected';
  }

  prepare_Npp_ChartData(Data: any[]) {
    return Data.map(item => ({
      label: item.cell_name,
      value: this.abnormalValue(item),
      tooltipData: item.degraded_kpis || []
    })).slice(0,5);
  }
  prepare_Viavi_ChartData(Data: any[]) {
    return Data.map(item => ({
      label: item.site_name,
      value: this.abnormalValue(item),
      tooltipData: item.degraded_kpis || []
    })).slice(0,5);
  }
  prepareSplunkChartData(splunkData: any[]) {
    return splunkData.map(item => ({
      label: item.mapped_value,
      value: item.impacted_subs,
      tooltipData: item.degraded_kpis || []
      // tooltipData: [
      //   {
      //     name: item.kpi_column,
      //     current_value: item.degraded_kpis|| []
      //   }
      // ]
    })).slice(0,5);
  }
  prepareRemedyChartData(remedyResponse: any) {
    
    const remedyOutput = remedyResponse?.remedy_output;
    if (!remedyOutput) return [];

    return Object.entries(remedyOutput).map(

      ([ticketId, ticketData]: [string, any]) => {

        const statusEntries = Object.entries(ticketData.status || {});
        const [statusName, statusCount] = statusEntries[0] || ['Unknown', 0];

        return {
          label: ticketId,
          value: ticketData.subs_count,
          tooltipData: [
            {
              name: statusName,
              current_value: ticketData.subs_count
            }
          ]
        };
      }
    ).slice(0,15);

  }
  prepareRemedytableData(remedyResponse: any) {
  if (!remedyResponse?.remedy_output) {
      this.remedyTableData = [];
      return;
    }

    const output = remedyResponse.remedy_output;

    this.remedyTableData = Object.values(output)
      .flatMap((site: any) => site.tickets || []);
    console.log('Remedy table data')
    console.log(this.remedyTableData)
  }
  Remedy_Data_Details_Modal(){
  this.showModal = false;          
  this.showRemedyTableModal = true; 
  this.prepareRemedytableData(this.smartcare_data_metrics.remedy)
  }
  
  getTopCellName(cellsObj: any): string | null {

  if (!cellsObj || Object.keys(cellsObj).length === 0) return null;

  let topCellName = null;
  let maxSubs = -Infinity;

  for (const [cellName, cellData] of Object.entries(cellsObj)) {

    const subs = (cellData as any)?.subs_count ?? 0;

    if (subs > maxSubs) {
      maxSubs = subs;
      topCellName = cellName;
    }

  }

  return topCellName;
}
getTopHandset_Type(metricsArr: any[]): string | null {

  if (!metricsArr || metricsArr.length === 0) return null;

  let topItem = metricsArr[0];

  for (const item of metricsArr) {
    if ((item?.value ?? 0) > (topItem?.value ?? 0)) {
      topItem = item;
    }
  }

  return topItem?.label || null;
}

  Formate_Variable_Data() {
    this.smartcare_data_metrics = [],
      this.npp_data = [],
      this.stats.totalComplaints = 0,
      this.stats.CellsAffected = 0,
      this.stats.total_cells = 0,
      this.stats.handsetTypes = 0,
      this.stats.hubsAffected = 0,
      this.stats.tacsLacsAffected = 0
    this.stats.citiesSpanned = 0
  }

isDataLoaded(): boolean {
  // Root Causes tab is enabled ONLY when RCA files are actually ready
  // (section 18) -- never before.
  return this.rcaReady;
}
onRootCauseClick() {
  if (!this.rcaReady) {
    return; // tab stays disabled until RCA files are ready
  }
  this.setActiveTab('root-causes');
}
  /**
   * Changes the active tab
   * @param tab - 'summary', 'root-causes', or 'correlations'
   */
  setActiveTab(tab: string) {
    this.activeTab = tab;
    // if (tab === 'root-causes')
    //    {
    //       this.router.navigate(['/Root-Cause']);
    //    }
  }

  Navigate_Correlation_Page() {
    this.router.navigate(['/Correlation']);
  }
}