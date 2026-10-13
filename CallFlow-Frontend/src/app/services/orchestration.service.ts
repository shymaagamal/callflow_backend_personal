// src/app/services/orchestration.service.ts
/**
 * Client for the data-availability orchestration backend.
 *
 * Consumes the EXISTING backend orchestration (POST /callflow/api/analysis and
 * its job/RCA status endpoints). It does not implement any scheduling itself --
 * the backend scheduler is the single source of truth; Angular only tracks and
 * displays its status.
 *
 *   submit()        -> POST  /callflow/api/analysis            -> { job_id }
 *   pollJob()       -> GET   /callflow/api/analysis/{id}        (until terminal)
 *   pollRcaStatus() -> GET   /callflow/api/analysis/{id}/rca-status (until ready)
 *
 * Polling stops automatically on terminal states and unsubscribes cleanly when
 * the caller unsubscribes (e.g. component ngOnDestroy) -- see takeWhile(...,true).
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, timer } from 'rxjs';
import { switchMap, takeWhile } from 'rxjs/operators';

export type JobStatus = 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED';
// NOT_AVAILABLE: the system has no data source in this context (NCI mode).
export type SystemStatus = 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED' | 'PARTIAL' | 'NOT_AVAILABLE';
export type RcaReadiness = 'NOT_STARTED' | 'GENERATING' | 'READY' | 'PARTIAL' | 'FAILED';

export interface SystemsMap {
  [system: string]: SystemStatus;
}

export interface RcaStatus {
  request_id: string;
  status: RcaReadiness;
  files: { [system: string]: string };
  rca_files_url?: string;
}

export interface DashboardValues {
  [key: string]: number | string | null | undefined;
}

export interface AbnormalBuckets {
  npp: any[];
  splunk: any[];
  viavi: any[];
}

export interface SystemAnalysisState {
  state?: string;            // NOT_RUN | SUCCESS_ALL_CLEAN | SUCCESS_WITH_ABNORMAL_KPIS | NO_DATA | FAILED
  message?: string;
  analysis_run?: boolean;
  entities_analyzed?: number;
  affected_count?: number;
  has_abnormal_kpis?: boolean;
  all_clean?: boolean;
}

export interface OrchestrationDetails {
  smartcare_data_metrics?: any | null;   // metrics, packetNodes, remedy, suspected_cells_raw
  npp_data?: any | null;                 // affected_cells_details, affected_metrics, ...
  abnormal?: AbnormalBuckets;            // chart-ready arrays per system
  states?: { [system: string]: SystemAnalysisState };  // explicit analysis states
}

// Per-card progressive readiness, computed by the backend from each card's REAL
// backing analysis task (never the overall job). One source of truth with RCA.
export type CardReadiness = 'PENDING' | 'READY_WITH_DATA' | 'READY_EMPTY' | 'FAILED';

export interface SummaryCard {
  status: CardReadiness;
  value?: number | null;       // affected_cells / tickets / viavi / splunk
  count?: number;              // abnormal_kpis
  task?: string;
  task_status?: string;
  ready_systems?: string[];
  pending_systems?: string[];
  failed_systems?: string[];
}

export interface SummaryCards {
  affected_cells?: SummaryCard;
  tickets?: SummaryCard;
  viavi?: SummaryCard;
  splunk?: SummaryCard;
  smartcare?: SummaryCard;
  abnormal_kpis?: SummaryCard;
  [key: string]: SummaryCard | undefined;
}

export interface OrchestratedResult {
  request_id: string;
  overall_status: 'COMPLETED' | 'PARTIAL_SUCCESS' | 'FAILED';
  systems: SystemsMap;
  rca: RcaStatus;
  dashboard?: DashboardValues;
  details?: OrchestrationDetails;
  summary?: SummaryCards;
  analysis?: any;
}

export interface OrchestratedJob {
  job_id: string;
  status: JobStatus;
  step: string;
  progress_percent: number;
  systems?: SystemsMap;
  rca?: RcaStatus;
  dashboard?: DashboardValues;         // live, pushed as each system finishes
  details?: OrchestrationDetails;      // live detailed structures for popups/abnormal
  summary?: SummaryCards;              // live per-card progressive readiness
  result?: OrchestratedResult | null;
  error?: string | null;
}

export interface SubmitResponse {
  job_id: string;
  status: string;
}

@Injectable({ providedIn: 'root' })
export class OrchestrationService {
  // Matches ApiService's base host.
  private apiUrl = 'http://127.0.0.1:8057';
  // private apiUrl = 'https://10.30.30.177';

  private base = `${this.apiUrl}/callflow/api`;
  /** NCI portal jobs (Summary opened from an NCI portal_url). Same job contract. */
  readonly nciJobsUrl = `${this.apiUrl}/callflow/NCI/portal/analysis`;
  private readonly POLL_MS = 1500;

  constructor(private http: HttpClient) {}

  private get opts() {
    return { withCredentials: true, headers: { 'Content-Type': 'application/json' } };
  }

  /** POST NCI portal analysis for the given sites -> { job_id }. */
  submitNci(sites: string[]): Observable<SubmitResponse> {
    return this.http.post<SubmitResponse>(this.nciJobsUrl, { sites }, this.opts);
  }

  /** POST /analysis -> { job_id }. request_id === job_id. */
  submit(payload: any): Observable<SubmitResponse> {
    return this.http.post<SubmitResponse>(`${this.base}/analysis`, payload, this.opts);
  }

  /** Poll the job until it reaches a terminal state (DONE/FAILED). */
  pollJob(requestId: string, jobsUrl: string = `${this.base}/analysis`): Observable<OrchestratedJob> {
    const url = `${jobsUrl}/${requestId}`;
    return timer(0, this.POLL_MS).pipe(
      switchMap(() => this.http.get<OrchestratedJob>(url, { withCredentials: true })),
      takeWhile((job) => job.status === 'PENDING' || job.status === 'RUNNING', true)
    );
  }

  /** Poll RCA readiness until it settles (READY/PARTIAL/FAILED). */
  pollRcaStatus(requestId: string): Observable<RcaStatus> {
    const url = `${this.base}/analysis/${requestId}/rca-status`;
    return timer(0, this.POLL_MS).pipe(
      switchMap(() => this.http.get<RcaStatus>(url, { withCredentials: true })),
      takeWhile((s) => s.status === 'NOT_STARTED' || s.status === 'GENERATING', true)
    );
  }

  rcaFilesUrl(requestId: string): string {
    return `${this.base}/analysis/${requestId}/rca-files`;
  }
}