// src/app/services/api.service.ts
/**
 * Every long-running call now follows the same shape:
 *   run*Job(payload): Observable<JobStatus>
 * Subscribing gives you a stream of updates as the backend works through
 * its steps (PENDING -> RUNNING -> DONE/FAILED), not a single response at
 * the end. Components read job.status / job.step / job.progress_percent
 * to drive the UI, and job.result once status === 'DONE'.
 *
 * Removed vs. the original file:
 *   - triggerSmartcareBackend(file, ...) and triggerSmartcareKpisBackend(...)
 *     posted FormData to /callflow/api/smartcare/kpis, which doesn't exist
 *     in urls.py -- these looked like dead code from an earlier iteration
 *     before the JSON-payload "_Core" methods replaced them. Removed
 *     rather than carried forward; flag if something still depends on them.
 *   - The "_Core" suffix is gone -- there's only one version of each call now.
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { JobPollingService, JobStatus, JobTrigger } from './job-polling.service';

@Injectable({ providedIn: 'root' })
export class ApiService {
  private apiUrl = 'https://10.30.30.177';
  // private apiUrl = 'http://127.0.0.1:8050';
  private base = `${this.apiUrl}/callflow/api`;

  constructor(private http: HttpClient, private jobs: JobPollingService) {}
  private get httpOptions() {
      return {
        withCredentials: true,
        headers: { 'Content-Type': 'application/json' }
      };
    }
  private triggerJob(path: string, data: any): Observable<JobTrigger> {
    return this.http.post<JobTrigger>(`${this.base}/${path}`, data,
      this.httpOptions

    );
  }

  private statusUrl(path: string, jobId: string): string {
    return `${this.base}/${path}/${jobId}`;
  }

  // ---- Smartcare ---------------------------------------------------
  runSmartcareJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('smartcare', data),
      (jobId) => this.statusUrl('smartcare', jobId)
    );
  }

  // ---- Splunk (standalone) ------------------------------------------
  runSplunkJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('splunk', data),
      (jobId) => this.statusUrl('splunk', jobId)
    );
  }

  // ---- NPP ------------------------------------------------------------
  runNppMinutesJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('npp/minutes', data),
      (jobId) => this.statusUrl('npp/minutes', jobId)
    );
  }

  runNppHourlyJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('npp/hourly', data),
      (jobId) => this.statusUrl('npp/hourly', jobId)
    );
  }

  // ---- Viavi ----------------------------------------------------------
  runViaviJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('viavi', data),
      (jobId) => this.statusUrl('viavi', jobId)
    );
  }

  // ---- Remedy ---------------------------------------------------------
  runRemedyJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('remedy', data),
      (jobId) => this.statusUrl('remedy', jobId)
    );
  }

  // ---- Smartcare failure -----------------------------------------------
  runSmartcareFailureJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('smartcare-failure', data),
      (jobId) => this.statusUrl('smartcare-failure', jobId)
    );
  }

  // ---- Abnormal KPIs (run independently, in parallel, by design) -------
  runAbnormalSplunkJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('abnormal/splunk/', data),
      (jobId) => this.statusUrl('abnormal/splunk', jobId)
    );
  }

  runAbnormalNppJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('abnormal/npp/', data),
      (jobId) => this.statusUrl('abnormal/npp', jobId)
    );
  }

  runAbnormalViaviJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('abnormal/viavi/', data),
      (jobId) => this.statusUrl('abnormal/viavi', jobId)
    );
  }

  // ---- Root cause: unchanged, still a single fast synchronous call -----
  // Payload must now include `cache_key` and `ts` (from a completed
  // Smartcare job's result) instead of relying on the old shared file.
  triggerRootCauseBackend(data: any): Observable<any> {
    return this.http.post(`${this.base}/root-cause`, data);
  }
    // ---- Download file export ----------------------------------------
  requestExportJob(data: any): Observable<JobStatus> {
    return this.jobs.runJob(
      this.triggerJob('export-data', data),
      (jobId) => this.statusUrl('export-data', jobId)
    );
  }
}