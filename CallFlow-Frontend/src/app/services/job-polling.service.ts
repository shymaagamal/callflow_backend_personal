// src/app/services/job-polling.service.ts
/**
 * Generic polling layer matching the backend's job contract:
 *   POST <endpoint>      -> { job_id, status: "PENDING" }
 *   GET  <endpoint>/<id> -> { status, step, progress_percent, result, error }
 *
 * Every job-backed call in ApiService goes through runJob() below, which
 * returns ONE Observable that emits every intermediate status (so the UI
 * can show real progress) and completes after the final DONE/FAILED state.
 * Components don't need to know polling is happening -- they just
 * subscribe like any other HTTP call and get a stream of updates instead
 * of a single value.
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, timer } from 'rxjs';
import { switchMap, takeWhile } from 'rxjs/operators';

export type JobUiStatus = 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED';

export interface JobStatus<T = any> {
  job_id: string;
  job_type: string;
  who: string;
  status: JobUiStatus;
  step: string;
  progress_percent: number;
  result: T | null;
  error: string | null;
}

export interface JobTrigger {
  job_id: string;
  status: string;
}

@Injectable({ providedIn: 'root' })
export class JobPollingService {
  /** How often to poll. 1.5s balances "feels live" against hammering the API. */
  private readonly DEFAULT_INTERVAL_MS = 1500;

  constructor(private http: HttpClient) {}

  private poll<T>(statusUrl: string, intervalMs: number): Observable<JobStatus<T>> {
    console.log(`🔄 Polling: ${statusUrl}`);
    return timer(0, intervalMs).pipe(
      switchMap(() => this.http.get<JobStatus<T>>(statusUrl, {
        withCredentials: true   // ← THIS WAS MISSING
      })),
      // `true` makes takeWhile inclusive -- the DONE/FAILED emission that
      // ends the stream is still delivered to subscribers.
      takeWhile((job) => job.status === 'PENDING' || job.status === 'RUNNING', true)
    );
  }

  /**
   * trigger$: the POST call that creates the job.
   * buildStatusUrl: given a job_id, returns the GET polling URL.
   */
  runJob<T = any>(
    trigger$: Observable<JobTrigger>,
    buildStatusUrl: (jobId: string) => string,
    intervalMs: number = this.DEFAULT_INTERVAL_MS
  ): Observable<JobStatus<T>> {
    return trigger$.pipe(
      switchMap((trigger) => this.poll<T>(buildStatusUrl(trigger.job_id), intervalMs))
    );
  }
}