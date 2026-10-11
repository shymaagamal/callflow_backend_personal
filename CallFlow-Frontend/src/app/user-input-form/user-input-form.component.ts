// src/app/components/user-input-form/user-input-form.component.ts
/**
 * What changed vs. the original:
 *   - onStartAnalyzing() no longer fires one blocking Smartcare call then
 *     chains to NPP via switchMap on a single HTTP response. It now uses
 *     ApiService.runSmartcareJob/runNppMinutesJob, each of which is a
 *     STREAM of {status, step, progress_percent} updates -- so `pipeline`
 *     below reflects exactly what the backend log file would show,
 *     instead of "blank spinner -> result".
 *   - The previous full-screen spinner (*ngIf="isLoading") is replaced
 *     with a small two-step pipeline panel built from
 *     <app-job-status-card>, the same component used on the abnormal-KPI
 *     panel in SummaryComponent -- one visual language for "something is
 *     running" everywhere in the app.
 *   - Smartcare's job result now includes `cache_key` and `ts`. Both are
 *     carried forward into the /summary navigation payload so
 *     RootcauseComponent can pass them to the backend instead of relying
 *     on the old shared singleton file.
 */

import { Component } from '@angular/core';
import { Router } from '@angular/router';
import { ApiService } from '../services/api.service';
import { JobStatus } from '../services/job-polling.service';
import { OrchestrationService } from '../services/orchestration.service';
import { AuthService } from '../auth.service';

interface PipelineStep {
  key: 'smartcare' | 'npp';
  label: string;
  status: 'PENDING' | 'RUNNING' | 'DONE' | 'FAILED';
  step: string;
  progress: number;
  error: string | null;
}

@Component({
  selector: 'app-user-input-form',
  templateUrl: './user-input-form.component.html',
  styleUrl: './user-input-form.component.css'
})
export class UserInputFormComponent {
  canDownload = false;
  constructor(
    private router: Router,
    private apiService: ApiService,
    private orchestration: OrchestrationService,
    private authService: AuthService,
  ) {}
  ngOnInit() {
      this.canDownload = this.authService.hasGroup('callflow_download_files');
    }
  formData = {
    start_date: null,
    end_date: null,
    Complain_Type: null,
    msisdn_list: null
  };

  smartcare_data_metrics: any = {};
  npp_data: any = {};

  complaintTypes = [
    { label: 'Data', value: 'Data' },
    { label: 'Voice', value: 'Voice' },
  ];

  isLoading = false;
  Total_Complaints = 0;

  pipeline: PipelineStep[] = [];

  onStartAnalyzing() {
    const msisdn_format = this.formData.msisdn_list
      .split(',')
      .map(x => x.trim())
      .filter(Boolean);

    const validation = this.validateMsisdnList(msisdn_format);
    if (!validation.valid) {
      alert(validation.message);
      return;
    }

    const dateValidation = this.validateDateRange(this.formData.start_date, this.formData.end_date);
    if (!dateValidation.valid) {
      alert(dateValidation.message);
      return;
    }

    if (!this.isFormValid()) {
      alert('Please fill all required fields');
      return;
    }

    const payload = {
      ...this.formData,
      start_date: this.formatDate(this.formData.start_date),
      end_date: this.formatDate(this.formData.end_date),
      msisdn_list: msisdn_format
    };
    this.Total_Complaints = msisdn_format.length;

    // NEW FLOW: submit ONE data-availability orchestrated job and hand off to
    // the summary page, which tracks per-system progress + RCA readiness.
    // (Replaces the old Smartcare -> extract cells -> NPP -> summary chain.)
    this.isLoading = true;
    this.orchestration.submit(payload).subscribe({
      next: (res) => {
        this.isLoading = false;
        this.router.navigate(['/summary'], {
          state: {
            request_id: res.job_id,
            payload,
            data: this.formData,
            msisdn_list: payload.msisdn_list,
            start_date: payload.start_date,
            end_date: payload.end_date,
            Total_Complaints: this.Total_Complaints,
          },
        });
      },
      error: (err) => {
        this.isLoading = false;
        alert(err?.message || 'Error submitting analysis request');
      },
    });
  }

  private isFormValid(): boolean {
    return !!(
      this.formData.Complain_Type &&
      this.formData.start_date &&
      this.formData.end_date &&
      this.formData.msisdn_list
    );
  }

  private formatDate(date: Date): string {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const MM = String(date.getMinutes()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}T${hh}:${MM}`;
  }

  private validateMsisdnList(msisdnArray: string[]): { valid: boolean; message?: string } {
    if (!msisdnArray.length) {
      return { valid: false, message: 'MSISDN list is empty' };
    }

    const msisdnRegex = /^(10|11|12|15)\d{8}$/;
    const invalidMsisdns = msisdnArray.filter(msisdn => !msisdnRegex.test(msisdn));
    if (invalidMsisdns.length) {
      return {
        valid: false,
        message: `Invalid MSISDN format: ${invalidMsisdns.join(', ')} Should be Start with 10 OR 11 OR 12 OR 15`
      };
    }

    const unique = new Set(msisdnArray);
    if (unique.size !== msisdnArray.length) {
      return { valid: false, message: 'Duplicate MSISDNs detected' };
    }

    return { valid: true };
  }

  private validateDateRange(start: Date, end: Date): { valid: boolean; message?: string } {
    if (!start || !end) {
      return { valid: false, message: 'Start date and End date are required' };
    }

    const today = new Date();
    today.setHours(0, 0, 0, 0);

    const sevenDaysAgo = new Date(today);
    sevenDaysAgo.setDate(today.getDate() - 7);

    const normalizedStart = new Date(start);
    normalizedStart.setHours(0, 0, 0, 0);

    if (normalizedStart <= sevenDaysAgo || normalizedStart > today) {
      return { valid: false, message: 'Start date must be between last 7 days' };
    }

    const diffMs = end.getTime() - start.getTime();
    const diffMinutes = diffMs / (1000 * 60);

    if (diffMinutes < 0) {
      return { valid: false, message: 'End date must be after Start date' };
    }
    if (diffMinutes > 120) {
      return { valid: false, message: 'Date range cannot exceed 2 Hour' };
    }

    return { valid: true };
  }
}