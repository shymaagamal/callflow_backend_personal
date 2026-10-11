// src/app/download-button/download-button.component.ts
import { Component, Input, OnDestroy } from '@angular/core';
import { ApiService } from '../services/api.service';
import { JobStatus } from '../services/job-polling.service';
import { Subscription } from 'rxjs';

const DOWNLOAD_GROUP = 'callflow_download_files';

interface SystemState {
  key: string;
  label: string;
  icon: string;
  checked: boolean;
  status: 'idle' | 'running' | 'done' | 'failed';
}

type PanelState = 'idle' | 'running' | 'done' | 'failed';

@Component({
  selector: 'app-download-button',
  templateUrl: './download-button.component.html',
  styleUrl: './download-button.component.css'
})
export class DownloadButtonComponent implements OnDestroy {
  /**
   * The form data from UserInputFormComponent.
   * formData.msisdn_list is still a raw string at this point
   * (comma-separated) -- we split it before sending.
   */
  @Input() formData: any = {};

  canDownload = false;
  showPanel = false;

  panelState: PanelState = 'idle';
  currentStep = '';
  overallProgress = 0;
  error: string | null = null;
  zipUrl: string | null = null;
  sizeKb = 0;
  failedSystems: string[] = [];

  systems: SystemState[] = [
    { key: 'smartcare', label: 'Smartcare',           icon: 'pi-server',   checked: true, status: 'idle' },
    { key: 'npp',       label: 'NPP',                 icon: 'pi-wifi',     checked: true, status: 'idle' },
    { key: 'splunk',    label: 'Splunk',               icon: 'pi-search',   checked: true, status: 'idle' },
    { key: 'viavi',     label: 'Transmission (Viavi)', icon: 'pi-sitemap',  checked: true, status: 'idle' },
    { key: 'remedy',    label: 'Remedy',               icon: 'pi-ticket',   checked: true, status: 'idle' },
  ];

  private sub: Subscription | null = null;
  authService: any;

  constructor(private apiService: ApiService) {
    this.canDownload = this.checkGroupAccess();
  }

  private checkGroupAccess(): boolean {
    try {
      const raw = localStorage.getItem('user');
      if (!raw) return false;
      const user = JSON.parse(raw);
      const groups: any[] = user?.user_details?.user_groups || [];
      return groups.some(g =>
        (typeof g === 'string' ? g : g?.group) === DOWNLOAD_GROUP
      );
    } catch { return false; }
  }

  get selectedKeys(): string[] {
    return this.systems.filter(s => s.checked).map(s => s.key);
  }

  get isFormReady(): boolean {
    return !!(
      this.formData?.msisdn_list &&
      this.formData?.start_date &&
      this.formData?.end_date &&
      this.formData?.Complain_Type
    );
  }

  togglePanel() {
    if (this.panelState === 'running') return;
    this.showPanel = !this.showPanel;
  }

  startExport() {
    if (!this.isFormReady || this.panelState === 'running') return;

    const msisdnArray = (this.formData.msisdn_list as string)
      .split(',').map((x: string) => x.trim()).filter(Boolean);

    const formatDate = (d: Date): string => {
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, '0');
      const dd = String(d.getDate()).padStart(2, '0');
      const hh = String(d.getHours()).padStart(2, '0');
      const min = String(d.getMinutes()).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
    };

    const payload = {
      start_date:    formatDate(this.formData.start_date),
      end_date:      formatDate(this.formData.end_date),
      Complain_Type: this.formData.Complain_Type,
      msisdn_list:   msisdnArray,
      systems:       this.selectedKeys,
    };

    // Reset system states
    this.systems.forEach(s => s.status = s.checked ? 'running' : 'idle');
    this.panelState = 'running';
    this.currentStep = 'Fetching Smartcare data first...';
    this.overallProgress = 0;
    this.error = null;
    this.zipUrl = null;
    this.failedSystems = [];
    this.sub = this.apiService.requestExportJob(payload).subscribe({
      next: (job: JobStatus) => this.onJobUpdate(job),
      error: (err) => {
        this.panelState = 'failed';
        this.error = err?.error?.error || err?.message || 'Request failed.';
        this.systems.forEach(s => { if (s.status === 'running') s.status = 'failed'; });
      }
    });
  }

  private onJobUpdate(job: JobStatus) {
    this.overallProgress = job.progress_percent || 0;
    this.currentStep = job.step || '';

    // Update per-system status from the step text
    // Backend step text looks like "Completed: smartcare, npp"
    if (job.step?.toLowerCase().startsWith('completed:')) {
      const donePart = job.step.replace(/completed:/i, '').trim();
      const doneSystems = donePart.split(',').map(s => s.trim().toLowerCase());
      this.systems.forEach(s => {
        if (doneSystems.includes(s.key)) s.status = 'done';
      });
    }

    if (job.status === 'DONE') {
      this.panelState = 'done';
      this.zipUrl = job.result?.zip_url || null;
      this.sizeKb = job.result?.size_kb || 0;
      this.failedSystems = job.result?.failed || [];

      const doneSystems: string[] = job.result?.systems_done || [];
      this.systems.forEach(s => {
        if (doneSystems.includes(s.key)) s.status = 'done';
        else if (s.status === 'running') s.status = 'failed';
      });

      if (this.zipUrl) this.triggerBrowserDownload(this.zipUrl);

    } else if (job.status === 'FAILED') {
      this.panelState = 'failed';
      this.error = job.error || 'Export failed.';
      this.systems.forEach(s => { if (s.status === 'running') s.status = 'failed'; });
    }
  }

  private triggerBrowserDownload(relativeUrl: string) {
    const url = relativeUrl.startsWith('http')
      ? relativeUrl
      : `https://10.30.30.177${relativeUrl}`;
    const a = document.createElement('a');
    a.href = url;
    a.download = '';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }

  reset() {
    this.panelState = 'idle';
    this.currentStep = '';
    this.overallProgress = 0;
    this.error = null;
    this.zipUrl = null;
    this.failedSystems = [];
    this.systems.forEach(s => s.status = 'idle');
    this.sub?.unsubscribe();
    this.sub = null;
  }

  ngOnDestroy() { this.sub?.unsubscribe(); }
}