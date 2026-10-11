// src/app/components/job-status-card/job-status-card.component.ts
import { Component, Input } from '@angular/core';
import { JobUiStatus } from '../services/job-polling.service';

@Component({
  selector: 'app-job-status-card',
  templateUrl: './job-status-card.component.html',
  styleUrl: './job-status-card.component.css'
})
export class JobStatusCardComponent {
  /** Drives icon + color: 'smartcare' | 'splunk' | 'npp' | 'viavi' | 'remedy' */
  @Input() system: string = 'smartcare';
  @Input() label: string = '';
  @Input() status: JobUiStatus = 'PENDING';
  @Input() step: string = '';
  @Input() progress: number = 0;
  @Input() error: string | null = null;
  /** 'card' for grids (abnormal-KPI panel), 'row' for vertical pipelines (analyzing screen) */
  @Input() layout: 'row' | 'card' = 'card';

  get statusLabel(): string {
    return this.status.charAt(0) + this.status.slice(1).toLowerCase();
  }

  get statusKey(): string {
    return this.status.toLowerCase();
  }

  get iconClass(): string {
    const icons: Record<string, string> = {
      smartcare: 'pi-server',
      splunk: 'pi-search',
      npp: 'pi-wifi',
      viavi: 'pi-sitemap',
      remedy: 'pi-ticket',
    };
    return icons[this.system] || 'pi-cog';
  }
}