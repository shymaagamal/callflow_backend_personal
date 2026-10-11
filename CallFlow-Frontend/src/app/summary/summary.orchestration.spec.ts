/**
 * Focused unit tests for the orchestrated job -> UI mapping in SummaryComponent.
 *
 * These instantiate the component class directly with lightweight mocks (no
 * template compilation), so they verify the state machine that drives progress,
 * partial success and RCA readiness gating -- the acceptance-critical logic.
 */
import { of } from 'rxjs';
import { SummaryComponent } from './summary.component';
import { OrchestratedJob, RcaStatus } from '../services/orchestration.service';

function makeComponent(rca: RcaStatus) {
  const router: any = {};
  const api: any = {};
  const cdr: any = { detectChanges: () => {} };
  const orchestration: any = {
    pollRcaStatus: () => of(rca),
    rcaFilesUrl: (id: string) => `/callflow/api/analysis/${id}/rca-files`,
  };
  return new SummaryComponent(router, api, orchestration, cdr);
}

describe('SummaryComponent orchestration mapping', () => {
  it('shows per-system PROCESSING while the job is RUNNING', () => {
    const c = makeComponent({ request_id: 'R', status: 'GENERATING', files: {} });
    const job: OrchestratedJob = {
      job_id: 'R', status: 'RUNNING', step: 'analysis', progress_percent: 40,
      systems: { trino: 'COMPLETED', smartcare: 'PROCESSING', npp: 'PROCESSING',
                 splunk: 'PROCESSING', viavi: 'PENDING', remedy: 'PENDING' } as any,
    };
    (c as any).applyJob(job);
    expect(c.processing).toBeTrue();
    const trino = c.systemRows.find(r => r.key === 'trino')!;
    const smartcare = c.systemRows.find(r => r.key === 'smartcare')!;
    expect(trino.status).toBe('COMPLETED');
    expect(smartcare.status).toBe('PROCESSING');
  });

  it('marks COMPLETED and enables RCA only after files are READY', () => {
    const c = makeComponent({
      request_id: 'R', status: 'READY',
      files: { smartcare: 'READY', npp: 'READY' },
      rca_files_url: '/callflow/api/analysis/R/rca-files',
    });
    (c as any).request_id = 'R';
    const job: OrchestratedJob = {
      job_id: 'R', status: 'DONE', step: 'analysis', progress_percent: 100,
      result: {
        request_id: 'R', overall_status: 'COMPLETED',
        systems: { trino: 'COMPLETED', smartcare: 'COMPLETED', npp: 'COMPLETED',
                   splunk: 'COMPLETED', viavi: 'COMPLETED', remedy: 'COMPLETED' } as any,
        rca: { request_id: 'R', status: 'READY', files: {} },
      },
    };
    (c as any).applyJob(job);
    expect(c.processing).toBeFalse();
    expect(c.overallStatus).toBe('COMPLETED');
    expect(c.rcaReady).toBeTrue();
    expect(c.isDataLoaded()).toBeTrue();
    expect(c.RootCause_Payload.request_id).toBe('R');
    expect(c.RootCause_Payload.rca_files_url).toContain('/analysis/R/rca-files');
  });

  it('keeps RCA disabled and shows GENERATING while files are still being written', () => {
    const c = makeComponent({ request_id: 'R', status: 'GENERATING', files: { npp: 'GENERATING' } });
    (c as any).request_id = 'R';
    const job: OrchestratedJob = {
      job_id: 'R', status: 'DONE', step: 'analysis', progress_percent: 100,
      result: {
        request_id: 'R', overall_status: 'COMPLETED',
        systems: {} as any,
        rca: { request_id: 'R', status: 'GENERATING', files: {} },
      },
    };
    (c as any).applyJob(job);
    expect(c.rcaReady).toBeFalse();
    expect(c.isDataLoaded()).toBeFalse();
    expect(c.rcaReadiness).toBe('GENERATING');
  });

  it('surfaces partial success with per-system failure messages', () => {
    const c = makeComponent({ request_id: 'R', status: 'PARTIAL', files: {} });
    (c as any).request_id = 'R';
    const job: OrchestratedJob = {
      job_id: 'R', status: 'DONE', step: 'analysis', progress_percent: 100,
      result: {
        request_id: 'R', overall_status: 'PARTIAL_SUCCESS',
        systems: { trino: 'COMPLETED', smartcare: 'COMPLETED', npp: 'COMPLETED',
                   splunk: 'COMPLETED', viavi: 'FAILED', remedy: 'COMPLETED' } as any,
        rca: { request_id: 'R', status: 'PARTIAL', files: {} },
      },
    };
    (c as any).applyJob(job);
    expect(c.overallStatus).toBe('PARTIAL_SUCCESS');
    const viavi = c.systemRows.find(r => r.key === 'viavi')!;
    expect(viavi.status).toBe('FAILED');
    expect(c.systemMessage(viavi)).toContain('Viavi processing failed');
    const npp = c.systemRows.find(r => r.key === 'npp')!;
    expect(c.systemMessage(npp)).toBe(''); // successful system: no error message
    // RCA still openable for partial results.
    expect(c.rcaReady).toBeTrue();
  });

  it('smartcare failure -> dependent systems PARTIAL with clear messaging', () => {
    const c = makeComponent({ request_id: 'R', status: 'PARTIAL', files: {} });
    (c as any).request_id = 'R';
    const job: OrchestratedJob = {
      job_id: 'R', status: 'DONE', step: 'analysis', progress_percent: 100,
      result: {
        request_id: 'R', overall_status: 'PARTIAL_SUCCESS',
        systems: { trino: 'COMPLETED', smartcare: 'FAILED', npp: 'PARTIAL',
                   splunk: 'PARTIAL', viavi: 'PARTIAL', remedy: 'PARTIAL' } as any,
        rca: { request_id: 'R', status: 'PARTIAL', files: {} },
      },
    };
    (c as any).applyJob(job);
    const sc = c.systemRows.find(r => r.key === 'smartcare')!;
    expect(c.systemMessage(sc)).toContain('Smartcare data could not be retrieved');
    const npp = c.systemRows.find(r => r.key === 'npp')!;
    expect(c.systemMessage(npp)).toContain('analysis unavailable');
  });
});
