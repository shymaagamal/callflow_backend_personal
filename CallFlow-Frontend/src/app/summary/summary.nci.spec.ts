/**
 * NCI context: the existing Summary page opened from an NCI portal_url
 * (?mode=nci&sites=...). The full UI stays; NPP / Viavi / Remedy show NCI data,
 * everything NCI does not produce shows "Not Available" (never Pending/zeros).
 * The normal portal must be unchanged.
 */
import { of, throwError } from 'rxjs';
import { SummaryComponent } from './summary.component';

function makeComponent(nciSites: string[] | null, orchestration: any = {}) {
  const cdr: any = { detectChanges: () => {} };
  const c = new SummaryComponent({} as any, {} as any, orchestration, cdr);
  (c as any).readNciContext = () => nciSites;   // stands in for window.location.search
  (c as any).readNciResultId = () => null;      // no nci_id unless a test sets one
  return c;
}

function fakeOrchestration(job: any) {
  const calls: any = { submitted: null, polledUrl: null, rcaPolled: false };
  const svc: any = {
    nciJobsUrl: 'http://x/callflow/NCI/portal/analysis',
    submitNci: (sites: string[]) => { calls.submitted = sites; return of({ job_id: 'NCI-1', status: 'PENDING' }); },
    pollJob: (id: string, url?: string) => { calls.polledUrl = `${url}/${id}`; return of(job); },
    pollRcaStatus: () => { calls.rcaPolled = true; return of({ status: 'NOT_STARTED', files: {} }); },
    rcaFilesUrl: () => '',
  };
  return { svc, calls };
}

const NCI_JOB = {
  job_id: 'NCI-1', status: 'DONE', step: 'completed', progress_percent: 100,
  systems: { npp: 'COMPLETED', viavi: 'COMPLETED', remedy: 'COMPLETED' },
  dashboard: { cellsAffected: 1, totalCells: 2, remedyAffected: 2 },
  summary: { affected_cells: { status: 'READY_WITH_DATA', value: 1 }, tickets: { status: 'READY_WITH_DATA', value: 2 } },
  details: {
    npp_data: { cells_affected_list: ['0160'], affected_cells_details: {
      '0160': { site_name: '0160', impacted_customers: null, kpis: { Latency: { severity: 'high' }, CFR: { severity: 'low' } } } } },
    abnormal: {
      npp: [{ cell_name: '0160', impacted_customers: null, degraded_kpis: [{ name: 'CFR' }] }],
      viavi: [{ site_name: '0160', impacted_customers: 0, degraded_kpis: [{ name: 'Latency' }, { name: 'Jitter' }] }],
      splunk: [],
    },
    states: {},
  },
  result: { request_id: 'NCI-1', overall_status: 'COMPLETED' },
};

describe('SummaryComponent NCI mode', () => {
  it('normal portal is unchanged when not opened from an NCI URL', () => {
    const g: any = globalThis as any;
    if (!g.history) g.history = { state: {} };      // node test runner only; browsers have it
    const c = makeComponent(null);
    c.ngOnInit();
    expect(c.nciMode).toBeFalse();
    expect(c.systemRows.length).toBe(6);
    expect(c.Abnormal_System.map((o) => o.value)).toContain('Splunk');
  });

  it('starts an NCI job for the URL sites and polls it on the NCI jobs URL', () => {
    const { svc, calls } = fakeOrchestration(NCI_JOB);
    const c = makeComponent(['0160', '1797'], svc);
    c.ngOnInit();
    expect(c.nciMode).toBeTrue();
    expect(calls.submitted).toEqual(['0160', '1797']);
    expect(calls.polledUrl).toBe('http://x/callflow/NCI/portal/analysis/NCI-1');
    expect(c.request_id).toBe('NCI-1');
  });

  it('keeps every system row; systems NCI does not provide are Not Available, never Pending', () => {
    const { svc, calls } = fakeOrchestration(NCI_JOB);
    const c = makeComponent(['0160', '1797'], svc);
    c.ngOnInit();
    expect(c.systemRows.length).toBe(6);
    const st: any = {};
    c.systemRows.forEach((r) => (st[r.key] = r.status));
    expect(st.trino).toBe('NOT_AVAILABLE');
    expect(st.smartcare).toBe('NOT_AVAILABLE');
    expect(st.splunk).toBe('NOT_AVAILABLE');
    expect(st.npp).toBe('COMPLETED');
    expect(st.viavi).toBe('COMPLETED');
    expect(st.remedy).toBe('COMPLETED');
    expect(c.statusText('NOT_AVAILABLE')).toBe('NOT AVAILABLE');
    expect(c.abnormalJobs['splunk'].status).toBe('NOT_AVAILABLE');
    expect(c.abnormalAnyPending).toBeFalse();
    expect(calls.rcaPolled).toBeFalse();
    expect(c.rcaMessage).toBe('Not Available');
    expect(c.overallStatus).toBe('COMPLETED');
  });

  it('unavailable systems stay Not Available even before the NCI job finishes', () => {
    const running = { ...NCI_JOB, status: 'RUNNING', systems: { npp: 'PROCESSING', viavi: 'PROCESSING', remedy: 'PROCESSING' } };
    const c = makeComponent(['0160'], fakeOrchestration(running).svc);
    c.ngOnInit();
    const st: any = {};
    c.systemRows.forEach((r) => (st[r.key] = r.status));
    expect(st).toEqual({ trino: 'NOT_AVAILABLE', smartcare: 'NOT_AVAILABLE', npp: 'PROCESSING',
                         splunk: 'NOT_AVAILABLE', viavi: 'PROCESSING', remedy: 'PROCESSING' });
    expect(c.abnormalJobs['npp'].status).toBe('RUNNING');
    expect(c.abnormalJobs['viavi'].status).toBe('RUNNING');
    expect(c.processing).toBeTrue();
    const splunk = c.systemRows.find((r) => r.key === 'splunk')!;
    expect(splunk.status).toBe('NOT_AVAILABLE');
    expect(c.abnormalJobs['splunk'].status).toBe('NOT_AVAILABLE');
  });

  it('keeps all cards; non-NCI cards and popups show Not Available', () => {
    const c = makeComponent(['0160', '1797'], fakeOrchestration(NCI_JOB).svc);
    c.ngOnInit();
    expect(c.Abnormal_System.map((o) => o.value)).toContain('Splunk');
    for (const k of ['totalComplaints', 'citiesSpanned', 'handsetTypes', 'hubsAffected', 'tacsLacsAffected', 'packetCoreNodes']) {
      expect(c.cardState(k)).toBe('notAvailable');
      expect(c.cardValue(k)).toBe('Not Available');
    }
    c.Select_Abnormal_System = 'Splunk';
    c.onAbnormalSystemChange();
    expect(c.Abnormal_Kpis_Data.length).toBe(0);
    expect(c.No_Abnormal_Data_Message).toBe('Not Available');
    c.openModal('hubs');
    expect(c.modalNotAvailable).toBeTrue();
    expect(c.modalNoData).toBeTrue();
  });

  it('normal portal never shows Not Available', () => {
    const g: any = globalThis as any;
    if (!g.history) g.history = { state: {} };
    const c = makeComponent(null);
    c.ngOnInit();
    expect(c.systemRows.every((r) => r.status !== 'NOT_AVAILABLE')).toBeTrue();
    expect(c.cardState('handsetTypes')).not.toBe('notAvailable');
    expect(c.rcaMessage).not.toBe('Not Available');
  });

  it('cards and charts use NCI data (sites, degraded-KPI counts)', () => {
    const { svc } = fakeOrchestration(NCI_JOB);
    const c = makeComponent(['0160', '1797'], svc);
    c.ngOnInit();
    expect(c.cellsAffectedText).toBe('1/2');
    expect(c.cardValue('remedyAffected')).toBe(2);
    expect(c.Select_Abnormal_System).toBe('NPP');
    expect(c.Abnormal_Kpis_Data[0].value).toBe(1);
    expect(c.abnormalYAxisLabel).toBe('No. of degraded KPIs');
    c.Select_Abnormal_System = 'Viavi';
    c.onAbnormalSystemChange();
    expect(c.Abnormal_Kpis_Data[0].value).toBe(2);
    c.openModal('cells');
    expect(c.modalTitle).toBe('Cells Affected - Detailed Breakdown');
    expect(c.modalNotAvailable).toBeFalse();
    expect(c.modalData[0].value).toBe(2);              // 2 degraded KPIs on site 0160
  });

  it('reads mode/sites from the route query params (e.g. after returning from login)', () => {
    const cdr: any = { detectChanges: () => {} };
    const route: any = { snapshot: { queryParamMap: { get: (k: string) => ({ mode: 'nci', sites: '0160, 1797' } as any)[k] ?? null } } };
    const c = new SummaryComponent({} as any, {} as any, {} as any, cdr, route);
    expect((c as any).readNciContext()).toEqual(['0160', '1797']);
    // portal_url copied with "&" escaped as \u0026 (address bar shows %5Cu0026)
    const esc: any = { snapshot: { queryParamMap: { get: (k: string) => ({ mode: 'nci\\u0026sites=0160,1797' } as any)[k] ?? null } } };
    expect((new SummaryComponent({} as any, {} as any, {} as any, cdr, esc) as any).readNciContext()).toEqual(['0160', '1797']);
    const normal: any = { snapshot: { queryParamMap: { get: () => null } } };
    const d = new SummaryComponent({} as any, {} as any, {} as any, cdr, normal);
    expect((d as any).readNciContext()).toBeNull();
  });

  it('link from the NCI API shows that stored result: no new NCI analysis', () => {
    const { svc, calls } = fakeOrchestration(NCI_JOB);
    const c = makeComponent(['0160', '1797'], svc);
    (c as any).readNciResultId = () => 'abc123abc123abcd';
    c.ngOnInit();
    expect(calls.submitted).toBeNull();                       // nothing re-run
    expect(calls.polledUrl).toBe('http://x/callflow/NCI/portal/analysis/abc123abc123abcd');
    expect(c.overallStatus).toBe('COMPLETED');
    expect(c.cellsAffectedText).toBe('1/2');
  });

  it('reads nci_id from the link, also when it was copied with \\u0026', () => {
    const cdr: any = { detectChanges: () => {} };
    const mk = (q: any) => new SummaryComponent({} as any, {} as any, {} as any, cdr,
      { snapshot: { queryParamMap: { get: (k: string) => q[k] ?? null } } } as any) as any;
    const plain = mk({ mode: 'nci', nci_id: 'abc123abc123abcd', sites: 'D2431,K4369' });
    expect(plain.readNciResultId()).toBe('abc123abc123abcd');
    expect(plain.readNciContext()).toEqual(['D2431', 'K4369']);
    const esc = mk({ mode: 'nci\\u0026nci_id=abc123abc123abcd\\u0026sites=D2431,K4369' });
    expect(esc.readNciResultId()).toBe('abc123abc123abcd');
    expect(esc.readNciContext()).toEqual(['D2431', 'K4369']);
  });

  it('failed NCI job: no system stays PROCESSING and the error is shown', () => {
    const failed = { job_id: 'NCI-1', status: 'FAILED', step: 'failed', error: 'boom',
                     systems: { npp: 'PROCESSING', viavi: 'PROCESSING', remedy: 'PROCESSING' } };
    const c = makeComponent(['D2431'], fakeOrchestration(failed).svc);
    c.ngOnInit();
    const st: any = {};
    c.systemRows.forEach((r) => (st[r.key] = r.status));
    expect(st).toEqual({ trino: 'NOT_AVAILABLE', smartcare: 'NOT_AVAILABLE', npp: 'FAILED',
                         splunk: 'NOT_AVAILABLE', viavi: 'FAILED', remedy: 'FAILED' });
    expect(c.nciError).toBe('boom');
    expect(c.processing).toBeFalse();
  });

  it('expired / unknown NCI result (404) fails clearly', () => {
    const { svc } = fakeOrchestration(NCI_JOB);
    svc.pollJob = () => throwError(() => ({ status: 404 }));
    const c = makeComponent(['D2431'], svc);
    (c as any).readNciResultId = () => 'abc123abc123abcd';
    c.ngOnInit();
    expect(c.nciError).toBe('NCI result not found or expired');
    expect(c.systemRows.find((r) => r.key === 'npp')!.status).toBe('FAILED');
  });

  it('NCI URL without sites fails clearly instead of loading forever', () => {
    const c = makeComponent([], fakeOrchestration(NCI_JOB).svc);
    c.ngOnInit();
    expect(c.nciMode).toBeTrue();
    expect(c.processing).toBeFalse();
    expect(c.overallStatus).toBe('FAILED');
    expect(c.systemRows.find((r) => r.key === 'npp')!.status).toBe('FAILED');
    expect(c.systemRows.find((r) => r.key === 'splunk')!.status).toBe('NOT_AVAILABLE');
  });
});
