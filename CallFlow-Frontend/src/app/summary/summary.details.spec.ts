/**
 * Tests for detail propagation: Cities/Hubs/TAC-LAC data, card-detail popups,
 * and the Abnormal KPIs section. Verifies the orchestrated `details` payload
 * flows to the popups and abnormal chart (the data that was previously lost).
 */
import { SummaryComponent } from './summary.component';
import { OrchestratedJob } from '../services/orchestration.service';

function makeComponent() {
  const cdr: any = { detectChanges: () => {} };
  const c = new SummaryComponent({} as any, {} as any, {} as any, cdr);
  c.request_id = 'REQ-1';
  return c;
}

function setSystem(c: SummaryComponent, key: string, status: any) {
  c.systemRows = c.systemRows.map((r) => (r.key === key ? { ...r, status } : r));
}

function jobWithDetails(details: any): OrchestratedJob {
  return { job_id: 'REQ-1', status: 'RUNNING', step: '', progress_percent: 50, details } as any;
}

describe('SummaryComponent detail propagation', () => {
  it('Cities/Hubs/TAC-LAC flow from npp_data.affected_metrics to popups', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    (c as any).applyDetails(jobWithDetails({
      npp_data: {
        affected_cells_details: {},
        affected_metrics: {
          matched_sites_count: 3,
          subs_per_region: [{ label: 'Cairo', value: 10 }],
          subs_per_hub: [{ label: 'H1', value: 5 }],
          subs_per_tac: [{ label: 'T1', value: 6 }],
          subs_per_lac: [{ label: 'L1', value: 7 }],
        },
      },
    }));

    c.openModal('cities');
    expect(c.modalNoData).toBeFalse();
    expect(c.modalData[0].label).toBe('Cairo');

    c.openModal('hubs');
    expect(c.modalNoData).toBeFalse();
    expect(c.modalData[0].label).toBe('H1');

    c.openModal('tac/lac');
    expect(c.modalNoData).toBeFalse();
    expect(c.modalDataTac[0].label).toBe('T1');
    expect(c.modalDataLac[0].label).toBe('L1');
  });

  it('Cells popup receives real cell details + impacted customers', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'smartcare', 'COMPLETED');
    (c as any).applyDetails(jobWithDetails({
      smartcare_data_metrics: { metrics: { subs_per_cell: [{ label: 'CELL_A', value: 68 }] } },
      npp_data: {
        affected_cells_details: {
          CELL_A: { site_name: 'S1', impacted_customers: 68, kpis: { K: { current_value: 4.1, previous_value: 18, deviation_pct: -77, deviation_direction: 'decrease' } } },
        },
        affected_metrics: {},
      },
    }));

    c.openModal('cells');
    expect(c.modalNoData).toBeFalse();
    expect(c.modalData[0].label).toBe('CELL_A');
    expect(c.modalData[0].value).toBe(68);
    expect(c.modalData[0].tooltip).toContain('S1');
  });

  it('Cells popup chart uses impacted_customers even before Smartcare finishes', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'smartcare', 'PROCESSING');
    (c as any).applyDetails(jobWithDetails({
      npp_data: {
        affected_cells_details: {
          CELL_A: { site_name: 'S1', impacted_customers: 12, kpis: { K: { current_value: 1 } } },
          CELL_B: { site_name: 'S2', impacted_customers: null, kpis: { K: { current_value: 2 } } },
        },
        affected_metrics: {},
      },
    }));

    c.openModal('cells');
    const a = c.modalData.find((r: any) => r.label === 'CELL_A');
    const b = c.modalData.find((r: any) => r.label === 'CELL_B');
    expect(a.value).toBe(12);
    expect(b.value).toBe(0);   // chart axis needs a number; the table shows N/A
  });

  it('empty detail data shows "No detailed data available" (no fake chart)', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    (c as any).applyDetails(jobWithDetails({ npp_data: { affected_cells_details: {}, affected_metrics: {} } }));

    c.openModal('cities');
    expect(c.modalNoData).toBeTrue();
    expect(c.modalData.length).toBe(0);

    c.openModal('tac/lac');
    expect(c.modalNoData).toBeTrue();
  });

  it('Abnormal KPIs: splunk results propagate and display', () => {
    const c = makeComponent();
    setSystem(c, 'splunk', 'COMPLETED');
    c.Select_Abnormal_System = 'Splunk';
    (c as any).applyDetails(jobWithDetails({
      abnormal: {
        npp: [], viavi: [],
        splunk: [{ mapped_value: 'MME-01', impacted_subs: 42, degraded_kpis: [{ name: 'x' }] }],
      },
    }));

    expect(c.Abnormal_Kpis_RawData.splunk.length).toBe(1);
    expect(c.Abnormal_Kpis_Data.length).toBe(1);
    expect(c.Abnormal_Kpis_Data[0].label).toBe('MME-01');
    expect(c.Abnormal_Kpis_Data[0].value).toBe(42);
    expect(c.No_Abnormal_Data_Message).toBe('');
  });

  it('Abnormal KPIs: NPP results propagate and display when NPP selected', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    c.Select_Abnormal_System = 'NPP';
    (c as any).applyDetails(jobWithDetails({
      abnormal: {
        splunk: [], viavi: [],
        npp: [{ cell_name: 'CELL_A', impacted_customers: 68, degraded_kpis: [] }],
      },
    }));
    expect(c.Abnormal_Kpis_Data[0].label).toBe('CELL_A');
    expect(c.Abnormal_Kpis_Data[0].value).toBe(68);
  });

  it('Abnormal KPIs are per system: NPP/Viavi ready while Splunk still runs (job RUNNING)', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'viavi', 'COMPLETED');
    setSystem(c, 'splunk', 'PROCESSING');
    setSystem(c, 'smartcare', 'PROCESSING');
    (c as any).applyDetails(jobWithDetails({
      abnormal: {
        npp: [{ cell_name: 'CELL_A', impacted_customers: 7, degraded_kpis: [] }],
        viavi: [{ site_name: '1333', impacted_customers: 3, degraded_kpis: [] }],
        splunk: [],
      },
    }));

    // Each status card follows its own analysis, not the overall job.
    expect(c.abnormalJobs['npp'].status).toBe('DONE');
    expect(c.abnormalJobs['viavi'].status).toBe('DONE');
    expect(c.abnormalJobs['splunk'].status).toBe('RUNNING');
    expect(c.abnormalAnyPending).toBeTrue();

    // Default 'Splunk' is not ready -> switched to a ready system, data shown.
    expect(['NPP', 'Viavi']).toContain(c.Select_Abnormal_System);
    expect(c.Abnormal_Kpis_Data.length).toBe(1);
  });

  it('auto-select picks the first ready system and keeps it on later polls', () => {
    const c = makeComponent();
    setSystem(c, 'viavi', 'COMPLETED');
    setSystem(c, 'npp', 'PROCESSING');
    setSystem(c, 'splunk', 'PROCESSING');
    const details = { abnormal: { npp: [], splunk: [],
      viavi: [{ site_name: '1333', impacted_customers: 3, degraded_kpis: [] }] } };
    (c as any).applyDetails(jobWithDetails(details));
    expect(c.Select_Abnormal_System).toBe('Viavi');

    setSystem(c, 'npp', 'COMPLETED');                 // NPP becomes ready later
    (c as any).applyDetails(jobWithDetails(details));
    expect(c.Select_Abnormal_System).toBe('Viavi');   // no jumping around
  });

  it('user choice in the dropdown is never overridden by auto-select', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'splunk', 'PROCESSING');
    c.Select_Abnormal_System = 'Splunk';
    c.onAbnormalSystemChange();
    (c as any).applyDetails(jobWithDetails({ abnormal: { npp: [{ cell_name: 'A', impacted_customers: 1, degraded_kpis: [] }], splunk: [], viavi: [] } }));
    expect(c.Select_Abnormal_System).toBe('Splunk');
    expect(c.abnormalJobs['splunk'].status).toBe('RUNNING');
    expect(c.Abnormal_Kpis_Data.length).toBe(0);
  });

  it('status grid disappears once all three systems finished, even before the job is DONE', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'viavi', 'COMPLETED');
    setSystem(c, 'splunk', 'FAILED');
    (c as any).applyDetails(jobWithDetails({ abnormal: { npp: [], splunk: [], viavi: [] } }));
    expect(c.abnormalJobs['splunk'].status).toBe('FAILED');
    expect(c.abnormalAnyPending).toBeFalse();
  });

  it('nothing ready yet -> cards PENDING and selection unchanged', () => {
    const c = makeComponent();
    (c as any).applyDetails(jobWithDetails({ abnormal: { npp: [], splunk: [], viavi: [] } }));
    expect(c.abnormalJobs['npp'].status).toBe('PENDING');
    expect(c.abnormalJobs['npp'].step).toContain('Waiting for NPP analysis');
    expect(c.Select_Abnormal_System).toBe('Splunk');
  });

  it('No abnormal KPIs (system succeeded, empty) -> "no abnormal KPI" message', () => {
    const c = makeComponent();
    setSystem(c, 'splunk', 'COMPLETED');
    c.Select_Abnormal_System = 'Splunk';
    (c as any).applyDetails(jobWithDetails({ abnormal: { npp: [], splunk: [], viavi: [] } }));
    expect(c.Abnormal_Kpis_Data.length).toBe(0);
    expect(c.No_Abnormal_Data_Message).toBe('There is no abnormal KPI in this system');
  });

  it('Abnormal KPIs: failed system -> "unavailable" message, not "no abnormal"', () => {
    const c = makeComponent();
    setSystem(c, 'splunk', 'FAILED');
    c.Select_Abnormal_System = 'Splunk';
    (c as any).applyDetails(jobWithDetails({ abnormal: { npp: [], splunk: [], viavi: [] } }));
    expect(c.No_Abnormal_Data_Message).toContain('unavailable');
  });

  it('Cells popup builds per-cell KPI detail rows (real values/insights)', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'smartcare', 'COMPLETED');
    (c as any).applyDetails(jobWithDetails({
      smartcare_data_metrics: { metrics: { subs_per_cell: [{ label: 'CELL_A', value: 5 }] } },
      npp_data: {
        affected_cells_details: {
          CELL_A: { site_name: 'S1', kpis: { AVG_DL: { current_value: 4.1, previous_value: 18, deviation_pct: -77, severity: 'high', message: 'throughput dropped' } } },
        },
        affected_metrics: {},
      },
    }));
    c.openModal('cells');
    expect(c.showCellDetails).toBeTrue();
    expect(c.cellDetailRows.length).toBe(1);
    expect(c.cellDetailRows[0].cell).toBe('CELL_A');
    expect(c.cellDetailRows[0].kpi).toBe('AVG_DL');
    expect(c.cellDetailRows[0].current).toBe(4.1);
    expect(c.cellDetailRows[0].severity).toBe('high');
    expect(c.cellDetailRows[0].message).toContain('throughput');
    expect(c.modalNoData).toBeFalse();
  });

  it('Cells popup includes per-cell customer count (N/A when unknown)', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    (c as any).applyDetails(jobWithDetails({
      npp_data: {
        affected_cells_details: {
          CELL_A: { site_name: 'S1', impacted_customers: 1245, kpis: { K: { current_value: 4 } } },
          CELL_B: { site_name: 'S2', impacted_customers: null, kpis: { K: { current_value: 5 } } },
        },
        affected_metrics: {},
      },
    }));
    c.openModal('cells');
    const a = c.cellDetailRows.find(r => r.cell === 'CELL_A');
    const b = c.cellDetailRows.find(r => r.cell === 'CELL_B');
    expect(a.customers).toBe(1245);
    expect(b.customers).toBe('N/A');   // unknown -> N/A, not 0
  });

  it('Splunk all-clean shows "Analysis completed" (distinct from no abnormal / failed)', () => {
    const c = makeComponent();
    setSystem(c, 'splunk', 'COMPLETED');
    c.Select_Abnormal_System = 'Splunk';
    (c as any).applyDetails(jobWithDetails({
      abnormal: { npp: [], splunk: [], viavi: [] },
      states: { splunk: { state: 'SUCCESS_ALL_CLEAN', all_clean: true, entities_analyzed: 5 } },
    }));
    expect(c.No_Abnormal_Data_Message).toContain('Analysis completed');
    expect(c.No_Abnormal_Data_Message).toContain('within the configured thresholds');
  });

  it('Viavi NO_DATA shows a data-availability message (not "no abnormal KPI")', () => {
    const c = makeComponent();
    setSystem(c, 'viavi', 'COMPLETED');
    c.Select_Abnormal_System = 'Viavi';
    (c as any).applyDetails(jobWithDetails({
      abnormal: { npp: [], splunk: [], viavi: [] },
      states: { viavi: { state: 'NO_DATA', all_clean: false } },
    }));
    expect(c.No_Abnormal_Data_Message).toContain('no usable data');
  });

  it('Cells popup with no affected cells shows the no-data state', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    (c as any).applyDetails(jobWithDetails({ npp_data: { affected_cells_details: {}, affected_metrics: {} } }));
    c.openModal('cells');
    expect(c.cellDetailRows.length).toBe(0);
    expect(c.modalNoData).toBeTrue();
  });

  it('Cities/Hubs/TAC-LAC omitted (unavailable) when master sheet did not match', () => {
    // Backend omits these when matched_sites_count == 0 -> card must be unavailable,
    // never a misleading 0.
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    // dashboard has NO citiesSpanned/hubsAffected/tacsLacsAffected keys.
    c.dashboard = { cellsAffected: 82, totalCells: 82 };
    expect(c.cardState('citiesSpanned')).toBe('unavailable');
    expect(c.cardState('hubsAffected')).toBe('unavailable');
    expect(c.cardState('tacsLacsAffected')).toBe('unavailable');
    // Cells still works.
    expect(c.cardState('cellsAffected')).toBe('loaded');
    expect(c.cellsAffectedText).toBe('82/82');
  });
});