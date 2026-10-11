/**
 * The Correlations tab renders one chart card per (affected cell x degraded KPI).
 * With many affected cells the tab froze. These tests verify the render cap
 * (reusing already-loaded orchestration data, no extra API calls).
 */
import { CorrelationsComponent } from './correlations.component';

function makeComponent() {
  return new CorrelationsComponent({} as any, {} as any);
}

describe('CorrelationsComponent KPI render cap', () => {
  it('caps the number of KPI cards and reports truncation', () => {
    const c = makeComponent();
    const npp: any[] = [];
    for (let i = 0; i < 50; i++) {
      npp.push({
        cell_name: 'C' + i,
        current_value: i,
        degraded_kpis: [{ name: 'K', current_value: i, kpi_data: [] }],
      });
    }
    c.Abnormal_Kpis_RawData = { npp, splunk: [], viavi: [] };
    c.Select_Abnormal_System = 'NPP';

    const res = c.prepareAbnormalKpiChartData(c.Abnormal_Kpis_RawData);
    expect(res.length).toBe(c.MAX_KPI_CARDS);
    expect(c.totalKpiCount).toBe(50);
    expect(c.truncatedKpiCount).toBe(50 - c.MAX_KPI_CARDS);
    // Highest-value entries kept first (most abnormal).
    expect(res[0].value).toBeGreaterThanOrEqual(res[res.length - 1].value);
  });

  it('does not truncate small datasets', () => {
    const c = makeComponent();
    c.Abnormal_Kpis_RawData = {
      npp: [{ cell_name: 'C1', current_value: 1, degraded_kpis: [{ name: 'K', current_value: 1, kpi_data: [] }] }],
      splunk: [], viavi: [],
    };
    c.Select_Abnormal_System = 'NPP';
    const res = c.prepareAbnormalKpiChartData(c.Abnormal_Kpis_RawData);
    expect(res.length).toBe(1);
    expect(c.truncatedKpiCount).toBe(0);
  });

  it('empty/null data -> empty result (no crash)', () => {
    const c = makeComponent();
    expect(c.prepareAbnormalKpiChartData(null).length).toBe(0);
    c.Select_Abnormal_System = 'NPP';
    expect(c.prepareAbnormalKpiChartData({ npp: [], splunk: [], viavi: [] }).length).toBe(0);
  });
});
