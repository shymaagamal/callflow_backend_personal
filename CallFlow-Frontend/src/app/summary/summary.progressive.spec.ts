/**
 * Progressive Summary readiness: cards driven by the backend per-card `summary`
 * (one source of truth with RCA). Verifies a real empty result (0) is distinct
 * from "still pending" and "failed", and that cards do not wait for the whole
 * job / for Smartcare.
 */
import { SummaryComponent } from './summary.component';

function makeComponent() {
  const cdr: any = { detectChanges: () => {} };
  const c = new SummaryComponent({} as any, {} as any, {} as any, cdr);
  c.request_id = 'REQ-1';
  return c;
}

describe('SummaryComponent progressive per-card readiness', () => {
  it('Cells Affected shows a spinner (loading) while NPP analysis is PENDING', () => {
    const c = makeComponent();
    c.summaryCards = { affected_cells: { status: 'PENDING' } };
    expect(c.cardState('cellsAffected')).toBe('loading');
    expect(c.cardLoading('cellsAffected')).toBeTrue();
  });

  it('Cells Affected shows the real count when READY_WITH_DATA', () => {
    const c = makeComponent();
    c.summaryCards = { affected_cells: { status: 'READY_WITH_DATA', value: 8 } };
    c.dashboard = { totalCells: 41 };
    expect(c.cardState('cellsAffected')).toBe('loaded');
    expect(c.cellsAffectedText).toBe('8/41');
  });

  it('Cells Affected shows a real 0 when READY_EMPTY (not unavailable)', () => {
    const c = makeComponent();
    c.summaryCards = { affected_cells: { status: 'READY_EMPTY', value: 0 } };
    c.dashboard = { totalCells: 41 };
    expect(c.cardState('cellsAffected')).toBe('loaded');
    expect(c.cellsAffectedText).toBe('0/41');
  });

  it('Cells Affected is unavailable when NPP analysis FAILED', () => {
    const c = makeComponent();
    c.summaryCards = { affected_cells: { status: 'FAILED' } };
    expect(c.cardState('cellsAffected')).toBe('unavailable');
    expect(c.cardUnavailable('cellsAffected')).toBeTrue();
  });

  it('Cells Affected does NOT wait for Smartcare', () => {
    const c = makeComponent();
    // NPP analysis ready, Smartcare still processing -> card is loaded already.
    c.summaryCards = { affected_cells: { status: 'READY_WITH_DATA', value: 7 },
                       smartcare: { status: 'PENDING' } };
    c.systemRows = c.systemRows.map((r) =>
      r.key === 'smartcare' ? { ...r, status: 'PROCESSING' as any } : r);
    expect(c.cardState('cellsAffected')).toBe('loaded');
  });

  it('Tickets card is driven by remedy readiness independently of NPP', () => {
    const c = makeComponent();
    c.summaryCards = { tickets: { status: 'READY_WITH_DATA', value: 3 },
                       affected_cells: { status: 'PENDING' } };
    expect(c.cardState('remedyAffected')).toBe('loaded');
    expect(c.cardValue('remedyAffected')).toBe(3);
  });

  it('backend summary readiness overrides the legacy system-status fallback', () => {
    const c = makeComponent();
    // Legacy path would call this loading (npp PENDING), but the backend says
    // the affected-cells analysis already completed empty -> show the real 0.
    c.summaryCards = { affected_cells: { status: 'READY_EMPTY', value: 0 } };
    expect(c.cardState('cellsAffected')).toBe('loaded');
  });

  it('falls back to legacy system-status logic when no summary is present', () => {
    const c = makeComponent();
    // No summaryCards -> behaves exactly as before (npp running -> loading).
    c.systemRows = c.systemRows.map((r) =>
      r.key === 'npp' ? { ...r, status: 'PROCESSING' as any } : r);
    expect(c.cardState('cellsAffected')).toBe('loading');
  });
});
