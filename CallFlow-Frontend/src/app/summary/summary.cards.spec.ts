/**
 * Tests for dashboard card state (loading / loaded / unavailable) driven by
 * real task status + data availability -- never dummy values.
 */
import { SummaryComponent } from './summary.component';

function makeComponent() {
  const cdr: any = { detectChanges: () => {} };
  const c = new SummaryComponent({} as any, {} as any, {} as any, cdr);
  c.request_id = 'REQ-1';
  return c;
}

function setSystem(c: SummaryComponent, key: string, status: any) {
  c.systemRows = c.systemRows.map((r) => (r.key === key ? { ...r, status } : r));
}

describe('SummaryComponent dashboard cards', () => {
  it('card is LOADING while its system is still running (no dummy value)', () => {
    const c = makeComponent();
    setSystem(c, 'smartcare', 'PROCESSING');
    expect(c.cardState('handsetTypes')).toBe('loading');
    expect(c.cardLoading('handsetTypes')).toBeTrue();
    // Must NOT show a dummy/default number while waiting.
    expect(c.cardValue('handsetTypes')).toBe('');
  });

  it('card shows the REAL value once the system completed and data arrived', () => {
    const c = makeComponent();
    setSystem(c, 'smartcare', 'COMPLETED');
    c.dashboard = { ...c.dashboard, handsetTypes: 5 };
    expect(c.cardState('handsetTypes')).toBe('loaded');
    expect(c.cardValue('handsetTypes')).toBe(5);
  });

  it('card shows UNAVAILABLE when the system completed but no data is present', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'COMPLETED');
    // no dashboard value for cellsAffected -> unavailable, not "0/45" dummy
    expect(c.cardState('cellsAffected')).toBe('unavailable');
    expect(c.cardUnavailable('cellsAffected')).toBeTrue();
  });

  it('card shows UNAVAILABLE when the system failed', () => {
    const c = makeComponent();
    setSystem(c, 'remedy', 'FAILED');
    expect(c.cardState('remedyAffected')).toBe('unavailable');
  });

  it('different cards transition independently as their systems finish', () => {
    const c = makeComponent();
    // NPP finished with data, Smartcare still running.
    setSystem(c, 'npp', 'COMPLETED');
    setSystem(c, 'smartcare', 'PROCESSING');
    c.dashboard = { ...c.dashboard, cellsAffected: 2, totalCells: 45, citiesSpanned: 3 };

    expect(c.cardState('cellsAffected')).toBe('loaded');
    expect(c.cellsAffectedText).toBe('2/45');
    expect(c.cardState('citiesSpanned')).toBe('loaded');
    // Smartcare-dependent card is still loading.
    expect(c.cardState('handsetTypes')).toBe('loading');

    // Now Smartcare finishes with data -> its card flips to loaded independently.
    setSystem(c, 'smartcare', 'COMPLETED');
    c.dashboard = { ...c.dashboard, handsetTypes: 2, packetCoreNodes: 7 };
    expect(c.cardState('handsetTypes')).toBe('loaded');
    expect(c.cardState('packetCoreNodes')).toBe('loaded');
  });

  it('total complaints (input-derived) loads immediately without a system', () => {
    const c = makeComponent();
    c.dashboard = { totalComplaints: 12 };
    expect(c.cardState('totalComplaints')).toBe('loaded');
    expect(c.cardValue('totalComplaints')).toBe(12);
  });

  it('partial system state (analysis unavailable) marks the card unavailable', () => {
    const c = makeComponent();
    setSystem(c, 'npp', 'PARTIAL'); // data retrieved, analysis unavailable
    expect(c.cardState('cellsAffected')).toBe('unavailable');
  });
});
