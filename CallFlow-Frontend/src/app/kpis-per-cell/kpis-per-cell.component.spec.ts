import { ComponentFixture, TestBed } from '@angular/core/testing';

import { KpisPerCellComponent } from './kpis-per-cell.component';

describe('KpisPerCellComponent', () => {
  let component: KpisPerCellComponent;
  let fixture: ComponentFixture<KpisPerCellComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [KpisPerCellComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(KpisPerCellComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
