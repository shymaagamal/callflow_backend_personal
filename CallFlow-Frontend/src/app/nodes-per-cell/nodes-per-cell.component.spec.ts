import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NodesPerCellComponent } from './nodes-per-cell.component';

describe('NodesPerCellComponent', () => {
  let component: NodesPerCellComponent;
  let fixture: ComponentFixture<NodesPerCellComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NodesPerCellComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(NodesPerCellComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
