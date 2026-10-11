import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FailureRateListComponent } from './failure-rate-list.component';

describe('FailureRateListComponent', () => {
  let component: FailureRateListComponent;
  let fixture: ComponentFixture<FailureRateListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [FailureRateListComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(FailureRateListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
