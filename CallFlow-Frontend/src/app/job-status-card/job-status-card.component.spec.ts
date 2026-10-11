import { ComponentFixture, TestBed } from '@angular/core/testing';

import { JobStatusCardComponent } from './job-status-card.component';

describe('JobStatusCardComponent', () => {
  let component: JobStatusCardComponent;
  let fixture: ComponentFixture<JobStatusCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [JobStatusCardComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(JobStatusCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
