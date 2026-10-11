import { TestBed } from '@angular/core/testing';

import { JobPollingService } from './job-polling.service';

describe('JobPollingService', () => {
  let service: JobPollingService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(JobPollingService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
