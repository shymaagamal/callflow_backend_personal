import { TestBed } from '@angular/core/testing';

import { SharedinfoService } from './sharedinfo.service';

describe('SharedinfoService', () => {
  let service: SharedinfoService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SharedinfoService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
