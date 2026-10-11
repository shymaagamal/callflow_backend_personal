import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RootcauseComponent } from './rootcause.component';

describe('RootcauseComponent', () => {
  let component: RootcauseComponent;
  let fixture: ComponentFixture<RootcauseComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RootcauseComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(RootcauseComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
