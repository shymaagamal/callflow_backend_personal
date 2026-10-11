import { ComponentFixture, TestBed } from '@angular/core/testing';

import { Areachart1Component } from './areachart-1.component';

describe('Areachart1Component', () => {
  let component: Areachart1Component;
  let fixture: ComponentFixture<Areachart1Component>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [Areachart1Component]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(Areachart1Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
