import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UserInputFormComponent } from './user-input-form.component';

describe('UserInputFormComponent', () => {
  let component: UserInputFormComponent;
  let fixture: ComponentFixture<UserInputFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserInputFormComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(UserInputFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
