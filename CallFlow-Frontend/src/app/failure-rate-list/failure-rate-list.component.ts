import { Component , Input } from '@angular/core';
import { FailureRateData } from '../progress-bar/progress-bar.component';

@Component({
  selector: 'app-failure-rate-list',
  templateUrl: './failure-rate-list.component.html',
  styleUrl: './failure-rate-list.component.css'
})
export class FailureRateListComponent {
  @Input() title: string = 'Failure Rate by Cell';
  @Input() failureRates: FailureRateData[] = [];
}
