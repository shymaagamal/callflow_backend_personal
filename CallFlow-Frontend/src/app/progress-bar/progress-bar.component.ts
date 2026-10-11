import { Component , Input } from '@angular/core';


export interface FailureRateData {
  clusterName: string;
  location: string;
  failureRate: number;
  users: number;
  cells: number;
  tacs: number;
  anomaly: number;
  highlighted?: boolean;
}
@Component({
  selector: 'app-progress-bar',
  templateUrl: './progress-bar.component.html',
  styleUrl: './progress-bar.component.css'
})
export class ProgressBarComponent {
 @Input() clusterName: string = '';      // Cluster name (e.g., "Downtown Metro")
  @Input() location: string = '';         // Location (e.g., "Cairo")
  @Input() failureRate: number = 0;       // Failure rate percentage (0-100)
  @Input() users: number = 0;             // Number of users
  @Input() cells: number = 0;             // Number of cells
  @Input() tacs: number = 0;              // Number of TACs
  @Input() anomaly: number = 0;           // Anomaly count
  @Input() highlighted: boolean = false;  // If true, shows red border


    /**
   * Get progress bar color based on failure rate
   */
  getProgressBarColor(): string {
    if (this.failureRate >= 15) {
      return '#4440C2'; // Blue/Indigo for high failure rate
    } else if (this.failureRate >= 10) {
      return '#4440C2'; // Lighter blue
    } else {
      return '#4440C2'; // Even lighter blue
    }
  }
    getProgressWidth(): string {
    return `${this.failureRate}%`;
  }
}
