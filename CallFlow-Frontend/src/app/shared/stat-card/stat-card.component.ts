import { Component, EventEmitter, Input, Output } from '@angular/core';

/**
 * REUSABLE STAT CARD COMPONENT
 * 
 * Purpose: Display a statistic with icon, title, and value
 * 
 * How to use:
 * <app-stat-card 
 *   [title]="'Total Complaints'" 
 *   [value]="156" 
 *   [icon]="'pi-exclamation-triangle'"
 *   [iconColor]="'#9333ea'"
 *   [clickable]="false">
 * </app-stat-card>
 */
@Component({
  selector: 'app-stat-card',
  templateUrl: './stat-card.component.html',
  styleUrl: './stat-card.component.css'
})
export class StatCardComponent {
  
  @Input() title: string = '';          // Card title (e.g., "Total Complaints")
  @Input() value: string | number = ''; // Main value to display (e.g., 156)
  @Input() icon: string = '';           // PrimeIcons icon class (e.g., "pi-users")
  @Input() iconColor: string = '#666';  // Icon background color
  @Input() clickable: boolean = false;  // If true, shows "Click for more info"
  @Input() subtitle: string = '';       // Optional subtitle text
  @Input() width: string = 'auto'; // default
  @Input() fontSize: string = '2.1rem';       // Font size for value (default 2.1rem)
  @Input() titleFontSize: string = '1.1rem';  // Font size for title (default 1.1rem)
  @Input() margin_size: string = '40%';  // Font size for title (default 1.1rem)

  @Input() Correlation_Cards: boolean = false;

  // Data-availability states (never show dummy values as real data).
  @Input() loading: boolean = false;       // underlying task still running
  @Input() unavailable: boolean = false;   // task finished but data failed/missing
  @Input() unavailableText: string = 'Data unavailable';

  @Output() onMoreInfoClick = new EventEmitter<void>();

    handleMoreInfoClick() {
    this.onMoreInfoClick.emit();
  }
}
