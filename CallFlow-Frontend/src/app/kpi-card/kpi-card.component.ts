import { Component, Input } from '@angular/core';
import { KpiItem } from '../shared/kpi-line-chart/kpi-line-chart.component';

@Component({
  selector: 'app-kpi-card',
  templateUrl: './kpi-card.component.html',
  styleUrl: './kpi-card.component.css'
})
export class KpiCardComponent {
  @Input() kpi!: KpiItem;
@Input() index!: number;

}
