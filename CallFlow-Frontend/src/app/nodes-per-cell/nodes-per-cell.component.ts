import { Component, Input } from '@angular/core';


export interface NodeItem {
  nodeName: string;
  band: string;
  tac: string;
  cellsCount: number;
  anomaly: number;
  users: number;
}
@Component({
  selector: 'app-nodes-per-cell',
  templateUrl: './nodes-per-cell.component.html',
  styleUrl: './nodes-per-cell.component.css'
})
export class NodesPerCellComponent {
  @Input() title = 'Nodes per Cluster';
  @Input() nodes: NodeItem[] = [];

  selectedFilter = 'cellName';
}
