import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { CommonModule } from '@angular/common';           // ← Added
import { HttpClientModule } from '@angular/common/http';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';

// Components
import { HeaderComponent } from './dashboard/dashboard.component';
import { InputComponent } from './userinput/input.component';
import { Areachart1Component } from './graphs/areachart-1/areachart-1.component';
import { UserInputFormComponent } from './user-input-form/user-input-form.component';
import { SummaryComponent } from './summary/summary.component';
import { StatCardComponent } from './shared/stat-card/stat-card.component';
import { BarChartComponent } from './shared/bar-chart/bar-chart.component';
import { CorrelationsComponent } from './correlations/correlations.component';
import { ProgressBarComponent } from './progress-bar/progress-bar.component';
import { FailureRateListComponent } from './failure-rate-list/failure-rate-list.component';
import { NodesPerCellComponent } from './nodes-per-cell/nodes-per-cell.component';
import { KpiLineChartComponent } from './shared/kpi-line-chart/kpi-line-chart.component';
import { KpiCardComponent } from './kpi-card/kpi-card.component';
import { KpisPerCellComponent } from './kpis-per-cell/kpis-per-cell.component';
import { RootcauseComponent } from './rootcause/rootcause.component';
import { LoginComponent } from './login/login.component';
import { JobStatusCardComponent } from './job-status-card/job-status-card.component';

// PrimeNG
import { ButtonModule } from 'primeng/button';
import { CalendarModule } from 'primeng/calendar';
import { DropdownModule } from 'primeng/dropdown';
import { DialogModule } from 'primeng/dialog';
import { TableModule } from 'primeng/table';
import { InputTextModule } from 'primeng/inputtext';
import { TagModule } from 'primeng/tag';

// Material
import { MatTableModule } from '@angular/material/table';
import { MatPaginatorModule } from '@angular/material/paginator';
import { MatSortModule } from '@angular/material/sort';
import { MatInputModule } from '@angular/material/input';

// Charts
import { NgChartsModule } from 'ng2-charts';
import { NgApexchartsModule } from 'ng-apexcharts';

import { JoinPipe } from './join.pipe';
import 'chartjs-adapter-date-fns';
import { provideAnimationsAsync } from '@angular/platform-browser/animations/async';
import { CUSTOM_ELEMENTS_SCHEMA } from '@angular/core';
import { DownloadButtonComponent } from './download-button/download-button.component';

@NgModule({
  declarations: [
    AppComponent,
    HeaderComponent,
    InputComponent,
    Areachart1Component,
    JoinPipe,
    UserInputFormComponent,
    JobStatusCardComponent,
    SummaryComponent,
    StatCardComponent,
    BarChartComponent,
    CorrelationsComponent,
    ProgressBarComponent,
    FailureRateListComponent,
    NodesPerCellComponent,
    KpiLineChartComponent,
    KpiCardComponent,
    KpisPerCellComponent,
    RootcauseComponent,
    LoginComponent,
    DownloadButtonComponent,
  ],
  imports: [
    BrowserModule,
    CommonModule,                    // ← Added (important for ngClass, etc.)
    AppRoutingModule,
    FormsModule,
    HttpClientModule,

    // Material
    MatTableModule,
    MatPaginatorModule,
    MatSortModule,
    MatInputModule,

    // PrimeNG
    ButtonModule,
    CalendarModule,
    DropdownModule,
    DialogModule,
    TableModule,
    InputTextModule,
    TagModule,

    // Charts
    NgChartsModule,
    NgApexchartsModule,
  ],
  providers: [
    provideAnimationsAsync()
  ],
  bootstrap: [AppComponent],
})
export class AppModule { }