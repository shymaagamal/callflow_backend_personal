import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HeaderComponent } from './dashboard/dashboard.component'
import { UserInputFormComponent } from './user-input-form/user-input-form.component';
import { SummaryComponent } from './summary/summary.component';
import { CorrelationsComponent } from './correlations/correlations.component';
import { RootcauseComponent } from './rootcause/rootcause.component';
import { LoginComponent } from './login/login.component';
import { AuthGuard } from './auth.guard';
import { ALLOWED_GROUPS } from './shared/constant';

const routes: Routes =  [
  // { path: '', redirectTo: 'user-input-form', pathMatch:'full'},
  // { path: 'main', component: HeaderComponent},
  {path:'', component: LoginComponent ,data: { title: 'Login Page' }},
  {path:'login', component: LoginComponent ,data: { title: 'Login Page' }},
  { path: 'user-input-form', component: UserInputFormComponent,canActivate: [AuthGuard] ,data: { title: 'user-input-form ' ,allowedGroups:ALLOWED_GROUPS}},
  { path: 'summary', component: SummaryComponent ,canActivate: [AuthGuard] ,data: { title: 'summary ' ,allowedGroups:ALLOWED_GROUPS}},
  { path: 'Correlation', component: CorrelationsComponent ,canActivate: [AuthGuard] ,data: { title: 'Correlation' ,allowedGroups:ALLOWED_GROUPS}},
  { path:'Root-Cause' , component:RootcauseComponent,canActivate: [AuthGuard] ,data: { title: 'Root-Cause' ,allowedGroups:ALLOWED_GROUPS}}

];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
