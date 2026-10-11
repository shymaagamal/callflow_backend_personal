import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Router } from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  private apiUrl = 'https://10.30.30.177/ac/ldap/authenticate/'
  // private apiUrl = 'https://dataintelligence-apps.internal.eg.vodafone.com/ac/ldap/authenticate/';


  constructor(private http: HttpClient, private router: Router) {}

  authenticate(username: string, password: string): Observable<any> {
    return this.http.post(this.apiUrl, { username, password });
  }
  isAuthenticated(): boolean {
    console.log('check data',localStorage.getItem('user'))
    return !!localStorage.getItem('user'); 
    
  }
  hasGroup(groupName: string): boolean {
  try {
    const raw = localStorage.getItem('user');
    if (!raw) return false;

    const user = JSON.parse(raw);
    const groups: any[] = user?.user_details?.user_groups || [];

    return groups.some(g => {
      const groupStr = typeof g === 'string' ? g : g?.group;
      return groupStr === groupName;
    });
  } catch {
    return false;
  }
}
  logout() {
    // Remove session data
    sessionStorage.removeItem('isLoggedIn');
    sessionStorage.removeItem('user');
    this.router.navigate(['']);
  }


}
