import { Injectable } from '@angular/core';
import { CanActivate, Router, UrlTree, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { AuthService } from './auth.service';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {

  constructor(private authService: AuthService, private router: Router , private http: HttpClient,) {}

  canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): boolean | UrlTree {
    const user = this.readUser();
    // Safe when nobody is logged in in this tab (user = {}): no crash, empty groups.
    const userGroups: any[] = user?.user_details?.user_groups || [];

    // Check if user belongs to a specific group
    if (userGroups.some((group: any) => group?.group === 'CallFlow')) {
      this.UserLogs(user);
      return true;
    }

    // For other users, check route-specific allowed groups
    const allowedGroups = route.data?.['allowedGroups'] as string[] | undefined;
    if (allowedGroups && userGroups.some((group: any) => allowedGroups.includes(group?.group))) {
      return true;
    }

    // Not logged in / not allowed: go to login and remember where the user
    // was going (e.g. an NCI link /summary?mode=nci&sites=...).
    return this.router.createUrlTree([''], { queryParams: { returnUrl: state.url } });
  }

  /**
   * The logged-in user of THIS tab (sessionStorage). sessionStorage is per
   * tab, so a link opened in a new tab (e.g. the NCI portal_url) starts empty;
   * in that case use the login saved for this browser (localStorage) and copy
   * it into this tab, so the rest of the app (which reads sessionStorage)
   * works unchanged. Same user object, same group checks as before.
   */
  private readUser(): any {
    const parse = (raw: string | null): any => {
      try { return raw ? JSON.parse(raw) : null; } catch { return null; }
    };
    const tabUser = parse(sessionStorage.getItem('user'));
    if (tabUser?.user_details) return tabUser;

    const saved = localStorage.getItem('user');
    const savedUser = parse(saved);
    if (savedUser?.user_details && saved) {
      sessionStorage.setItem('user', saved);   // restore this tab's session
      return savedUser;
    }
    return {};
  }

  UserLogs(user: any): void {
    console.log('user_id_check', user?.user_details?.user?.id)
    // this.http.post(this.apiUrl, { user: user.user_details.user.id }).subscribe(response => {
    //     // Handle the response
    // });
  }

}
