import { Component } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { AuthService } from '../auth.service';
import { HttpClient } from '@angular/common/http';
import { ALLOWED_GROUPS } from '../shared/constant';


@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrl: './login.component.css'
})
export class LoginComponent {

  username: string = '';
  password: string = '';
  errorMessage: string = '';

  constructor(private authService: AuthService, private router: Router, private route: ActivatedRoute) {}

  login() {
    console.log('Login button clicked');
    this.authService.authenticate(this.username, this.password)
      .subscribe(
        (response: any) => {
          console.log('Authentication successful');
          if (response.message === "Authentication successful") {
            console.log('Before Store Date');
            this.storeUserData(response);
            console.log('After Store Data');
            
            const userData = {
              first_name: response.first_name,
              last_name: response.last_name,
              title: response.title
            };
          }
        },
        (error) => {
          console.log('Error',error)
          console.log('Wrong username or password');
          this.errorMessage = "Wrong username or password";
        }
      );
  }

  storeUserData(response: any) {
    sessionStorage.setItem('user', JSON.stringify(response));
    localStorage.setItem('user', JSON.stringify(response));
    console.log("local storge",localStorage.getItem('user'));
    this.authorizeUser(response.user_details.user_groups || []);
    console.log(response.user_details.user_groups);
  }


  authorizeUser(userGroups: any) {
  
    if (Array.isArray(userGroups) && userGroups.some((group: any) => ALLOWED_GROUPS.includes(group.group))) {
      
      console.log('In the redirect condition');

      // Go back to the page the user originally asked for (e.g. an NCI
      // portal_url /summary?mode=nci&sites=...), otherwise the normal start page.
      const returnUrl = this.safeReturnUrl();
      const navigation = returnUrl
        ? this.router.navigateByUrl(returnUrl)
        : this.router.navigate(['user-input-form']);

      navigation.then(success => console.log('Navigation Success:', success))
      .catch(error => console.error('Navigation Error:', error));
    } else {
      console.log('Unauthorized access');
      this.errorMessage = "Unauthorized access";
    }
  }

  /** Only in-app paths ("/summary?..."), never another site ("//evil.com", "https://..."). */
  private safeReturnUrl(): string | null {
    const url = this.route.snapshot.queryParamMap.get('returnUrl');
    return url && url.startsWith('/') && !url.startsWith('//') ? url : null;
  }
}