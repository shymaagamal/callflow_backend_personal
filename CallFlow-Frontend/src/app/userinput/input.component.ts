import { Component, OnInit } from '@angular/core';
import { NgForm } from '@angular/forms';
import 'bootstrap';

@Component({
  selector: 'app-input',
  templateUrl: './input.component.html',
  styleUrl: './input.component.css'
})
export class InputComponent {
  selectedFile: File | null = null;

  userinput = {
    // msisdn: '',
    imsi: '',
    start_date: '',
    end_date: ''
  }


  onFileSelected(event: any) {
  this.selectedFile = event.target.files[0];
}
onSubmit(form: NgForm){
 
//  this.userinput.msisdn = form.value.msisdn
 this.userinput.imsi = form.value.imsi
 this.userinput.start_date = form.value.startDate
 this.userinput.end_date = form.value.endDate

  console.log("Selected file:", this.selectedFile);
  console.log("User input:", this.userinput);
//  console.log(this.userinput)

  }

}
