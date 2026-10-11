import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class SharedinfoService {

  constructor() { }
    private payloadData: any;

  setPayload(data: any) {
    this.payloadData = data;
  }

  getPayload() {
    return this.payloadData;
  }
}
