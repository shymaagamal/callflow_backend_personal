import { Component, ViewChild, ElementRef, OnInit, OnChanges, SimpleChanges, AfterViewInit, ViewEncapsulation, Input } from '@angular/core';
import { DomSanitizer, SafeResourceUrl } from '@angular/platform-browser';

@Component({
  selector: 'app-rootcause',
  templateUrl: './rootcause.component.html',
  styleUrl: './rootcause.component.css',
  encapsulation: ViewEncapsulation.None
})
export class RootcauseComponent implements OnInit, AfterViewInit, OnChanges {

  @ViewChild('rcaFrame') rcaFrame!: ElementRef<HTMLIFrameElement>;
  @Input() payload: any;                 // ← comes from SummaryComponent

  rcaUrl: SafeResourceUrl = '';

  private isIframeReady = false;

  constructor(private sanitizer: DomSanitizer) {}

  ngOnInit() {
    this.rcaUrl = this.sanitizer.bypassSecurityTrustResourceUrl(
      'assets/external/rca_tool_V24.html'
    );
  }

  ngAfterViewInit() {
    // Wait until iframe is fully loaded
    this.rcaFrame.nativeElement.onload = () => {
      this.isIframeReady = true;
      console.log('✅ RCA iframe fully loaded');

      if (this.payload) {
        this.sendPayloadToIframe(this.payload);
      }
    };
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes['payload']?.currentValue && this.isIframeReady) {
      this.sendPayloadToIframe(changes['payload'].currentValue);
    }
  }

  private sendPayloadToIframe(payload: any) {
    if (!this.rcaFrame?.nativeElement?.contentWindow) return;

    console.log('🚀 Sending RootCause_Payload to iframe:', payload);

    this.rcaFrame.nativeElement.contentWindow.postMessage({
      type: 'ROOT_CAUSE_PAYLOAD',
      data: payload
    }, '*');   // '*' is fine for local/dev
  }
}