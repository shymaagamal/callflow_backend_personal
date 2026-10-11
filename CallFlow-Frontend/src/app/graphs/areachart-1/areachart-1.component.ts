import { Component, Input, OnInit, ElementRef, AfterViewInit, OnChanges, SimpleChanges } from '@angular/core';
import { Chart } from 'chart.js/auto';

@Component({
  selector: 'app-areachart-1',
  templateUrl: './areachart-1.component.html',
  styleUrl: './areachart-1.component.css'
})

export class Areachart1Component {

  @Input() chartData: any ;
  @Input() chartTitle: string = '';
  @Input() yAxisLabel: string = '';
  @Input() chartType: any = 'line';
  @Input() minY: number = 0;
  @Input() maxY: number = 20000;
  @Input() unit: string = ''
  @Input() timeUnit: any = 'minute'
  
  @Input() useColorMap: boolean = true
  @Input() tension: number = 0.4

  
  @Input() colors: any = ['#551a4b',  '	#2a5d5c', 'rgb(110, 101, 8)',  'rgb(235, 16, 198)' ];



  private chartInstance: any;
  private ctx: any;

  
  constructor(private elementRef: ElementRef) {}

  ngOnInit(): void {}

  ngAfterViewInit(): void {
    this.ctx = this.elementRef.nativeElement.querySelector('canvas');
    this.createChart();
  }

  ngOnChanges(changes: SimpleChanges): void {
  
      this.updateChart();
    
  }

  private createChart(): void {
    if (this.chartInstance ) {
      this.chartInstance.destroy();
    }
    

    this.chartInstance = new Chart(this.ctx, {
      type: this.chartType,
      data: {
        datasets: this.chartData.map((series: { name: string; data: any }) => ({
          
          label: series.name,
          data: series.data,
          tension: this.tension,
          pointRadius: 2,              
          borderWidth: 2.5, 
          backgroundColor: this.colors,
          borderColor: this.colors,      
          fill: false,
          
          pointBackgroundColor: '#000',   // dot fill
          pointBorderColor: '#686666ff',               // white border around dot
          pointHoverBackgroundColor: '#f30e0eff',
          pointHoverBorderColor: '#FFF',
                  })),
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        //height: 400, // Set the height here (in pixels)
        plugins: {
          title: {
            display: true,
            text: this.chartTitle,
            font: { size: 16, weight: 'bold' },
            color: '#000',
            align: 'start',
            padding: { bottom: 10 },
          },
          legend: {
            display: true,
            position: 'bottom',
      
            labels: {
              color: '#333',
              font: { size: 12, weight: 'bold' },
            boxWidth: 14}, 
            },
          
          tooltip: {
            callbacks: {
              label: (context: any) => {
                const yValue = Math.round(context.raw.y);
                let tooltipContent = [`${context.dataset.label}:`, `${yValue} `];
                return tooltipContent;
              },
            },
          },
        },
        scales: {x: {
        type: 'time',
        time: {
          //parser: 'yyyy/MM/dd HH:mm:ss',
          unit: this.timeUnit,
          tooltipFormat: 'MMM d HH:mm',
          displayFormats: {
            minute: 'HH:mm',
            hour: 'HH:mm',
            day: 'MMM d',
          },
        },
        ticks: {
          color: '#555',
          font: { size: 10 },
          maxRotation: 45,
          autoSkip: true
        }, 
        grid: {
        display: true,
        color: '#ddd',          
      },
        border: {
          color: '#777',
      }
    },

          y: {
            ticks: { color: '#555', font: { size: 11 } },
            grid: { display: true, color: '#f4f5f7' },

      
            title: {
              display: true,
              text: this.yAxisLabel,
              color: '#464565',
              font: { size: 14, weight: 'bold' },
            },           
            min: this.minY,   
            ...(this.chartTitle === 'Discrepency' ? { max: this.maxY } : {})
          },
        },
      },
    });
  }    
  

  private updateChart(): void {
    this.createChart();  // Recreate chart with updated filter value
  }
  
}


