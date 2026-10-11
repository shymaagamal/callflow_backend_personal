import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'join'
})
export class JoinPipe implements PipeTransform {
  transform(value: any[] | null | undefined, separator: string = ', '): string {
    if (!Array.isArray(value)) {
      return 'N/A';
    }
    return value.length > 0 ? value.join(separator) : 'N/A';
  }
}