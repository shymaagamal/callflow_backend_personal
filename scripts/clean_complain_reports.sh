#!/bin/bash

DIR="/home/CallFlow/BO/complaint_ticket_location"
LOG="/home/AutomationProjects/CallFlowTool/backend/callflow_portal/callflow_output/logs/clean_complain_reports.log"

mkdir -p "$(dirname "$LOG")"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') - Starting cleanup of $DIR ===" >> "$LOG"

cd "$DIR" || {
    echo "ERROR: Cannot access directory $DIR" | tee -a "$LOG"
    exit 1
}

excel_count=$(find . -maxdepth 1 -type f \( -name "*.xlsx" -o -name "*.xls" -o -name "*.xlsm" \) | wc -l)

if [ "$excel_count" -le 1 ]; then
    echo "INFO: Only $excel_count Excel file(s) found. Nothing to delete." | tee -a "$LOG"
    exit 0
fi

echo "INFO: Found $excel_count Excel files. Keeping the newest one and deleting the rest." | tee -a "$LOG"

find . -maxdepth 1 -type f \( -name "*.xlsx" -o -name "*.xls" -o -name "*.xlsm" \) \
  -printf '%T@ %p\n' | sort -nr | tail -n +2 | cut -d' ' -f2- | xargs -r rm -v >> "$LOG" 2>&1

echo "INFO: Cleanup completed successfully." | tee -a "$LOG"