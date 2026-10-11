# callflow/views/base.py
import os

from django.http import JsonResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import datetime
from datetime import timedelta
import logging
from pathlib import Path
import pandas as pd
import pytz
from typing import Tuple, Union, Dict
from django.conf import settings
from dataclasses import dataclass
logger = logging.getLogger(__name__)


TIME_FMT_QUERY = "%m/%d/%Y:%H:%M:%S"
DEFAULT_SHIFT_HOURS = 2
DEFAULT_LOOKBACK_DAYS = (1, 3)
REQUEST_TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def parse_time_window(start_str: str, end_str: str) -> dict:
    """
    time parser for API inputs.
    Returns datetime objects + commonly used string formats.
    Raises ValueError on invalid input.
    """
    def safe_parse(s: str) -> datetime.datetime:
        try:
            return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M')
        except ValueError:
            try:
                return pd.to_datetime(s).to_pydatetime()
            except Exception as e:
                raise ValueError(f"Cannot parse time '{s}': {e}")

    start_dt = safe_parse(start_str)
    end_dt = safe_parse(end_str)

    if start_dt >= end_dt:
        raise ValueError("start_date must be before end_date")

    return {
        'start_dt': start_dt,
        'end_dt': end_dt,
        'formatted_startDate': start_dt.strftime('%Y-%m-%d %I:%M %p'),
        'formatted_endDate': end_dt.strftime('%Y-%m-%d %I:%M %p'),
        'formatted_startDate_3': start_dt.strftime("%m/%d/%Y:%H:%M:%S"),
        'formatted_endDate_3': end_dt.strftime("%m/%d/%Y:%H:%M:%S"),
        'start_hour': str(start_dt.hour),
        'end_hour': str(end_dt.hour),
    }





class BaseCallFlowAPIView(APIView):
    """
    Base class for all CallFlow API views.
    Provides common utilities: time parsing, logging, file handling.
    """
    def parse_request_time_range(self, request_data: dict) -> dict:
        start_str = request_data.get('start_date')
        end_str = request_data.get('end_date')
        if not start_str or not end_str:
            raise ValueError("Both 'start_date' and 'end_date' are required")
        return parse_time_window(start_str, end_str)

    def build_analysis_time_windows(
        self,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime,
        shift_hours: int = DEFAULT_SHIFT_HOURS,
        lookback_days: Tuple[int, ...] = DEFAULT_LOOKBACK_DAYS
    ) -> dict:
        """
        Build analysis time windows:
        - shifted current window
        - previous day windows (1d, 3d by default)
        - query ranges covering current + history
        """
        # --- Current window ---
        current_start = start_dt - timedelta(hours=shift_hours)
        current_end = end_dt
        windows: Dict[str, Union[datetime.datetime, str]]= {
            "current_start": current_start,
            "current_end": current_end,
        }
        for days in lookback_days:
            prev_start = current_start - timedelta(days=days)
            prev_end = current_end - timedelta(days=days)

            query_start = min(current_start, prev_start)
            query_end = max(current_end, prev_end)

            suffix = f"{days}day" if days == 1 else f"{days}days"

            windows.update({
                f"prev_start_{suffix}": prev_start,
                f"prev_end_{suffix}": prev_end,
                f"query_start_{suffix}": query_start,
                f"query_end_{suffix}": query_end,
                f"query_start_{suffix}_str": query_start.strftime(TIME_FMT_QUERY),
                f"query_end_{suffix}_str": query_end.strftime(TIME_FMT_QUERY),
            })

        return windows
    def get_log_file(self, prefix: str,start_date,end_date) -> tuple:
        local_tz = pytz.timezone("Africa/Cairo")
        ts = f"{start_date}_{end_date}"
        logs_dir = settings.OUTPUT_DIR / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / f"{prefix}.txt"
        return log_file , ts

    def log_request(self, log_path: str, request, extra: str = ""):
        user = str(request.user) if request.user.is_authenticated else 'anonymous'
        log_file = Path(log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, 'a') as f:
            f.write(f"\n{datetime.datetime.now():%Y-%m-%d %H:%M:%S} - {extra}\nUser: {user}\n")
    
    