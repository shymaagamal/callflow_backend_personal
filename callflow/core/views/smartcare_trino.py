import io
import hashlib
import os
import re
import tempfile
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
import pandas as pd
import numpy as np
from ..utils.trino_client import run_trino_query
from datetime import datetime, timezone
from ..utils.mapping_cell_name import load_site_phy_to_cid_lac, get_4g_site_names , get_2g3g_site_names,map_cell_ids

from datetime import datetime , timedelta
from rest_framework import status
from django.conf import settings


from django.http import HttpResponse
import logging

logger = logging.getLogger(__name__)


class smartcare_trino(APIView):

     def post(self, request, *args, **kwargs):
        """Test Trino connection and query."""

        logger.info("Trino Request")

        DATE_FMT = "%Y-%m-%d %H:%M"

        msisdn_list = request.data.get("msisdn", [])
        print('we are starting----')
        # ---- Date range validation ----
        start_date = request.data.get("start_date")
        end_date = request.data.get("end_date")

        try:
                start_dt = datetime.strptime(start_date, DATE_FMT).replace(tzinfo=timezone.utc)
                end_dt = datetime.strptime(end_date, DATE_FMT).replace(tzinfo=timezone.utc)
        except ValueError:
            return Response(
                {"error": f"Invalid date format, expected '{DATE_FMT}'"},
                status=status.HTTP_400_BAD_REQUEST
            )

        if start_dt >= end_dt:
            return Response(
                {"error": "start_date must be before end_date"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Epoch seconds, for the bigint time_unix / calc_time columns
        start_epoch = int(start_dt.timestamp())
        end_epoch = int(end_dt.timestamp())

        # Trino timestamp literals, for the native `datetime` column in npl_ho_4g
        start_ts_literal = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_ts_literal = end_dt.strftime("%Y-%m-%d %H:%M:%S")


        if not msisdn_list or not isinstance(msisdn_list, list):
            return Response(
                {"error": "msisdn must be a non-empty list"},
                status=status.HTTP_400_BAD_REQUEST
            )

        safe_msisdns = [re.sub(r"[^0-9]", "", str(m)) for m in msisdn_list]
        safe_msisdns = [m for m in safe_msisdns if m]  # drop empties

        if not safe_msisdns:
            return Response(
                {"error": "No valid msisdn values provided"},
                status=status.HTTP_400_BAD_REQUEST
            )

        in_clause = ", ".join(f"'{m}'" for m in safe_msisdns)

        try:
            df = run_trino_query(
        f"""SELECT
            DATE_FORMAT(FROM_UNIXTIME(CAST(time_unix AS DOUBLE)), '%Y/%m/%d %H:%i') AS myday,
            MSISDN,
            Site_name AS cell_ID,
            lac_tac_dec
            FROM hive.smartcare.npl_4g
            WHERE MSISDN IN ({in_clause})
            AND CAST(time_unix AS DOUBLE) >= {start_epoch}
            AND CAST(time_unix AS DOUBLE) < {end_epoch}
        UNION ALL
            SELECT
            DATE_FORMAT(FROM_UNIXTIME(CAST(time_unix AS DOUBLE)), '%Y/%m/%d %H:%i') AS myday,
            MSISDN,
            Site_name AS cell_ID,
            lac_tac_dec
            FROM iceberg.smartcare_iceberg.npl_attach_4g
            WHERE MSISDN IN ({in_clause})
            AND CAST(time_unix AS DOUBLE) >= {start_epoch}
            AND CAST(time_unix AS DOUBLE) < {end_epoch}
        UNION ALL
            SELECT
                DATE_FORMAT(FROM_UNIXTIME(CAST(calc_time AS DOUBLE)), '%Y/%m/%d %H:%i') AS myday,
                msisdn,
                sai_cgi_ecgi AS cell_ID,
                NULL AS lac_tac_dec
            FROM iceberg.smartcare_iceberg.npl_ho_4g
            WHERE msisdn IN ({in_clause}) AND datetime >= TIMESTAMP '{start_ts_literal}'
            AND datetime < TIMESTAMP '{end_ts_literal}'"""
            )

            if not df.empty:
                df['cell_name'] = map_cell_ids(df['cell_ID'].tolist())

            else:
                df['cell_name'] = pd.Series(dtype='object')

            df.to_excel(settings.OUTPUT_DIR / f"sc_trino_cell__filter_{start_epoch}.xlsx", index=False)

        except Exception as exc:
            logger.error("Trino query failed", exc_info=True)
            return Response(
                {"error": "Database query failed"},
                status=500
            )

        logger.info(
            f"Query results → {len(df):,} rows"
        )

        return Response(
            {
                "count": len(df),
                "data": df.to_dict(orient="records")
            },
            status=200
        )