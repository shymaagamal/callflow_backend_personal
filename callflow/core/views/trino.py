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


from datetime import datetime , timedelta
from rest_framework import status


from django.http import HttpResponse
import logging

logger = logging.getLogger(__name__)


class trino_test(APIView):

     def get(self, request, *args, **kwargs):
        """Test Trino connection and query."""

        logger.info("Trino Request")

        try:
            df = run_trino_query(
                """SELECT
    CAST(time_unix AS BIGINT) AS time_unix,
    MSISDN,
    Site_name AS cell_ID,
    lac_tac_dec
FROM hive.smartcare.npl_4g
UNION ALL
SELECT
    CAST(time_unix AS BIGINT) AS time_unix,
    MSISDN,
    Site_name AS cell_ID,
    lac_tac_dec
FROM iceberg.smartcare_iceberg.npl_attach_4g
UNION ALL
SELECT
    CAST(calc_time AS BIGINT) AS time_unix,
    MSISDN,
    sai_cgi_ecgi AS cell_ID,
    NULL AS lac_tac_dec
FROM iceberg.smartcare_iceberg.npl_ho_4g LIMIT 50"""
            )

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