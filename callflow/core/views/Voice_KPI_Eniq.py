import numpy as np
from rest_framework.views import APIView 
from rest_framework.response import Response 
from rest_framework import status 
from ..utils.connections import EniqConnection

from ..utils.data_utils import process_sitename ,convert_java_types, safe_unique
import pandas as pd

class SybaseQueryView(APIView): 
    """ 
    Example endpoint that queries Sybase. 
    GET /api/sybase/query/?table=your_table&limit=50 
    """ 
    def get(self, request): 
        hours = int(request.query_params.get("hours", 1))  # Default to 1 hour if not provided
        query = f"""SELECT
            D1."BLADESYSTEMID" AS "BLADESYSTEMID",
            D1."DATETIME_ID" AS "DATETIMEID",
            D1."DNPREFIX" AS "DNPREFIX",
            D1."Identifier" AS "IDENTIFIER",
            D1."MOID" AS "MOID",
            D1."NE_ID" AS "NEID",
            D1."NEDN" AS "NEDN",
            D1."OBJ_ID" AS "OBJID",
            D1."sbgSipChallengedIncInvSessions" AS "SBGSIPCHALLENGEDINCINVSESSIO",
            D1."sbgSipIngressInitRegThrottlingRej" AS "SBGSIPINGRESSINITREGTHROTTLI",
            D1."sbgSipInitiatedSessionRelease" AS "SBGSIPINITIATEDSESSIONRELEAS",
            D1."sbgSipInitRegThrottlingRejectedCnt" AS "SBGSIPINITREGTHROTTLINGREJEC",
            D1."sbgSipNetIncSessionEstabNetworkSuccess" AS "SBGSIPNETINCSESSIONESTABNETW",
            D1."sbgSipNetIncSessionSetupTime" AS "SBGSIPNETINCSESSIONSETUPTIME",
            D1."sbgSipNetIncSessionSetupUserSuccess" AS "SBGSIPNETINCSESSIONSETUPUSER",
            D1."sbgSipNetOutSessionEstabNetworkSuccess" AS "SBGSIPNETOUTSESSIONESTABNETW",
            D1."sbgSipNetOutSessionSetupTime" AS "SBGSIPNETOUTSESSIONSETUPTIME",
            D1."sbgSipNetOutSessionSetupUserSuccess" AS "SBGSIPNETOUTSESSIONSETUPUSER",
            D1."sbgSipRejectedIncAlertingSessions488" AS "SBGSIPREJECTEDINCALERTINGSES",
            D1."sbgSipRejectedIncPreAlertingSessions488" AS "SBGSIPREJECTEDINCPREALERTING",
            D1."sbgSipSessionIncNormalRelease" AS "SBGSIPSESSIONINCNORMALRELEAS",
            D1."sbgSipSessionOutNormalRelease" AS "SBGSIPSESSIONOUTNORMALRELEAS",
            D1."sbgSipSuccessIncSessions" AS "SBGSIPSUCCESSINCSESSIONS",
            D1."sbgSipTotalIncSessSetups" AS "SBGSIPTOTALINCSESSSETUPS",
            D1."sbgSipTotalOutSessSetups" AS "SBGSIPTOTALOUTSESSSETUPS"
            FROM
            dcpublic.DC_E_IMSSBG_SIP_RAW D1
            WHERE
            D1."DATETIME_ID">= dateadd(hour, -{hours}, getdate())""" 
        conn = None 
        cursor = None 
        try: 
            conn = EniqConnection() 
            print("Connection established successfully.")
            cursor = conn.cursor() 
            # Example query – adjust as needed 

            cursor.execute(query ) 
            columns = [str(desc[0]) for desc in cursor.description] 
            rows = cursor.fetchall() 
            df = pd.DataFrame(rows, columns=columns)
            df = convert_java_types(df)
            df = df.replace({pd.NA: None, pd.NaT: None, np.nan: None})  # Replace NaN/NaT with None
            data = df.to_dict(orient="records")
            return Response({ 
                "hours": hours, 
                "count": len(data),
                "results": data 
            }) 
        except Exception as e: 
            return Response( 
                {"error": str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR 
            ) 
        finally: 
            if cursor: 
                cursor.close() 
            if conn: 
                conn.close() 