from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from ..utils.connections import Eniq_Connection

class EniqIntegrationAPIView(APIView):
    job_type = "eniq"

    def get(self, request, *args, **kwargs):
        conn = None
        try:
            
            conn = Eniq_Connection()
            if conn:
             print("Success connection")

            return Response(conn, status=status.HTTP_200_OK)

          

        except Exception as e:
            return Response(
                {"error": f"ENIQ connection/query failed: {str(e)}"},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass