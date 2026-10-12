# DB/API connections (Smartcare, Viavi, etc.)
import jaydebeapi
from django.conf import settings
from ..constants.configs import  jdbc_driver_jar,  jdbc_driver_class, jdbc_url
import  cx_Oracle
from rest_framework.response import Response
from rest_framework import status
import logging
import jpype

logger = logging.getLogger(__name__)

#____________________________________  Smartcare INTEGRATION ______________________________________#
def smartcareConnection(logs_file):


  connection_properties = {
      "user": settings.SMARTCARE_DB_USER,
      "password": settings.SMARTCARE_DB_PASSWORD,
      "socketTimeout": "1800000",
      "connectTimeout": "60000",
      
  }

  conn = None
  try:
    logger.info("Trying to connect to smartcare database....")
    if not jpype.isJVMStarted():
      logger.info("[SMARTCARE] Starting JVM with driver JAR: %s", jdbc_driver_jar)
      jpype.startJVM(
          jpype.getDefaultJVMPath(),
          f"-Djava.class.path={jdbc_driver_jar}",
          # "-Dsun.net.client.defaultConnectTimeout=600000",
          # "-Dsun.net.client.defaultReadTimeout=600000"
      )
    conn = jaydebeapi.connect(
      jdbc_driver_class,
      jdbc_url,
      connection_properties,
      jdbc_driver_jar,
      # connection_properties 
    )
    try:
        conn.jconn.setNetworkTimeout(None, 1800000)  
        logger.info("[SMARTCARE] setNetworkTimeout applied successfully")
    except Exception as timeout_err:
        logger.warning(f"[SMARTCARE] could not apply setNetworkTimeout: {timeout_err}")

    return conn
  except Exception as e:
        msg = f"Smartcare connection failed: {str(e)}"
        logger.error(msg, exc_info=True)
        with open(logs_file, 'a') as f:
            f.write(msg + '\n')
        raise RuntimeError(msg)




#____________________________________  VIAVE INTEGRATION ______________________________________#
def viaviConnection(logs_file):
  
  viavi_conn = None
  try:
    dsn_tns = cx_Oracle.makedsn(settings.VIAVE_IP, settings.VIAVE_PORT, service_name=settings.VIAVE_SERVICE_NAME)
    viavi_conn = cx_Oracle.connect(user=settings.VIAVE_USER, password=settings.VIAVE_PASSWORD, dsn=dsn_tns)
    if not viavi_conn:
        raise Exception("cx_Oracle.connect returned None")

    logger.info("Viavi connection successful")
    return viavi_conn
  except Exception as e:
    error_msg = f"Viavi connection failed: {str(e)}"
    logger.error(error_msg, exc_info=True)
    with open(logs_file, 'a') as logs_f:
        logs_f.write(error_msg + '\n')
    raise RuntimeError(error_msg)


def Eniq_Connection():

  Eniq_USER = "E2PErf"
  Eniq_Pass = "Voda@5050"
  JDBC_URL_Eniq = "jdbc:yacht://10.21.85.94:2642/dwhdb"
  jdbc_driver_jar_eniq = "/usr/local/drivers/yacht-jdbc-2.18.36.jar"
  connection_properties = {
      "user": Eniq_USER,
      "password": Eniq_Pass,
      "ssl": "false",
      "useSSL": "false",
      "encrypt": "false",
      "secure": "false",
  }

  conn = None
  try:
    logger.info("Trying to connect to eniq database....")
    if not jpype.isJVMStarted():
      logger.info("[eniq] Starting JVM with driver JAR: %s", jdbc_driver_jar_eniq)
      jpype.startJVM(
          jpype.getDefaultJVMPath(),
          f"-Djava.class.path={jdbc_driver_jar_eniq}",
          "-Dsun.net.client.defaultConnectTimeout=600000",
          "-Dsun.net.client.defaultReadTimeout=600000"
      )
    conn = jaydebeapi.connect(
      jdbc_driver_class,
      JDBC_URL_Eniq,
      connection_properties,
      jdbc_driver_jar_eniq,
      # connection_properties 
  )
    return conn
  except Exception as e:
        msg = f"Eniq connection failed: {str(e)}"
        logger.error(msg, exc_info=True)
        

