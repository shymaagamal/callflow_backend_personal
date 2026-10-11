# Constants (mediation_address, jdbc_url, etc.)

from .general_constants import MACHINE, REMOTE_MACHINE

jdbc_driver_jar = ''
#SmartCare
if MACHINE == REMOTE_MACHINE:
  jdbc_driver_jar = "/usr/local/drivers/yacht-jdbc-2.18.36.jar"

jdbc_driver_class = "com.huawei.ryoma.yacht.client.YachtDriver"  
jdbc_url = ("jdbc:yacht://172.30.109.95:38443/SPARK-SPARKSQL"
            "?TIMEOUT=1800000"
            "&connectTimeout=1800000"
            "&readTimeout=1800000"
            "&writeTimeout=1800000"
            "&callTimeout=1800000"
            "&queryTimeOut=2000"
            "&loginTimeout=900")

