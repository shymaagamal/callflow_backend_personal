
from platform import machine
from general_apis import *
from general_apis.constants import  REMOTE_MACHINE, MACHINE

#NBA
mediation_address = '10.230.91.47'
mediation_username = 'NBAMED'
mediation_password = 'NBAMED123'
mediation_port = 22


#SmartCare
if MACHINE == REMOTE_MACHINE:
  jdbc_driver_jar = "/usr/local/drivers/yacht-jdbc-2.18.36.jar"
    # jdbc_driver_jar = "/data/httpd/End-2-End-Call-flow/CallFlow_Backend/Drivers/yacht-jdbc-2.18.36.jar"
elif MACHINE == LOCAL_SHOROUK:
  jdbc_driver_jar = "D:\Work\Re-Org\1- E2E Call Flow\yacht-jdbc (1)/yacht-jdbc-2.18.36.jar"
else:
  jdbc_driver_jar = ''
jdbc_driver_class = "com.huawei.ryoma.yacht.client.YachtDriver"  
jdbc_url = "jdbc:yacht://172.30.109.95:38443/SPARK-SPARKSQL"
db_user = "E2E_DataCube"
db_password = "Voda@e2e@543"




if MACHINE == REMOTE_MACHINE:
  oss_eniq_data_output_dir = r'/home/CallFlow/OSS_ENIQ_Output/Data/'
elif MACHINE == LOCAL_SHOROUK:
  oss_eniq_data_output_dir = r'D:\Work\Re-Org\1- E2E Call Flow\OSS\ENIQ_Output'
else:
  oss_eniq_data_output_dir = ''


## Splunk APIs
splunk_user = 'api_user'
splunk_password = 'API@1234'











