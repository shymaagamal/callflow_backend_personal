# THIS FILE CONTAINS ALL THE CONSTANTS/CONFIGRUATION USED IN THE DJANGO PORTAL

# MACHINES CONSTANTS
from calendar import LocaleHTMLCalendar
from http.client import RemoteDisconnected


REMOTE_MACHINE = "Remote"
# Set this to REMOTE_MACHINE if on the remote machine. Set to "LOCAL_<USER>" While running on local machines
MACHINE = REMOTE_MACHINE

# Setting up the SQL Driver based on the Machine running
if MACHINE == REMOTE_MACHINE:
    # SQL Driver
    SQL_DRIVER = "ODBC Driver 13 for SQL Server"
  
else:
   #SQL_DRIVER = "SQL Server Native Client 11.0"
    SQL_DRIVER = "ODBC Driver 17 for SQL Server"  
    

# Django Server Data
DJANGO_SERVER = "10.30.30.177"
#DJANGO_SERVER_USERNAME = "aa"
#DJANGO_SERVER_PASSWORD = "voda_aa_045"
DJANGO_SERVER_USERNAME = "sa"
DJANGO_SERVER_PASSWORD = "045-145-245v"



# MTX_WORKFLOW_TESTING_DATABASE = "CoreNW_MtxWorkflow_Testing"
MTX_VIGILENT_DATABASE = "CoreNW_MTX_Vigilent"
MTX_WORKFLOW_TESTING_DATABASE = "CoreNW_MtxWorkflow_Testing"
MTX_RACKS_DATABASE = "CoreNW_MtxRacks"
MTX_DC_STATIONS_DATABASE = "MtxDcStations"
DOCUMENTATION_DATABASE = "Documentation"
AUTOMATION_DATABASE = "Automation"
APNS_DATABASE = "APNs"
CORE_NW_DATABASE = "CoreNW_KPIS"
CORE_NW_CONFIGURATION_DB = "CoreNW_Configuration"
CORE_NW_CLOUD_DB = "CoreNW_Cloud"
CORE_NW_GILAN_DB = "CoreNW_GiLAN"
CORE_NW_ROAMING_DB = "CoreNW_Roaming"
CORE_NW_Monitor_DB = "CoreNW_Monitor"
CORE_NW_ROAMING_COMMERTIAL_DB = "CoreNW_Commertial_Roaming"
CoreNW_MTX_Capacity_Module_DB = "CoreNW_MTX_Capacity_Module"
CoreNW_Vacations_Module_DB = "CoreNW_Vacations"
Mobile_squad_DB = "Mobile_squad"
Customer_Experience_DB = "Customer_Excellence"
SocialMediaComplaints_DB="SocialMediaComplaints"
StockManagemnet_DB = "Stock_Management"
MTX_ACCEPTANCE_RESTART_DB = "MtxAcceptanceRestart"
MTX_ACCEPTANCE_RESTART_TESTING_DB = "MtxAcceptanceRestartTesting"
Roaming_Location_DB="Roaming_Location"
Revenue_Assurance_DB = "Revenue_Assurance"
Fraud_Detection_DB="Fraud_Detection"
Enterprise_DB="Enterprise"
