# Columns return from APIs Complain_Type ['Voice']:
    [   
        'myday', 'MSISDN', 'NODE',
        'CELL_NAME', 'SITE_NAME','CLUSTER',
        'SUBREGION', 'REGION', 'IMPU_TEL_URI1',
        'cell_name', 'p_cscf_Name',
        'I_cscf_Name', 'S_cscf_Name', 'AS_NAME'
    ]

# How to calculate rejections or standered deviation

#### Step 1: Data Preparation for This Time Range
- Splunk returns data points typically every 5 minutes.
- For 1 hour (10:00 AM to 11:00 AM), you get 12 data points (timestamps: 10:00, 10:05, ..., 10:55).
The code groups all columns starting with MME_Overload-Protection-Rejection-Newconnection_ into one sub-DataFrame.

MME_Overload-Protection-Rejection-Newconnection_SGSN06
MME_Overload-Protection-Rejection-Newconnection_SGSN07
MME_Overload-Protection-Rejection-Newconnection_SGSN10
MME_Overload-Protection-Rejection-Newconnection_MNS-vSGSN-07


> 1000
MME_Overload-Protection-Rejection-Newconnection-->

| Timestamp           | _SGSN06 | _SGSN07 | _SGSN10 | _MNS-vSGSN-07 |
|--------------------|---------|---------|---------|----------------|
| 2025-12-01 10:00   | 100     | 50      | 200     | 0              |
| 10:05              | 120     | 60      | 220     | 0              |
| ...                | ...     | ...     | ...     | ...            |
| 10:55              | 220     | 160     | 320     | 0              |




alarms --> rejections and deviation [] 
output --> 

#### Step 2: Rejections Calculation

`Rejections = maximum value in the entire sub-DataFrame`

##### How it's computed:

Find max per column (instance) → e.g., SGSN06: 220, SGSN07: 160, SGSN10: 320, MNS: 0
Then take the overall max → 320

Result example: Rejections = 320

#### Step 3: Deviation Rate Calculation
`Deviation Rate (%) = (standard deviation / mean) × 100`

The code uses Coefficient of Variation (CV) to measure relative fluctuation. It computes this over the time range (or rolling windows, e.g., 1H — here the full hour).

Overall deviation: Take the maximum across all instances (detects if any instance is unstable).

##### Step-by-step example (using the sample data above):

- Instance SGSN06 (values: 100 → 220):
    Mean ≈ 164.17
    Std ≈ 37.53
    Deviation = (37.53 / 164.17) × 100 ≈ 22.86%

- Instance SGSN07 (values: 50 → 160):
    Mean ≈ 105.00
    Std ≈ 36.06
    Deviation ≈ `34.34%` (highest — more relative fluctuation despite lower counts)

- Instance SGSN10 (values: 200 → 320):
    Mean ≈ 264.17
    Std ≈ 37.53
    Deviation ≈ 14.21%

- Instance MNS-vSGSN-07 (all 0s): Skipped (or 0%)

**Result: Overall Deviation Rate = 34.34% (max of all instances)**


deviation is calculated on the rolling window time


MME--> nE


-------------------------------


# Test and development

'MME_Overload-Protection-Rejection-Newconnection_BS-vSGSN-02',
'MME_Overload-Protection-Rejection-Newconnection_MNS-vSGSN-07',
'MME_Overload-Protection-Rejection-Newconnection_MOK-vSGSN-06',
'MME_Overload-Protection-Rejection-Newconnection_RMD-vSGSN-08',
'MME_Overload-Protection-Rejection-Newconnection_SGSN06',
'MME_Overload-Protection-Rejection-Newconnection_SGSN08',
'MME_Overload-Protection-Rejection-Newconnection_SGSN09',
'MME_Overload-Protection-Rejection-Newconnection_SGSN10',
'MME_Overload-Protection-Rejection-Newconnection_SGSN7',
'MME_Overload-Protection-Rejection-Newconnection_TAG-vSGSN-04',
'MME_Overload-Protection-Rejection-Newconnection_TNT-vSGSN-03',
'MME_Overload-Protection-Rejection-Newconnection_ZAH-vSGSN-05',
'MME_Overload-Protection-Rejection-Newconnection_vSGSN-MME',


     'MME_Overload-Protection-Rejection-Newconnection_SGSN06',
       'MME_Overload-Protection-Rejection-Newconnection_MOK-vSGSN-06',




['MME_Overload-Protection-Rejection-Newconnection_TNT-vSGSN-03', 'MME_Overload-Protection-Rejection-Newconnection_SGSN7', 'MME_Overload-Protection-Rejection-Newconnection_MOK-vSGSN-06', 'MME_Overload-Protection-Rejection-Newconnection_TAG-vSGSN-04', 'MME_Overload-Protection-Rejection-Newconnection_SGSN09', 'MME_Overload-Protection-Rejection-Newconnection_BS-vSGSN-02', 'MME_Overload-Protection-Rejection-Newconnection_SGSN06', 'MME_Overload-Protection-Rejection-Newconnection_vSGSN-MME', 'MME_Overload-Protection-Rejection-Newconnection_SGSN08', 'MME_Overload-Protection-Rejection-Newconnection_SGSN10', 'MME_Overload-Protection-Rejection-Newconnection_ZAH-vSGSN-05', 'MME_Overload-Protection-Rejection-Newconnection_MNS-vSGSN-07', 'MME_Overload-Protection-Rejection-Newconnection_RMD-vSGSN-08']   



### Steps u have to follow regarding the part of  restrictions on KPIs
1. For each individual KPI column (e.g., MME_Overload-Protection-Rejection-Newconnection_BS-vSGSN-02)
2. Analyze a custom time window: [start_time - 2 hours, end_time]
3. Compare it to the same time window 1 day before
4. Calculate:
    * Total rejections (sum) in current window
    * Deviation rate (%) compared to previous day
5. Trigger alarm if:
   * Total rejections > threshold
   * Deviation rate > threshold  
   