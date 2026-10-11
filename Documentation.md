**Key Output**  
Ranked list of the **top 5 most degraded network elements** (cells, nodes, sites) with:
- How many customers are impacted
- Severity (high / medium / low)
- Clear explanation why it is bad right now

**Required input (from complaint )**
- List of MSISDN(s) of complaining customer(s)
- Optional: IMSI
- Complaint type (Data or Voice)
- Time window of the issue

**Output**
Top degraded network elements (cells, core nodes, sites) ranked by impact, with:
- Number of affected customers
- Severity level
- Why it is considered bad
- Time-series data ready for charts
---

## Four Systems Architecture

The system analyzes four independent data sources and only shows the worst issues.

| Layer       | What we check                          | Granularity       | Main benefit                              |
|-------------|----------------------------------------|-------------------|-------------------------------------------|
| Smartcare   | Which cells/nodes the customer used    | Subscriber → Cell | Starting point + customer count per area  |
| Splunk      | Core network health (MME, SGSN, GGSN…) | Core nodes        | CPU overload, rejections, packet loss     |
| NPP         | Radio cell performance                 | Individual cells  | Throughput, drops, quality, utilization   |
| Viavi       | Transport / Ethernet links             | Sites             | Latency spikes, frame loss                |

>All layers are checked in parallel — if one fails, the others still return results.

---

## Input Example

```json
{
  "msisdn_list":      ["0123456789", "0112345678"],
  "Complain_Type":    "Data",
  "start_time":       "2026-02-15 10:00:00",
  "end_time":         "2026-02-15 10:30:00"
}
```

##  Smartcare (Base Layer – Entry Point)

- Uses customer phone numbers to find which cells, sites and core nodes they were connected to
- Counts how many other customers are using the same cell/site/node (impact estimation)
- Provides the list of suspected cells → sent to NPP
- Provides suspected site codes → sent to Viavi

>This is the foundation — everything else builds on it.
---

## Splunk (Core Nodes)

**Input**  
- Smartcare DataFrame (to map nodes & count subscribers)
- Time windows (current + previous)

**Steps**
1. For each node type (SGSN, MME, GGSN, DSC, DNS):
   - Parallel HTTP calls to many KPI endpoints
   - Merges all KPIs into one wide DataFrame (timestamp + many KPI columns)
2. analyze_core_node()

Take raw Splunk data for one node type
  - Decide which KPIs are really bad right now compared to before
  - Only return the ones that are clearly degraded
  ### Step-by-step: What we do with each KPI from Splunk
  1- We receive a big table from Splunk
  | timestamp           | MME_CPULOAD_SGSN1 | MME_CPULOAD_SGSN2 | MME_Overload-Rejection-New_SGSN1 | ... |
  |--------------------|------------------|------------------|----------------------------------|-----|
  | 2025-02-15 08:00   | 42.3             | 38.1             | 12                               | ... |
  | 2025-02-15 08:05   | 45.6             | 41.2             | 18                               | ... |

 2- We group similar KPIs together

We look at column names and group everything that belongs to the same base KPI.
 ### Examples

| Base KPI | Columns |
|--------|---------|
| `MME_CPULOAD` | `MME_CPULOAD_SGSN1`, `MME_CPULOAD_SGSN2` |
| `MME_Overload-Protection-Rejection-Newconnection` | `MME_Overload..._SGSN1`, `MME_Overload..._RMD01` |
| `SGSN_Attach_SR_2G` | `SGSN_Attach_SR_2G_NODEA`, `SGSN_Attach_SR_2G_NODEB` |

---

  3- For each group (each base KPI), we do the same 5 things
  |  Step | What we do                                                                                                                                                     | Example (MME_CPULOAD)                             |
| ----: | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| **A** | Decide how to summarize numbers (mean, max, or sum)                                                                                                            | CPU load → use **max** (the worst moment matters) |
| **B** | Calculate the **previous** value (previous period)                                                                                                      | Previous 30 minutes → **max = 38%**               |
| **C** | Calculate the **current** value (current period)                                                                                                               | Last 30 minutes → **max = 78%**                   |
| **D** | Check two kinds of rules:<br>1. **Absolute threshold** (is it too high or too low?)<br>2. **Deviation rule** (did it change significantly compared to before?) | See thresholds configuration                      |
| **E** | If **any rule is violated**, mark the KPI as **degraded**                                                                                                      | `78% > 70%` threshold → **degraded**              |


4- How we decide “this KPI is degraded” — the two checks
| KPI Name                                            | Absolute Rule (Threshold)                         | Change Rule (Deviation)       | What Is Considered Bad                |
| --------------------------------------------------- | ------------------------------------------------- | ----------------------------- | ------------------------------------- |
| **MME_CPULOAD**                                     | `> 70%`                                           | —                             | CPU utilization is too high           |
| **MME_Overload-Protection-Rejection-Newconnection** | `> 50`                                            | `> +100%` (doubled or more)   | Too many new connection rejections    |
| **SGSN_Attach_SR_2G**                               | `< 94%`<br>(written internally as `> 6%` failure) | Drop more than `-10%`         | Attach success rate became much worse |
| **GGSN_PDPSR**                                      | `< 99%`                                           | Drop more than `-10%`         | PDP context success rate is too low   |
| **DNS_HitRatio_HQ**                                 | `< 95%`                                           | Drop more than `-10%`         | DNS cache hit rate is poor            |
| **GGSN_PKTLoss**                                    | —                                                 | Increase greater than `+100%` | Packet loss suddenly increased        |

> So two ways to fail:
Value now is outside allowed range (absolute threshold)
OR value changed too much compared to previous period (deviation)

5- Severity is decided from how bad the change is
```
Change %         → Severity
5–15% worse      → low
15–30% worse     → medium
>30% worse       → high

Special case: for drop rates, latency, loss → increase is bad
for success rates, throughput → decrease is bad
```

6- Mapping back to Smartcare & counting impact
Once a KPI is flagged as bad, we map its node name to the matching Smartcare column and count how many unique MSISDNs are affected — this number becomes impacted_subs (e.g. 42).

**Output per degraded node**
```json
{
  "node": "MME",
  "impacted_subs": 42,
  "mapped_value": "MME-01",
  "degraded_kpis": [
    {
      "name": "MME-01",
      "current_value": 78.4,
      "kpi_data": [ { "datetime": "...", "value": 76.2 }, ... ]
    }
  ],
  "system": "splunk"
}
```

##  NPP (Radio / Cell Level KPIs)
**Input**  
- Unique cell_name list from Smartcare
-  Time range

**Steps**
- HTTP POST to internal service (loops over cells)
- returns DataFrame with columns like Cell, DATETIME_ID, AVG_DL_User_Throu, CDR_Cell, ...
```
analyze_npp(...)
  → per cell
  → current vs previous period
  → mean aggregation
  → checks thresholds + deviation
```
**Output**
```json
{
  "cell_name": "ABC12345H",
  "system": "NPP",
  "impacted_customers": 68,
  "degraded_kpis": [
    {
      "name": "AVG_DL_User_Throu",
      "current_value": "4.12",
      "previous_value": "18.90",
      "deviation_pct": -78.2,
      "severity": "high",
      "message": "...",
      "kpi_data": [ ... time-series points ... ]
    },
    ...
  ]
}
```

## Viavi (Transport / Ethernet Layer)
**Input**  

- Smartcare DataFrame → processed cell name to extract suspected site codes
- Time window 

**Steps**

We apply the same degradation logic as NPP, but instead of cells, we analyze KPIs per FLWID and only keep records where FLWID contains the site name, then count the impacted subscribers for that site.

**Output**

```json
{
  "site_name": "ABC",
  "system": "Viavi",
  "impacted_customers": 145,
  "degraded_kpis": [
    {
      "name": "AVG_FD_MS",           // or mapped name
      "current_value": "12.45",
      "deviation_pct": 38.7,
      "severity": "high",
      "kpi_data": [ ... ]
    }
  ]
}
```

## Reliability Features

- Graceful: if Splunk, NPP or Viavi is down → other layers still work
- Fast: results cached where possible, parallel queries
- Auditable: every step is logged
- Conservative: only flags clear, significant degradations