"""
Voice KPI formulas on top of the ENIQ queries in voice_kpis_Eniq_Queries.py.

Source of truth: Eniq/Voice ENIQ Formules.xlsx (Sheet2: KPI -> formula) and the
sample workbooks in Eniq/eniq_Samples/, where
    yellow / blue header  = counter used by the formula
    green header          = output column (time, node, KPI value)

One KpiSpec per KPI. The calculation itself is in
services/eniq_voice_kpi_service.py; this module is only the mapping.

Fields
    name         KPI name as written in the formula sheet (green column).
    query        key in VOICE_KPI_ENIQ_QUERIES that returns the counters.
    node         column(s) that name the node (green column). The first label
                 found in the result is used.
    extra_dims   other green dimension columns kept in the output.
    object_dims  sub-object columns (plug-in unit, peer, route ...) kept only
                 when level=object; used if present in the result.
    counters     formula variable -> accepted column labels. Matching is
                 case-insensitive. The first label is the ENIQ counter name,
                 the second the alias used in the queries (some are
                 truncated by ENIQ, e.g. SBGSIPCHALLENGEDINCINVSESSIO), so
                 both the updated and the previous query versions resolve.
    kind         "ratio": value = post(sum(num) / sum(den)) per group;
                 "value": value = agg(expr) per group.
    num/den/expr functions of a dict {variable: pandas Series}.
    post         transform applied to the ratio (scaling, 1 - x, ...).
    agg          aggregation for kind="value": "sum" | "mean" | "max".
    unit         "%", "ratio", "count", "erlang".
    row_filter   optional function(df) -> boolean mask of rows to keep.
    notes        decisions taken where the sheet and the samples disagree
                 or say nothing; also returned in the API response.

Node-level aggregation (level=node, the default): counters are summed over
the sub-objects of a node in a 15-minute period *before* the formula is
applied, which gives the traffic-weighted KPI of the node. Gauges (CPU load)
are averaged instead. level=object returns one value per ENIQ row, which is
exactly what the per-row Excel formulas in the sample workbooks compute.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Sequence, Tuple

DATETIME_COLUMNS = ("DATETIMEID", "DATETIME_ID")

Counters = Dict[str, Tuple[str, ...]]


@dataclass(frozen=True)
class KpiSpec:
    name: str
    query: str
    node: Tuple[str, ...]
    counters: Counters
    kind: str                                   # "ratio" | "value"
    unit: str
    num: Optional[Callable] = None
    den: Optional[Callable] = None
    post: Callable = lambda r: r
    expr: Optional[Callable] = None
    agg: str = "sum"
    extra_dims: Tuple[str, ...] = ()
    object_dims: Tuple[str, ...] = ()
    row_filter: Optional[Callable] = None
    formula: str = ""
    notes: Sequence[str] = field(default_factory=tuple)


def _c(*labels):
    return tuple(labels)


# Counters shared by the SBG SIP KPIs (DC_E_IMSSBG_SIP_RAW).
_SBG_SIP = {
    "estab_net_success": _c("sbgSipNetIncSessionEstabNetworkSuccess", "SBGSIPNETINCSESSIONESTABNETW"),
    "total_inc_setups": _c("sbgSipTotalIncSessSetups", "SBGSIPTOTALINCSESSSETUPS"),
    "challenged_inc": _c("sbgSipChallengedIncInvSessions", "SBGSIPCHALLENGEDINCINVSESSIO"),
    "inc_normal_release": _c("sbgSipSessionIncNormalRelease", "SBGSIPSESSIONINCNORMALRELEAS"),
    "out_normal_release": _c("sbgSipSessionOutNormalRelease", "SBGSIPSESSIONOUTNORMALRELEAS"),
    "initiated_release": _c("sbgSipInitiatedSessionRelease", "SBGSIPINITIATEDSESSIONRELEAS"),
    "ingress_throttling_rej": _c("sbgSipIngressInitRegThrottlingRej", "SBGSIPINGRESSINITREGTHROTTLI"),
    "throttling_rej_cnt": _c("sbgSipInitRegThrottlingRejectedCnt", "SBGSIPINITREGTHROTTLINGREJEC"),
    "success_inc": _c("sbgSipSuccessIncSessions", "SBGSIPSUCCESSINCSESSIONS"),
    "rej_prealert_488": _c("sbgSipRejectedIncPreAlertingSessions488", "SBGSIPREJECTEDINCPREALERTING"),
    "rej_alert_488": _c("sbgSipRejectedIncAlertingSessions488", "SBGSIPREJECTEDINCALERTINGSES"),
}


def _pick(*names):
    return {n: _SBG_SIP[n] for n in names}


SBG_NODE = _c("NEID", "NE_ID")
MOID = _c("MOID")

VOICE_KPI_SPECS = (
    # ------------------------------------------------------------- BGF / MRS
    KpiSpec(
        name="BGF PKT LOSS", query="BGF_Packerloss_IL", unit="%",
        node=_c("NODENAME", "NODE_NAME"), object_dims=_c("BGFNETWORK", "BgfNetwork"),
        counters={"lost": _c("rtpLostPackets", "RTPLOSTPACKETS"),
                  "received": _c("rtpReceivedPackets", "RTPRECEIVEDPACKETS")},
        kind="ratio",
        num=lambda c: c["lost"], den=lambda c: c["lost"] + c["received"],
        post=lambda r: 100 * r,
        formula="100 * rtpLostPackets / (rtpLostPackets + rtpReceivedPackets)",
        notes=("Sheet2 has no x100, but the sample output column is 'Packetloss %', "
               "so the value is returned as a percentage. The sample's array formula "
               "actually spills the received share (~99.99%), not the loss.",),
    ),
    # ------------------------------------------------------------- SBG SIP
    KpiSpec(
        name="SBG CFR", query="CSSR_CDR_THROTTLING_IL", unit="%", node=SBG_NODE,
        object_dims=MOID,
        counters=_pick("estab_net_success", "total_inc_setups", "challenged_inc"),
        kind="ratio",
        num=lambda c: c["estab_net_success"],
        den=lambda c: c["total_inc_setups"] - c["challenged_inc"],
        post=lambda r: 100 * (1 - r),
        formula="100 * (1 - sbgSipNetIncSessionEstabNetworkSuccess / "
                "(sbgSipTotalIncSessSetups - sbgSipChallengedIncInvSessions))",
    ),
    KpiSpec(
        name="SBG CDR", query="CSSR_CDR_THROTTLING_IL", unit="%", node=SBG_NODE,
        object_dims=MOID,
        counters=_pick("inc_normal_release", "out_normal_release", "initiated_release"),
        kind="ratio",
        num=lambda c: c["inc_normal_release"] + c["out_normal_release"],
        den=lambda c: c["inc_normal_release"] + c["out_normal_release"] + c["initiated_release"],
        post=lambda r: 100 - 100 * r,
        formula="100 - 100 * (sbgSipSessionIncNormalRelease + sbgSipSessionOutNormalRelease) / "
                "(sbgSipSessionIncNormalRelease + sbgSipSessionOutNormalRelease + "
                "sbgSipInitiatedSessionRelease)",
    ),
    KpiSpec(
        name="ThrottlingRej", query="CSSR_CDR_THROTTLING_IL", unit="count", node=SBG_NODE,
        object_dims=MOID, counters=_pick("ingress_throttling_rej"),
        kind="value", expr=lambda c: c["ingress_throttling_rej"], agg="sum",
        formula="sbgSipIngressInitRegThrottlingRej",
    ),
    KpiSpec(
        name="ThrottlingRejCNT", query="CSSR_CDR_THROTTLING_IL", unit="count", node=SBG_NODE,
        object_dims=MOID, counters=_pick("throttling_rej_cnt"),
        kind="value", expr=lambda c: c["throttling_rej_cnt"], agg="sum",
        formula="sbgSipInitRegThrottlingRejectedCnt",
    ),
    KpiSpec(
        name="SBG SRVCC SR", query="SBG_SIP_SRVCC_IL", unit="ratio", node=SBG_NODE,
        object_dims=MOID,
        counters=_pick("success_inc", "rej_prealert_488", "rej_alert_488", "total_inc_setups"),
        kind="ratio",
        num=lambda c: c["success_inc"] + c["rej_prealert_488"] + c["rej_alert_488"],
        den=lambda c: c["total_inc_setups"],
        formula="(sbgSipSuccessIncSessions + sbgSipRejectedIncPreAlertingSessions488 + "
                "sbgSipRejectedIncAlertingSessions488) / sbgSipTotalIncSessSetups",
    ),
    # ------------------------------------------------------------- SBG other
    KpiSpec(
        name="SBG Intial REG SR", query="SBG_IntialReG_IL", unit="ratio", node=SBG_NODE,
        object_dims=MOID,
        counters={
            "acc": _c("sbgSipRegStatInitialAccCnt", "SBGSIPREGSTATINITIALACCCNT"),
            "rej400": _c("sbgSipRegStatRejInitialRegCnt400", "SBGSIPREGSTATREJINITIALREGCN1"),
            "rej403": _c("sbgSipRegStatRejInitialRegCnt403", "SBGSIPREGSTATREJINITIALREGCN2"),
            "rej404": _c("sbgSipRegStatRejInitialRegCnt404", "SBGSIPREGSTATREJINITIALREGCN3"),
            "att": _c("sbgSipRegStatInitialAttCnt", "SBGSIPREGSTATINITIALATTCNT"),
        },
        kind="ratio",
        num=lambda c: c["acc"] + c["rej400"] + c["rej403"] + c["rej404"],
        den=lambda c: c["att"],
        formula="(sbgSipRegStatInitialAccCnt + sbgSipRegStatRejInitialRegCnt400 + "
                "sbgSipRegStatRejInitialRegCnt403 + sbgSipRegStatRejInitialRegCnt404) / "
                "sbgSipRegStatInitialAttCnt",
    ),
    KpiSpec(
        name="SBG CPU LOAD", query="SBG_CPU_IL", unit="%", node=SBG_NODE, object_dims=MOID,
        counters={"cpu": _c("CPULoad_Total", "CPULOADTOTAL")},
        kind="value", expr=lambda c: c["cpu"], agg="mean",
        row_filter=lambda df: ~df["MOID"].astype(str).str.contains("OSDevice", na=False)
        if "MOID" in df.columns else None,
        formula="CPULoad_Total (average over the node's processing units)",
        notes=("Sheet2 gives no formula; CPULOADTOTAL is the green column in the sample.",
               "OSDevice (disk) rows are excluded: they always report 0 CPU load in the "
               "sample and would pull the node average down.",),
    ),
    KpiSpec(
        name="SBG CPU Usage", query="SBG_CPUUsage_IL", unit="%", node=SBG_NODE, object_dims=MOID,
        counters={"cpu": _c("sbgApplCPUUsageMax", "SBGAPPLCPUUSAGEMAX")},
        kind="value", expr=lambda c: c["cpu"], agg="mean",
        formula="sbgApplCPUUsageMax (average over the node's payload processors)",
    ),
    KpiSpec(
        name="SBG Total Users", query="SBG_Users_IL", unit="count", node=SBG_NODE,
        object_dims=MOID,
        counters={"reg": _c("sbgSgcRegResCurrentTotalReg", "SBGSGCREGRESCURRENTTOTALREG")},
        kind="value", expr=lambda c: c["reg"], agg="sum",
        formula="sbgSgcRegResCurrentTotalReg (summed over the node's mated pairs)",
    ),
    KpiSpec(
        name="SBG Traffic", query="Traffic_MSC_IL", unit="erlang", node=_c("SN"),
        object_dims=_c("MOID", "ELEM"),
        counters={"trali": _c("TRALI"), "tralo": _c("TRALO")},
        kind="value", expr=lambda c: c["trali"] + c["tralo"], agg="sum",
        formula="TRALI + TRALO (summed over the node's trunk routes)",
        notes=("No sample workbook was provided for Traffic_MSC_IL, so the output columns "
               "are not confirmed; SN is used as the node, as in the other CNAXE tables.",
               "TRALI/TRALO come from DC_E_CNAXE_TRUNKROUTE_RAW (MSC trunk routes) although "
               "the KPI is named 'SBG Traffic'.",),
    ),
    # ------------------------------------------------------------- DSC
    KpiSpec(
        name="DSC Throttling", query="DSC_Throttling_IL", unit="count", node=_c("DSC"),
        object_dims=_c("DSCPEERGROUP", "DscPeerGroup"),
        counters={"rej": _c("CC_OUT_REQ_PEER_GROUP_THROTTLING_REJECTED", "CCOUTREQPEERGROUPTHROTTLINGR")},
        kind="value", expr=lambda c: c["rej"], agg="sum",
        formula="CC_OUT_REQ_PEER_GROUP_THROTTLING_REJECTED",
    ),
    KpiSpec(
        name="DSC Error", query="DSC_ERROR_IL", unit="count", node=_c("DSC"),
        object_dims=_c("DSCREMOTEPEER", "DscRemotePeer"),
        counters={"ebit": _c("CC_IN_ANS_NUM_OF_MSGS_EBIT_SET", "CCINANSNUMOFMSGSEBITSET")},
        kind="value", expr=lambda c: c["ebit"], agg="sum",
        formula="CC_IN_ANS_NUM_OF_MSGS_EBIT_SET",
        notes=("Sheet2 says 'ANS_NUM_OF_MSGS_EBIT'; the green sample column is "
               "CCINANSNUMOFMSGSEBITSET (incoming answers), so CC_IN_... is used, "
               "not CC_OUT_ANS_NUM_OF_MSGS_EBIT_SET.",),
    ),
    # ------------------------------------------------------------- IPSTP
    KpiSpec(
        name="IPSTP PRC MESG", query="IPSTP_PRC_THROTTLING_IL", unit="count", node=_c("ELEM"),
        object_dims=MOID, counters={"msg": _c("MSGPERPRC")},
        kind="value", expr=lambda c: c["msg"], agg="sum",
        formula="MSGPERPRC (PRC600-, PRC601-, PRC575-, PRC578-)",
    ),
    # ------------------------------------------------------------- MGW
    KpiSpec(
        name="MGW_CPU", query="MGW_CPU_IL", unit="%", node=_c("MGW"), object_dims=MOID,
        counters={"load": _c("pmProcessorLoad", "PMPROCESSORLOAD")},
        kind="value", expr=lambda c: c["load"], agg="mean",
        formula="pmProcessorLoad (average over the node's plug-in units)",
    ),
    KpiSpec(
        name="MGW_NB_ReJ", query="MGW_NBREJ_IL", unit="ratio", node=_c("MGW"),
        object_dims=_c("VMGW", "Vmgw"),
        counters={"faults": _c("pmNrOfOrigNbInitFaults", "PMNROFORIGNBINITFAULTS"),
                  "init": _c("pmNrOfOrigNbInit", "PMNROFORIGNBINIT")},
        kind="ratio", num=lambda c: c["faults"], den=lambda c: c["init"],
        formula="pmNrOfOrigNbInitFaults / pmNrOfOrigNbInit",
    ),
    KpiSpec(
        name="MGW_PKTLOSS", query="MGW_PKTLOSS_IL", unit="%", node=_c("MGW"),
        object_dims=_c("REMOTESITE", "RemoteSite"),
        counters={"lost": _c("pmRtpLostPkts", "PMRTPLOSTPKTS"),
                  "hi": _c("pmRtpReceivedPktsHi", "PMRTPRECEIVEDPKTSHI"),
                  "lo": _c("pmRtpReceivedPktsLo", "PMRTPRECEIVEDPKTSLO")},
        kind="ratio",
        num=lambda c: c["lost"],
        den=lambda c: c["lost"] + c["hi"] * (2 ** 31) + c["lo"],
        post=lambda r: 100 * r,
        formula="100 * pmRtpLostPkts / (pmRtpLostPkts + pmRtpReceivedPktsHi * 2^31 + "
                "pmRtpReceivedPktsLo)",
    ),
    # ------------------------------------------------------------- MSC
    KpiSpec(
        name="MSC_Total_LU", query="MGW_LU_UPD_IL", unit="ratio", node=_c("SN"),
        object_dims=MOID,
        counters={"succ": _c("NLALOCSUCC"), "tot": _c("NLALOCTOT")},
        kind="ratio", num=lambda c: c["succ"], den=lambda c: c["tot"],
        formula="NLALOCSUCC / NLALOCTOT",
    ),
    KpiSpec(
        name="MSC_Users", query="MSC_USERS_IL", unit="count", node=_c("SN"),
        object_dims=_c("ELEM"),
        counters={"users": _c("NMMESATSGS")},
        kind="value", expr=lambda c: c["users"], agg="sum",
        formula="NMMESATSGS (summed over the node's rows)",
    ),
    KpiSpec(
        name="MSC_CPLOAD", query="MSCBC_CPU_IL", unit="ratio", node=_c("ELEM"),
        counters={"acc": _c("LOAS_ACCLOAD", "LOASACCLOAD"), "nscan": _c("LOAS_NSCAN", "LOASNSCAN")},
        kind="ratio", num=lambda c: c["acc"], den=lambda c: c["nscan"],
        formula="LOAS_ACCLOAD / LOAS_NSCAN",
    ),
    KpiSpec(
        name="MSC_SAE", query="SAE_MSC_IL", unit="ratio", node=_c("SN"),
        extra_dims=_c("UTCDATETIMEID", "UTC_DATETIME_ID"), object_dims=MOID,
        counters={"tralacc": _c("NTRALACC"), "nind": _c("NIND"), "nscan": _c("NSCAN")},
        kind="ratio", num=lambda c: c["tralacc"], den=lambda c: c["nind"] * c["nscan"],
        formula="NTRALACC / (NIND * NSCAN)",
    ),
    # ------------------------------------------------------------- SAPC
    KpiSpec(
        name="SAPC GX CCR FR", query="SAPC_RX_GX_IL", unit="ratio",
        node=_c("G3MANAGEDELEMENT", "g3ManagedElement"), object_dims=MOID,
        counters={"resp": _c("GxResponsesSent", "GXRESPONSESSENT"),
                  "act": _c("GxPdpActivates", "GXPDPACTIVATES"),
                  "mod": _c("GxPdpModifies", "GXPDPMODIFIES"),
                  "term": _c("GxPdpTerminates", "GXPDPTERMINATES")},
        kind="ratio",
        num=lambda c: c["resp"], den=lambda c: c["act"] + c["mod"] + c["term"],
        post=lambda r: 1 - r,
        formula="1 - GxResponsesSent / (GxPdpActivates + GxPdpModifies + GxPdpTerminates)",
    ),
    KpiSpec(
        name="SAPC RX FR", query="SAPC_RX_GX_IL", unit="ratio",
        node=_c("G3MANAGEDELEMENT", "g3ManagedElement"), object_dims=MOID,
        counters={"af_sig_success": _c("rxAaasAfSignallingSuccess", "RXAAASAFSIGNALLINGSUCCESS"),
                  "term_resp": _c("RxTerminateResponses", "RXTERMINATERESPONSES"),
                  "auth_req": _c("RxAuthReqCreates", "RXAUTHREQCREATES"),
                  "aars_update": _c("rxAarsUpdate", "RXAARSUPDATE"),
                  "strs_af_sig": _c("rxStrsAfSignalling", "RXSTRSAFSIGNALLING")},
        kind="ratio",
        num=lambda c: c["af_sig_success"] + c["term_resp"],
        den=lambda c: c["auth_req"] + c["aars_update"] + c["strs_af_sig"],
        post=lambda r: 1 - r,
        formula="1 - (rxAaasAfSignallingSuccess + RxTerminateResponses) / "
                "(RxAuthReqCreates + rxAarsUpdate + rxStrsAfSignalling), nulls counted as 0",
        notes=("Sheet2 writes If([x] Is Null,0,0), which is always 0; the sample's Excel "
               "formula IF(ISBLANK(x),0,x) uses the counter when present, and that is "
               "what is implemented (sample row: 1 - (1+2)/(3+4+5) = 0.75).",),
    ),
)

VOICE_KPI_SPECS_BY_NAME = {s.name: s for s in VOICE_KPI_SPECS}
