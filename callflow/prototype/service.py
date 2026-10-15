"""
The Prototype Smartcare analysis, extracted from
PrototypeSmartcareIntegrationAPIView.post so it can run either inside the
request (sync, previous contract) or in a background job (async opt-in).

`run_prototype_analysis` returns (http_status, body) where body is exactly
what the view used to put in Response(...): same keys, same values, same
error shapes ({'error': ...} with 400/500).
"""
import datetime
import logging

from . import apis
from . import conf
from . import runtime

logger = logging.getLogger("callflow.prototype.service")


def parse_dates(startDate, endDate):
    formatted_startDate = datetime.datetime.strptime(startDate, '%Y-%m-%dT%H:%M')
    start_hour = str(formatted_startDate.hour)
    formatted_startDate = formatted_startDate.strftime('%Y-%m-%d %I:%M %p')
    startDate = startDate.replace('T', ' ')
    formatted_startDate_2 = datetime.datetime.strptime(startDate, '%Y-%m-%d %H:%M')
    formatted_startDate_3 = datetime.datetime.fromisoformat(startDate).strftime("%m/%d/%Y:%H:%M:%S")
    formatted_startDate_4 = datetime.datetime.fromisoformat(startDate).strftime("%Y/%m/%d %H:%M:%S")

    formatted_endDate = datetime.datetime.strptime(endDate, '%Y-%m-%dT%H:%M')
    end_hour = str(formatted_endDate.hour)
    formatted_endDate = formatted_endDate.strftime('%Y-%m-%d %I:%M %p')
    endDate = endDate.replace('T', ' ')
    formatted_endDate_2 = datetime.datetime.strptime(endDate, '%Y-%m-%d %H:%M')
    formatted_endDate_3 = datetime.datetime.fromisoformat(endDate).strftime('%m/%d/%Y:%H:%M:%S')
    formatted_endDate_4 = datetime.datetime.fromisoformat(endDate).strftime("%Y/%m/%d %H:%M:%S")

    return {
        'start_hour': start_hour,
        'end_hour': end_hour,
        'formatted_startDate': formatted_startDate,
        'formatted_endDate': formatted_endDate,
        'formatted_startDate_2': formatted_startDate_2,
        'formatted_endDate_2': formatted_endDate_2,
        'formatted_startDate_3': formatted_startDate_3,
        'formatted_endDate_3': formatted_endDate_3,
        'formatted_startDate_4': formatted_startDate_4,
        'formatted_endDate_4': formatted_endDate_4,
    }


def completeness(agg):
    """The same two check lists the 6-attempt loop used."""
    (data, site_counts, cell_counts, suspected_sgw, rat_counts, region_counts,
     subregion_counts, sgsn_counts, tac_counts, mme_counts, DRA_NAME, OCS_NAME, PCRF) = agg
    required_checks = [
        not data.empty, bool(cell_counts), bool(site_counts), bool(suspected_sgw),
        bool(region_counts), bool(subregion_counts), bool(sgsn_counts), bool(DRA_NAME),
    ]
    all_checks = [
        not data.empty, bool(site_counts), bool(cell_counts), bool(suspected_sgw),
        bool(rat_counts), bool(region_counts), bool(subregion_counts), bool(sgsn_counts),
        bool(tac_counts), bool(mme_counts), bool(DRA_NAME), bool(OCS_NAME), bool(PCRF),
    ]
    return sum(required_checks), len(required_checks), sum(all_checks) / len(all_checks)


def run_prototype_analysis(params, ctx):
    """
    params: dict with msisdn_list, start_date, end_date, Complain_Type, imsi, user
    ctx:    runtime.RunContext (already entered)
    """
    msisdn_list = params.get('msisdn_list')
    start = params.get('start_date')
    end = params.get('end_date')
    complain_type = params.get('Complain_Type')
    imsi = params.get('imsi')

    try:
        if not start or not end:
            return 400, {'error': 'Missing start_date or end_date'}
        if not msisdn_list:
            return 400, {'error': 'msisdn_list is required'}

        with ctx.stage("parse_and_validate"):
            dates = parse_dates(start, end)
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            logs_file = conf.legacy_logs_file()
            runtime.legacy_log(logs_file, f"\n{timestamp} - Smartcare Request\n{params.get('user')}\n"
                                          f"request_id={ctx.request_id}\n")
            apis.smartcare_Validation_Inputs(msisdn_list, dates['formatted_startDate_2'],
                                             dates['formatted_endDate_2'])

        # Same early "can we connect?" check as before - but the connection is
        # now USED for the queries and closed afterwards (it used to leak).
        with ctx.stage("smartcare_connect_check"):
            cursor = apis.smartcareConnection(logs_file)
        if cursor is None:
            return 500, {'error': 'Database connection failed'}

        try:
            with ctx.stage("smartcare_total"):
                nodes_data = apis.smartcareNodesQueries(
                    msisdn_list, imsi, complain_type,
                    dates['formatted_startDate_2'], dates['formatted_endDate_2'],
                    logs_file, timestamp, cursor=cursor)
        finally:
            apis.close_cursor(cursor)

        if isinstance(nodes_data, dict) and "error" in nodes_data:
            return 400, nodes_data
        ctx.event("nodes_data", rows=len(nodes_data), columns=len(nodes_data.columns))

        if complain_type == 'Data':
            # The old code ran this up to 6 times. smartcareOutput_Aggregated_Data
            # is a pure function of nodes_data (it never re-queries and never
            # mutates its input), so attempts 2..6 returned identical values.
            with ctx.stage("aggregate_data", rows=len(nodes_data)):
                agg = apis.smartcareOutput_Aggregated_Data(nodes_data, logs_file, timestamp)
            (data, site_counts, cell_counts, suspected_sgw, rat_counts, region_counts,
             subregion_counts, sgsn_counts, tac_counts, mme_counts, DRA_NAME, OCS_NAME, PCRF) = agg
            ok_required, n_required, completion = completeness(agg)
            ctx.event("aggregation_completeness", required=f"{ok_required}/{n_required}",
                      overall=f"{completion:.0%}",
                      complete=(ok_required == n_required and completion >= 0.85))
            if not (ok_required == n_required and completion >= 0.85):
                ctx.warning("aggregation_incomplete", required=f"{ok_required}/{n_required}",
                            overall=f"{completion:.0%}",
                            note="old code repeated the identical aggregation 6x here")

            cell_list = list(cell_counts.keys())
            affected_hub = {}
            if nodes_data is not None:
                with ctx.stage("hub_metrics", cells=len(cell_list)):
                    affected_hub = apis.calculate_affected_hub_metrics(nodes_data, cell_list, timestamp)
        else:
            ctx.dump_csv(nodes_data, 'sc_nodes_before_voice.csv')
            with ctx.stage("aggregate_voice", rows=len(nodes_data)):
                (data, site_counts, cell_counts, cluster_counts, region_counts, subregion_counts,
                 p_cscf_name, AS_Name, MSC_NAME, DRA_NAME, OCS_NAME, PCRF) = \
                    apis.smartcareOutput_Aggregated_Voice(nodes_data, logs_file, timestamp)
            cell_list = list(cell_counts.keys())
            affected_hub = {}
            if nodes_data is not None:
                with ctx.stage("hub_metrics", cells=len(cell_list)):
                    affected_hub = apis.calculate_affected_hub_metrics(nodes_data, cell_list, timestamp)

        ctx.dump_csv(nodes_data, 'sc_nodes.csv')
        # (the unused `smartcare_json_output` - built, json.dumps'ed and then
        #  discarded - is no longer computed)

        if complain_type == 'Data':
            return 200, {
                'suspected_sites': site_counts,
                'suspected_cells_raw': cell_counts,
                'suspected_sgws': suspected_sgw,
                'HUB_Counts': affected_hub,
                'rat_counts': rat_counts,
                'DRA_NAME': DRA_NAME,
                'OCS_NAME': OCS_NAME,
                'PCRF': PCRF,
                'region_counts': region_counts,
                'subregion_counts': subregion_counts,
                'sgsn_counts': sgsn_counts,
                'tac_counts': tac_counts,
                'mme_counts': mme_counts,
            }
        return 200, {
            'Suspected_Sites': site_counts,
            'Suspected_Cells': cell_counts,
            'Cluster_Counts': cluster_counts,
            'Region_Counts': region_counts,
            'Sub_Region_Counts': subregion_counts,
            'HUB_Counts': affected_hub,
            'CSCF_Name': p_cscf_name,
            'AS_Name': AS_Name,
            'MSC_Name': MSC_NAME,
            'DRA_NAME': DRA_NAME,
            'OCS_NAME': OCS_NAME,
            'PCRF': PCRF,
        }

    except Exception as e:
        ctx.exception("analysis_error", error=str(e))
        return 500, {'error': str(e)}
