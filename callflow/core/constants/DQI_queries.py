dqi_script = """SELECT Date, DED_BEARER_REQ_TIMES
FROM (
SELECT Date, SUM(DED_BEARER_REQ_TIMES)  AS DED_BEARER_REQ_TIMES
FROM clickhouse.incident_db.Node_DETAIL_CDR_S1MME
WHERE Date >= date_add('hour' , -6 , current_timestamp)
GROUP BY Date
ORDER BY Date DESC
OFFSET 2
)
ORDER BY Date ASC"""


dqi_script_1= """WITH cfg AS (
  SELECT
    20.0 AS rtt_good, 700.0 AS rtt_bad,
    0.5 AS retrans_good, 15 AS retrans_bad,
    2.0 AS rtt_tail_good, 25.0 AS rtt_tail_bad,
    0.05 AS outseq_good_TCP, 2.00 AS outseq_bad_TCP,
    0.01 AS outseq_good_QUIC, 0.3 AS outseq_bad_QUIC,
    0.05 AS quic_loss_good, 20.00 AS quic_loss_bad,
    20.0 AS udp_jitter_good, 250.0 AS udp_jitter_bad,
    0.14 AS pipe2_w_rel_retrans,
    0.14 AS pipe2_w_rel_outseq,
    0.14 AS pipe2_w_rel_quic_loss,
    0.14 AS pipe2_w_rel_quic_outseq,
    0.14 AS pipe2_w_lat_rtt,
    0.14 AS pipe2_w_lat_rttlong,
    0.14 AS pipe2_w_lat_udpjitter,
    85.0 AS pipe_class_excellent,   
    70.0 AS pipe_class_good,        
    50.0 AS pipe_class_fair         
),
pipe_base AS (
  SELECT
    p.cgisai,
    b.cell_name,
    date_parse(p.time_t, '%Y/%m/%d %H:%i') AS time,

    SUM(CAST(p.tcp_dl_packets_withpl AS DOUBLE)) AS tcp_dl_pkts,
    SUM(CAST(p.tcp_ul_packets_withpl AS DOUBLE)) AS tcp_ul_pkts,
    SUM(CAST(p.tcp_dl_retrans_withpl AS DOUBLE)) AS tcp_dl_retrans,
    SUM(CAST(p.tcp_ul_retrans_withpl AS DOUBLE)) AS tcp_ul_retrans,
    SUM(CAST(p.tcp_dl_outofsequ AS DOUBLE)) AS tcp_dl_ooseq,
    SUM(CAST(p.tcp_ul_outofsequ AS DOUBLE)) AS tcp_ul_ooseq,

    SUM(
      CAST(p.avg_dw_rtt AS DOUBLE) / 1000
      * CAST(p.dw_rtt_stat_num AS DOUBLE)
    ) AS dl_rtt_wsum,

    SUM(CAST(p.dw_rtt_stat_num AS DOUBLE)) AS dl_rtt_w,

    SUM(
      CAST(p.avg_ul_rtt AS DOUBLE) / 1000
      * CAST(p.ul_rtt_stat_num AS DOUBLE)
    ) AS ul_rtt_wsum,

    SUM(CAST(p.ul_rtt_stat_num AS DOUBLE)) AS ul_rtt_w,

    SUM(CAST(p.dw_rtt_long_num AS DOUBLE)) AS dl_rtt_long,
    SUM(CAST(p.ul_rtt_long_num AS DOUBLE)) AS ul_rtt_long,

    SUM(CAST(p.udp_dl_packets AS DOUBLE)) AS udp_dl_pkts,
    SUM(CAST(p.udp_ul_packets AS DOUBLE)) AS udp_ul_pkts,

    SUM(CAST(p.udp_dl_jitter AS DOUBLE)) AS udp_dl_jitter_wsum,
    SUM(CAST(p.udp_ul_jitter AS DOUBLE)) AS udp_ul_jitter_wsum,

    SUM(CAST(p.quic_l4_dw_packets AS DOUBLE)) AS quic_dl_pkts,
    SUM(CAST(p.quic_l4_ul_packets AS DOUBLE)) AS quic_ul_pkts,

    SUM(CAST(p.quic_l4_dw_packets_lost AS DOUBLE)) AS quic_dl_lost,
    SUM(CAST(p.quic_l4_ul_packets_lost AS DOUBLE)) AS quic_ul_lost,

    SUM(CAST(p.quic_dw_outofsequ AS DOUBLE)) AS quic_dl_ooseq,
    SUM(CAST(p.quic_ul_outofsequ AS DOUBLE)) AS quic_ul_ooseq

  FROM hive.smartcare.pspipe_kpi p

  LEFT JOIN hive.smartcare.dim_loc_cgisai b
    ON p.cgisai = b.cgisai

  WHERE p.time_t BETWEEN '{start_date}' AND '{end_date}'

    AND b.cell_name IN {cell_list}

  GROUP BY 1, 2, 3
  ORDER BY time
),
pipe_metrics AS (
  SELECT
	cgisai,
	cell_name,
    time,
    (COALESCE(x.tcp_dl_pkts,0) + COALESCE(x.tcp_ul_pkts,0)) AS tcp_pkts,
    (COALESCE(x.tcp_dl_retrans,0) + COALESCE(x.tcp_ul_retrans,0)) AS tcp_retrans,
    (COALESCE(x.tcp_dl_ooseq,0) + COALESCE(x.tcp_ul_ooseq,0)) AS tcp_ooseq,
    (COALESCE(x.quic_dl_pkts,0) + COALESCE(x.quic_ul_pkts,0)) AS quic_pkts,
    (COALESCE(x.quic_dl_lost,0) + COALESCE(x.quic_ul_lost,0)) AS quic_lost,
    (COALESCE(x.quic_dl_ooseq,0) + COALESCE(x.quic_ul_ooseq,0)) AS quic_ooseq,
    CASE
      WHEN COALESCE(x.dl_rtt_w,0) + COALESCE(x.ul_rtt_w,0) > 0 THEN
        (COALESCE(x.dl_rtt_wsum,0) + COALESCE(x.ul_rtt_wsum,0))
        / NULLIF(COALESCE(x.dl_rtt_w,0) + COALESCE(x.ul_rtt_w,0), 0)
    END AS rtt_ms,
    CASE
      WHEN COALESCE(x.dl_rtt_w,0) + COALESCE(x.ul_rtt_w,0) > 0 THEN
        (COALESCE(x.dl_rtt_long,0) + COALESCE(x.ul_rtt_long,0))
        / (COALESCE(x.dl_rtt_w,0) + COALESCE(x.ul_rtt_w,0)) * 100.0
    END AS rtt_tail_pct,
    CASE
      WHEN COALESCE(x.udp_dl_pkts,0) + COALESCE(x.udp_ul_pkts,0) > 0 THEN
        (COALESCE(x.udp_dl_jitter_wsum,0) + COALESCE(x.udp_ul_jitter_wsum,0))
        / (COALESCE(x.udp_dl_pkts,0) + COALESCE(x.udp_ul_pkts,0))
    END AS udp_jitter_ms
  FROM pipe_base x
),
final_pscore AS (
  SELECT
	cgisai,
	m.cell_name,
    time,
    CASE
      WHEN (
        (CASE WHEN m.tcp_pkts > 0 THEN c.pipe2_w_rel_retrans ELSE 0 END) +
        (CASE WHEN m.tcp_pkts > 0 THEN c.pipe2_w_rel_outseq  ELSE 0 END) +
        (CASE WHEN m.quic_pkts > 0 THEN c.pipe2_w_rel_quic_loss  ELSE 0 END) +
        (CASE WHEN m.quic_pkts > 0 THEN c.pipe2_w_rel_quic_outseq ELSE 0 END) +
        (CASE WHEN m.rtt_ms IS NOT NULL THEN c.pipe2_w_lat_rtt ELSE 0 END) +
        (CASE WHEN m.rtt_tail_pct IS NOT NULL THEN c.pipe2_w_lat_rttlong ELSE 0 END) +
        (CASE WHEN m.udp_jitter_ms IS NOT NULL THEN c.pipe2_w_lat_udpjitter ELSE 0 END)
      ) = 0
      THEN NULL
      ELSE (
        COALESCE(
          CASE WHEN m.tcp_pkts > 0 THEN
            CASE WHEN (m.tcp_retrans*100.0)/m.tcp_pkts <= c.retrans_good THEN 100.0
                 WHEN (m.tcp_retrans*100.0)/m.tcp_pkts >= c.retrans_bad  THEN 0.0
                 ELSE 100.0 * (c.retrans_bad - (m.tcp_retrans*100.0)/m.tcp_pkts) / (c.retrans_bad - c.retrans_good)
            END
          END, 0.0) * c.pipe2_w_rel_retrans
        + COALESCE(
          CASE WHEN m.tcp_pkts > 0 THEN
            CASE WHEN (m.tcp_ooseq*100.0)/m.tcp_pkts <= c.outseq_good_TCP THEN 100.0
                 WHEN (m.tcp_ooseq*100.0)/m.tcp_pkts >= c.outseq_bad_TCP  THEN 0.0
                 ELSE 100.0 * (c.outseq_bad_TCP - (m.tcp_ooseq*100.0)/m.tcp_pkts) / (c.outseq_bad_TCP - c.outseq_good_TCP)
            END
          END, 0.0) * c.pipe2_w_rel_outseq
        + COALESCE(
          CASE WHEN m.quic_pkts > 0 THEN
            CASE WHEN (m.quic_lost*100.0)/m.quic_pkts <= c.quic_loss_good THEN 100.0
                 WHEN (m.quic_lost*100.0)/m.quic_pkts >= c.quic_loss_bad  THEN 0.0
                 ELSE 100.0 * (c.quic_loss_bad - (m.quic_lost*100.0)/m.quic_pkts) / (c.quic_loss_bad - c.quic_loss_good)
            END
          END, 0.0) * c.pipe2_w_rel_quic_loss
        + COALESCE(
          CASE WHEN m.quic_pkts > 0 THEN
            CASE WHEN (m.quic_ooseq*100.0)/m.quic_pkts <= c.outseq_good_QUIC THEN 100.0
                 WHEN (m.quic_ooseq*100.0)/m.quic_pkts >= c.outseq_bad_QUIC  THEN 0.0
                 ELSE 100.0 * (c.outseq_bad_QUIC - (m.quic_ooseq*100.0)/m.quic_pkts) / (c.outseq_bad_QUIC - c.outseq_good_QUIC)
            END
          END, 0.0) * c.pipe2_w_rel_quic_outseq
        + COALESCE(
          CASE WHEN m.rtt_ms IS NOT NULL THEN
            CASE WHEN m.rtt_ms <= c.rtt_good THEN 100.0
                 WHEN m.rtt_ms >= c.rtt_bad  THEN 0.0
                 ELSE 100.0 * (c.rtt_bad - m.rtt_ms) / (c.rtt_bad - c.rtt_good)
            END
          END, 0.0) * c.pipe2_w_lat_rtt
        + COALESCE(
          CASE WHEN m.rtt_tail_pct IS NOT NULL THEN
            CASE WHEN m.rtt_tail_pct <= c.rtt_tail_good THEN 100.0
                 WHEN m.rtt_tail_pct >= c.rtt_tail_bad  THEN 0.0
                 ELSE 100.0 * (c.rtt_tail_bad - m.rtt_tail_pct) / (c.rtt_tail_bad - c.rtt_tail_good)
            END
          END, 0.0) * c.pipe2_w_lat_rttlong
        + COALESCE(
          CASE WHEN m.udp_jitter_ms IS NOT NULL THEN
            CASE WHEN m.udp_jitter_ms <= c.udp_jitter_good THEN 100.0
                 WHEN m.udp_jitter_ms >= c.udp_jitter_bad  THEN 0.0
                 ELSE 100.0 * (c.udp_jitter_bad - m.udp_jitter_ms) / (c.udp_jitter_bad - c.udp_jitter_good)
            END
          END, 0.0) * c.pipe2_w_lat_udpjitter
      )
      /
      (
        (CASE WHEN m.tcp_pkts > 0 THEN c.pipe2_w_rel_retrans ELSE 0 END) +
        (CASE WHEN m.tcp_pkts > 0 THEN c.pipe2_w_rel_outseq  ELSE 0 END) +
        (CASE WHEN m.quic_pkts > 0 THEN c.pipe2_w_rel_quic_loss  ELSE 0 END) +
        (CASE WHEN m.quic_pkts > 0 THEN c.pipe2_w_rel_quic_outseq ELSE 0 END) +
        (CASE WHEN m.rtt_ms IS NOT NULL THEN c.pipe2_w_lat_rtt ELSE 0 END) +
        (CASE WHEN m.rtt_tail_pct IS NOT NULL THEN c.pipe2_w_lat_rttlong ELSE 0 END) +
        (CASE WHEN m.udp_jitter_ms IS NOT NULL THEN c.pipe2_w_lat_udpjitter ELSE 0 END)
      )
    END AS Pipe_Score
  FROM pipe_metrics m
  CROSS JOIN cfg c
)
SELECT
	f.time,
	f.cgisai,
	f.cell_name,
	CASE
	WHEN f.Pipe_Score IS NULL THEN 'No_Data'
	WHEN f.Pipe_Score >= c.pipe_class_excellent THEN 'Excellent'
	WHEN f.Pipe_Score >= c.pipe_class_good      THEN 'Good'
	WHEN f.Pipe_Score >= c.pipe_class_fair      THEN 'Fair'
	ELSE 'Poor'
	END AS Pipe_Class,
	avg( f.Pipe_Score) as avg_pipe_score
FROM final_pscore f
CROSS JOIN cfg c
where Pipe_Score >= 0
group by 1,2,3,4
order by f.time"""

DQI_Queries={
  "dqi_script":dqi_script_1
}