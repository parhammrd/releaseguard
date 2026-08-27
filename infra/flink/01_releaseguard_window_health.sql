INSERT INTO `releaseguard_window_health` (
  `release_id`,
  `window_start`,
  `window_end`,
  `stable_request_count`,
  `stable_error_count`,
  `stable_error_rate`,
  `stable_avg_latency_ms`,
  `canary_request_count`,
  `canary_error_count`,
  `canary_error_rate`,
  `canary_avg_latency_ms`
)
SELECT
  `release_id`,
  `window_start`,
  `window_end`,
  COUNT(CASE WHEN `cohort` = 'stable' THEN 1 END),
  SUM(CASE WHEN `cohort` = 'stable' AND `status_code` >= 500 THEN 1 ELSE 0 END),
  CAST(SUM(CASE WHEN `cohort` = 'stable' AND `status_code` >= 500 THEN 1 ELSE 0 END) AS DOUBLE)
    / CAST(COUNT(CASE WHEN `cohort` = 'stable' THEN 1 END) AS DOUBLE),
  AVG(CASE WHEN `cohort` = 'stable' THEN CAST(`latency_ms` AS DOUBLE) END),
  COUNT(CASE WHEN `cohort` = 'canary' THEN 1 END),
  SUM(CASE WHEN `cohort` = 'canary' AND `status_code` >= 500 THEN 1 ELSE 0 END),
  CAST(SUM(CASE WHEN `cohort` = 'canary' AND `status_code` >= 500 THEN 1 ELSE 0 END) AS DOUBLE)
    / CAST(COUNT(CASE WHEN `cohort` = 'canary' THEN 1 END) AS DOUBLE),
  AVG(CASE WHEN `cohort` = 'canary' THEN CAST(`latency_ms` AS DOUBLE) END)
FROM TABLE(
  HOP(
    TABLE `releaseguard_service_metrics`,
    DESCRIPTOR($rowtime),
    INTERVAL '2' SECOND,
    INTERVAL '6' SECOND
  )
)
GROUP BY `release_id`, `window_start`, `window_end`
HAVING
  COUNT(CASE WHEN `cohort` = 'stable' THEN 1 END) > 0
  AND COUNT(CASE WHEN `cohort` = 'canary' THEN 1 END) > 0;
