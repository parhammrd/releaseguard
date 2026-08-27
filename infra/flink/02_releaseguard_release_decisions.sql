INSERT INTO `releaseguard_release_decisions` (
  `key`,
  `decision_id`,
  `release_id`,
  `decision`,
  `reason_code`,
  `reason`,
  `stable_error_rate`,
  `canary_error_rate`,
  `stable_avg_latency_ms`,
  `canary_avg_latency_ms`,
  `decided_at`,
  `source`
)
SELECT
  CONCAT('dec_', `release_id`, '_', DATE_FORMAT(`window_end`, 'yyyyMMddHHmmssSSS')),
  CONCAT('dec_', `release_id`, '_', DATE_FORMAT(`window_end`, 'yyyyMMddHHmmssSSS')),
  `release_id`,
  'ROLLBACK',
  CASE
    WHEN `canary_error_rate` - `stable_error_rate` >= 0.05
      AND `canary_error_rate` >= 2.0 * `stable_error_rate`
      THEN 'ERROR_RATE_REGRESSION'
    ELSE 'LATENCY_REGRESSION'
  END,
  CASE
    WHEN `canary_error_rate` - `stable_error_rate` >= 0.05
      AND `canary_error_rate` >= 2.0 * `stable_error_rate`
      THEN 'Canary error rate is at least 5 points and 2x worse than stable'
    ELSE 'Canary latency is at least 1.75x and 150 ms worse than stable'
  END,
  `stable_error_rate`,
  `canary_error_rate`,
  `stable_avg_latency_ms`,
  `canary_avg_latency_ms`,
  CURRENT_ROW_TIMESTAMP(),
  'flink'
FROM `releaseguard_window_health`
/*+ OPTIONS(
  'kafka.consumer.isolation-level' = 'read-uncommitted',
  'scan.startup.mode' = 'latest-offset'
) */
WHERE
  `stable_request_count` >= 50
  AND `canary_request_count` >= 10
  AND (
    (
      `canary_error_rate` - `stable_error_rate` >= 0.05
      AND `canary_error_rate` >= 2.0 * `stable_error_rate`
    )
    OR (
      `canary_avg_latency_ms` >= 1.75 * `stable_avg_latency_ms`
      AND `canary_avg_latency_ms` - `stable_avg_latency_ms` >= 150.0
    )
  );
