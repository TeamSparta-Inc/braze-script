DROP TABLE amplitude.user_business;

-- 가입 경로 적재용
CREATE TABLE amplitude.user_business
AS (
  SELECT
    user_id,
    nullif(event_properties."brand"::varchar, '') AS business
  FROM amplitude.events_453928
  WHERE event_type = 'signup_completed'

  UNION ALL

  SELECT
    user_id,
    'sc' AS business
  FROM amplitude.events_453928
  WHERE event_type = 'scc_signup_completed'

  UNION ALL

  SELECT
    user_id,
    'kdt' AS business
  FROM amplitude.events_453928
  WHERE event_type = 'nbc_signup_completed'

  UNION ALL

  SELECT
    user_id,
    'hh' AS business
  FROM amplitude.events_453928
  WHERE event_type = 'hh_signup_completed'

  UNION ALL

  SELECT
    user_id,
    nullif(event_properties."business"::varchar, 'sc') AS business
  FROM amplitude.events_731525
  WHERE event_type = 'main_signup_join_completed'
);