drop table amplitude.braze_user;

CREATE TABLE amplitude.braze_user AS
WITH
ref AS (SELECT DATE '2025-10-28' AS d),

user_business_one AS (
  SELECT user_id, MAX(business) AS business
  FROM amplitude.user_business
  GROUP BY user_id
),

-- 2) 지원 이력 → applied_business(JSON 배열 문자열)
applicants AS (
  SELECT "user"."onlineUserId" AS external_id, 'kdt' AS applied
  FROM public.dbnbcamp_applicants
  UNION ALL
  SELECT "user"."onlineUserId" AS external_id, 'hh' AS applied
  FROM public.dbhanghae_v3_applicants
),
applied_business AS (
  SELECT
    external_id,
    '[' || LISTAGG(DISTINCT '"' || applied || '"', ',')
          WITHIN GROUP (ORDER BY applied)
    || ']' AS applied_business,
    -- 첫 번째 applied 값을 business fallback으로 사용 (business 누락 케이스 대응)
    MIN(applied) AS first_applied_business
  FROM applicants
  GROUP BY external_id
),

/* ---------------- in_progress ---------------- */
-- 3) KDT 진행중 과정
kdt_inprog_raw AS (
  SELECT
    m.online_user_id AS external_id,
    da.marketingRoundTitle AS course_name
  FROM mart_nbc_applicant_funnel m
  JOIN public.dbnbcamp_applicants da
    ON da."user"."onlineUserId" = m.online_user_id
   AND (
         CAST(da."round"."startDate" AS DATE) = CAST(m.round_start_date AS DATE)
      OR da."user"."onlineUserId" IN (
           '680bc15674d76a15f9164d0b','667134541cc4bfb45667ad32','60b3debebca425cf0f247f82',
           '680f72ae5270c34d19370a67','6745d26a01d61f5e5ec9a911','67fe3e84c40a63174ed0dd15',
           '6805cdb15885e61132e8c4e9'
         )
       )
  CROSS JOIN ref r
  WHERE m.round_start_date < r.d
    AND m.round_end_date   > r.d
    AND m.status IN ('최종합류', '최종합류 D1')
    AND da."funnel"."context"."title" IN ('최종합류', '최종합류 D1')
),
kdt_inprog AS (
  SELECT DISTINCT external_id, course_name
  FROM kdt_inprog_raw
),

-- 4) HH 진행중 과정
hh_inprog_raw AS (
  SELECT
    online_user_id AS external_id,
    (product_title || ' ' || round_title) AS course_name
  FROM mart_hh_applicant_funnel, ref r
  WHERE round_start_date < r.d
    AND round_end_date   > r.d
    AND funnel_status = '결제완료'
),
hh_inprog AS (
  SELECT DISTINCT external_id, course_name
  FROM hh_inprog_raw
),

-- 5) 진행중 과정 JSON 배열 문자열
in_progress AS (
  SELECT
    external_id,
    '[' ||
      LISTAGG(DISTINCT '"' || course_name || '"', ',')
        WITHIN GROUP (ORDER BY course_name)
    || ']' AS in_progress_business
  FROM (
    SELECT external_id, course_name FROM kdt_inprog
    UNION ALL
    SELECT external_id, course_name FROM hh_inprog
  ) x
  GROUP BY external_id
),

/* ---------------- completed_business ---------------- */
-- 6) KDT 완료(과거 종료) 과정
kdt_completed_raw AS (
  SELECT DISTINCT
    m.online_user_id       AS external_id,
    da.marketingRoundTitle AS course_name
  FROM mart_nbc_applicant_funnel m
  JOIN public.dbnbcamp_applicants da
    ON da."user"."onlineUserId" = m.online_user_id
   AND CAST(da."round"."startDate" AS DATE) = CAST(m.round_start_date AS DATE)
  CROSS JOIN ref r
  WHERE m.round_end_date < r.d
    AND m.status IN ('최종합류', '최종합류 D1', '취업 준비중', '취업 모수 제외',
                     '취업포기', '지원중', '취업보류', '취업완료')
    AND da."funnel"."context"."title" IN ('최종합류', '최종합류 D1', '취업 준비중',
                                       '취업 모수 제외', '취업포기', '지원중',
                                       '취업보류', '취업현황제출', '취업완료')
),
kdt_completed AS (
  SELECT DISTINCT external_id, course_name FROM kdt_completed_raw
),

-- 7) HH 완료(과거 종료) 과정
hh_completed_raw AS (
  SELECT DISTINCT
    online_user_id AS external_id,
    (product_title || ' ' || round_title) AS course_name
  FROM mart_hh_applicant_funnel m, ref r
  WHERE CAST(m.round_end_date AS DATE) < r.d
    AND m.funnel_status = '결제완료'
),
hh_completed AS (
  SELECT DISTINCT external_id, course_name FROM hh_completed_raw
),

-- 8) 완료 과정 JSON 배열 문자열
completed AS (
  SELECT
    external_id,
    '[' ||
      LISTAGG(DISTINCT '"' || course_name || '"', ',')
        WITHIN GROUP (ORDER BY course_name)
    || ']' AS completed_business
  FROM (
    SELECT external_id, course_name FROM kdt_completed
    UNION ALL
    SELECT external_id, course_name FROM hh_completed
  ) x
  GROUP BY external_id
),

/* ---------------- has_card ---------------- */
has_card_from_mart AS (
  SELECT
    online_user_id AS external_id,
    MAX(CASE WHEN COALESCE(has_card_yn, 'false') = 'true' THEN 1 ELSE 0 END)::boolean AS has_card
  FROM public.mart_nbc_applicant_funnel
  GROUP BY online_user_id
),
has_card_from_gov AS (
  SELECT
    user_id AS external_id,
    MAX(CASE WHEN status = 'has_card' THEN 1 ELSE 0 END)::boolean AS has_card
  FROM public.dbonline_v2_gov_user_card_status
  GROUP BY user_id
),
has_card_info AS (
  SELECT
    COALESCE(m.external_id, g.external_id) AS external_id,
    COALESCE(m.has_card, FALSE) OR COALESCE(g.has_card, FALSE) AS has_card
  FROM has_card_from_mart m
  FULL OUTER JOIN has_card_from_gov g
    ON m.external_id = g.external_id
),

/* ---------------- 최신 퍼널 오브젝트: KDT(NBC) ---------------- */
kdt_latest_funnel AS (
  SELECT
    da."user"."onlineUserId"                     AS external_id,
    da."funnel"."title"::varchar       AS kdt_funnel_stage,
    updatedAt     AS funnel_updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY da."user"."onlineUserId"
      ORDER BY updatedAt DESC
    ) AS rn
  FROM public.dbnbcamp_applicants da
),
kdt_latest_funnel_one AS (
  SELECT
    external_id,
    -- JSON 오브젝트 문자열
    '{' ||
      '"funnel_name":"'      || REPLACE(kdt_funnel_stage, '"', '\"') || '",' ||
      '"funnel_updated_at":"'  ||
         TO_CHAR(
           CONVERT_TIMEZONE('Asia/Seoul','UTC', funnel_updated_at),
           'YYYY-MM-DD"T"HH24:MI:SS"Z"'
         )
      || '"'
    || '}' AS nbc_funnel
  FROM kdt_latest_funnel
  WHERE rn = 1
),

/* ---------------- 최신 퍼널 오브젝트: HH(항해) ---------------- */
hh_latest_funnel AS (
  SELECT
    ha."user"."onlineUserId"                                 AS external_id,
    ha.funnel                                               AS hh_funnel_id,
    updatedAt               AS funnel_updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY ha."user"."onlineUserId"
      ORDER BY updatedAt DESC
    ) AS rn
  FROM public.dbhanghae_v3_applicants ha
),
hh_latest_funnel_join AS (
  SELECT
    h.external_id,
    h.hh_funnel_id,
    f.name                                                  AS hh_funnel_stage,
    h.funnel_updated_at
  FROM hh_latest_funnel h
  LEFT JOIN public.dbhanghae_v3_funnels f
    ON f._id = h.hh_funnel_id
  WHERE h.rn = 1
),
hh_latest_funnel_one AS (
  SELECT
    external_id,
    '{' ||
      '"funnel_name":"'      || REPLACE(hh_funnel_stage, '"', '\"') || '",' ||
      '"funnel_updated_at":"'  ||
         TO_CHAR(
           CONVERT_TIMEZONE('Asia/Seoul','UTC', funnel_updated_at),
           'YYYY-MM-DD"T"HH24:MI:SS"Z"'
         )
      || '"'
    || '}' AS hh_funnel
  FROM hh_latest_funnel_join
)

/* ---------------- user_base ---------------- */
, user_base AS (
  SELECT
    u._id AS external_id,
    name AS first_name,
    u.birthyear AS birthyear,
    u.birthday AS birthday,
    CASE
      WHEN country_code = '+82'
        THEN '+82' || REGEXP_REPLACE(phone, '^0', '')
      ELSE country_code || phone
    END AS phone,
    email AS email,
    'active' AS user_type,
    u.marketing AS is_marketing,
    CAST(COALESCE(u.created_at, u.last_login_at, u.marketing_date) AS DATE) AS signup_date,
    -- business가 없으면 applied_business의 첫 번째 값을 fallback으로 사용 (41,427명 커버)
    COALESCE(b.business, ab.first_applied_business) AS business,
    COALESCE(ab.applied_business,   '[]') AS applied_business,
    COALESCE(ip.in_progress_business,'[]') AS in_progress_business,
    COALESCE(c.completed_business,  '[]') AS completed_business,
    COALESCE(u.is_test, FALSE) AS is_test,
    COALESCE(k.nbc_funnel, '{}') AS kdt_funnel_stage,
    COALESCE(h.hh_funnel,  '{}') AS hh_funnel_stage
  FROM public.dbonline_v2_users u
  LEFT JOIN user_business_one b ON u._id = b.user_id
  LEFT JOIN applied_business ab ON u._id = ab.external_id
  LEFT JOIN in_progress ip      ON u._id = ip.external_id
  LEFT JOIN completed c         ON u._id = c.external_id
  LEFT JOIN kdt_latest_funnel_one k ON u._id = k.external_id
  LEFT JOIN hh_latest_funnel_one  h ON u._id = h.external_id
)

-- 최종
SELECT
  ub.*,
  COALESCE(hc.has_card, FALSE) AS has_card
FROM user_base ub
LEFT JOIN has_card_info hc
  ON ub.external_id = hc.external_id;