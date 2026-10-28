CREATE OR REPLACE TABLE mozdata.revenue_cat2_analysis.subscription_services_v3_forecasting AS (

  WITH expanded AS (
    SELECT
      DATE(model_version) AS model_version,
      product_name,
      DATE(t) AS original_date,
      DATE_ADD(DATE(t), INTERVAL OFFSET DAY) AS t,
      new_revenue_today / 365.0 AS daily_amortized_revenue,
      new_accounts_today AS daily_amortized_accounts
    FROM mozdata.revenue_cat2_analysis.subscription_services_v3_models, UNNEST(GENERATE_ARRAY(0, 364)) AS offset
    WHERE plan_type = '12'

    UNION ALL

    SELECT
      DATE(model_version) AS model_version,
      product_name,
      DATE(t) AS original_date,
      DATE_ADD(DATE(t), INTERVAL OFFSET DAY) AS t,
      new_revenue_today / 30.0 AS daily_amortized_revenue,
      new_accounts_today AS daily_amortized_accounts
    FROM mozdata.revenue_cat2_analysis.subscription_services_v3_models, UNNEST(GENERATE_ARRAY(0, 30)) AS offset
    WHERE plan_type = '1'

  ),

  amortized AS (
    SELECT
      model_version,
      product_name,
      t,
      SUM(daily_amortized_revenue) AS amortized_revenue,
      SUM(daily_amortized_accounts) AS amortized_accounts
    FROM expanded
    GROUP BY product_name, model_version, t
  )

  SELECT
    model_version,
    product_name,
    t,
    amortized_revenue,
    amortized_accounts
  FROM amortized
  WHERE t >= model_version
  ORDER BY model_version, product_name, t
);
