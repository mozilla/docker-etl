CREATE OR REPLACE TABLE mozdata.revenue_cat2_analysis.subscription_services_v3_forecasting_ltv AS (
  SELECT
    model_version,
    product_name,
    t,
    amortized_revenue,
    amortized_accounts,
    SUM(amortized_revenue) OVER (
      PARTITION BY model_version, product_name
      ORDER BY t
      ROWS BETWEEN CURRENT ROW AND 729 FOLLOWING
    ) AS ltv_revenue

  FROM mozdata.revenue_cat2_analysis.subscription_services_v3_forecasting
  ORDER BY model_version, product_name, t
);