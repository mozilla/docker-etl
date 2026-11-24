# from google.cloud import bigquery
# import pandas as pd
import os
print(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
# client = bigquery.Client('mozdata')

# def run(sql_statement: str) -> pd.DataFrame:
#         query = client.query(sql_statement)
#         return query.result().to_dataframe()

# # Load data from disk
# df_all_models = pd.read_csv('04_all_models.tsv', sep='\t')
# df_all_models['plan_type'] = df_all_models['plan_type'].astype(str)

# df = df_all_models.copy()
# df['new_revenue_today'] = df['total_revenue'].combine_first(df['total_revenue_truth'])
# df['new_accounts_today'] = df['total_accounts'].combine_first(df['total_accounts_truth'])
# #df = df[df['model_version'] == '2025-05-01']

# # Retain only relevant fields
# df = df[['model_version', 't', 'product_name', 'plan_type', 'new_revenue_today', 'new_accounts_today']].reset_index(drop=True)


# job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
# job = client.load_table_from_dataframe(df, 'mozdata.revenue_cat2_analysis.subscription_services_v3_models', job_config=job_config)
# job.result()

# with open('05b_subscription_services_v3_forecasting.sql') as f:
#     sql2 = f.read()

# run(sql2)

# with open('05c_subscription_services_v3_forecasting_ltv.sql') as f:
#     sql3 = f.read()

# run(sql3)