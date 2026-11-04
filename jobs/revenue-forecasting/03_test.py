from google.cloud import bigquery
import numpy as np
import pandas as pd

def main():
    SQL_TABLE = 'mozdata.revenue_cat2_analysis.daily_active_logical_subscriptions_rebuilt_20250611'
    YEARLY_PLAN = '12'
    MONTHLY_PLAN = '1'
    PLAN_TYPES = [YEARLY_PLAN, MONTHLY_PLAN]
    
    MOZILLA_VPN = 'Mozilla VPN'
    MOZILLA_MONITOR = 'Mozilla Monitor Plus'
    PRODUCTS = [
        (MOZILLA_VPN, YEARLY_PLAN),
        (MOZILLA_VPN, MONTHLY_PLAN),
        (MOZILLA_MONITOR, YEARLY_PLAN),
        (MOZILLA_MONITOR, MONTHLY_PLAN),
        ('Relay Premium', YEARLY_PLAN),
        ('Relay Premium', MONTHLY_PLAN),
        ('Mozilla VPN & Firefox Relay', YEARLY_PLAN),
        ('MDN Plus', YEARLY_PLAN),
        ('MDN Plus', MONTHLY_PLAN),
    ]
    
    DB_START_DATE = '2022-01-01'
    DB_END_DATE = '2025-06-01'
    OUTPUT_TSV = '03_compare_with_previous_period.tsv'
    
    client = bigquery.Client('mozdata')
    
    def run(sql_statement: str) -> pd.DataFrame:
        query = client.query(sql_statement)
        return query.result().to_dataframe()
    
    # Load subscription data
    def load_subscription_data() -> pd.DataFrame:
        df = run(f"""
    SELECT
      product_name AS product_name,
      plan_interval_months AS plan_type,
      active_date AS t,
      is_renewed_subscription AS is_renewal,
      SUM(total_period_amount_usd) AS revenue,
      COUNT(*) AS accounts
    FROM `{SQL_TABLE}`
    WHERE
      DATE('{DB_START_DATE}') <= active_date AND is_first_day_of_period
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
    """)
        df['t'] = pd.to_datetime(df['t'])
        df['plan_type'] = df['plan_type'].astype(str)
        return df
    
    df_raw = load_subscription_data()
    
    # # Print summary information
    # print('Total revenue = ${:,.2f}'.format(sum(df_raw['revenue'])))
    
    # print("\nList of products:")
    # for product_name in df_raw['product_name'].unique():
    #     print(product_name)
    
    # print("\nRevenue by product and plan type:")
    # print(df_raw.groupby(['product_name', 'plan_type']).aggregate({
    #     'revenue': 'sum',
    #     'accounts': 'sum',
    # }).sort_values(by=['revenue'], ascending=False))
    
    # Align two years of data to compute year-of-year ratio
    def compare_current_vs_previous_periods(df_raw: pd.DataFrame, product_name: str, plan_type: int) -> pd.DataFrame:
        print(f'\nproduct_name = {product_name}')
        print(f'plan_type = {plan_type}')
        assert plan_type in PLAN_TYPES
        df_service = df_raw[(df_raw['product_name'] == product_name) & (df_raw['plan_type'] == plan_type)]
        
        if df_service.empty:
            print(f"No data found for {product_name} with plan type {plan_type}")
            return pd.DataFrame()

        # Baseline period
        current_end_date = pd.to_datetime(DB_END_DATE) - pd.Timedelta(days=1)

        # Reference or "previous" period
        if plan_type == YEARLY_PLAN:
            current_start_date = df_service['t'].min() + pd.DateOffset(years=1)
            previous_start_date = current_start_date - pd.DateOffset(years=1)
            previous_end_date = current_end_date - pd.DateOffset(years=1)
        else:
            current_start_date = df_service['t'].min() + pd.DateOffset(months=1)
            previous_start_date = current_start_date - pd.DateOffset(months=1)
            previous_end_date = current_end_date - pd.DateOffset(months=1)

        # Extract data for current and previous periods
        df_current = df_service[df_service['t'].between(current_start_date, current_end_date)].reset_index(drop=True)
        df_previous = df_service[df_service['t'].between(previous_start_date, previous_end_date)].reset_index(drop=True)

        # Split current period into new subscriptions and renewals
        df_current_new = df_current[df_current['is_renewal'] == False].drop(columns='is_renewal').reset_index(drop=True)
        df_current_renewal = df_current[df_current['is_renewal'] == True].drop(columns='is_renewal').reset_index(drop=True)
        
        # print('Current period new subscription = ${:>14,.2f} in {} days from {} to {}'.format(
        #     sum(df_current_new['revenue']), 
        #     df_current_new['t'].nunique(), 
        #     min(df_current_new['t']), 
        #     max(df_current_new['t'])
        # ))
        # print('        Current period renewals = ${:>14,.2f} in {} days from {} to {}'.format(
        #     sum(df_current_renewal['revenue']), 
        #     df_current_renewal['t'].nunique(), 
        #     min(df_current_renewal['t']), 
        #     max(df_current_renewal['t'])
        # ))

        # Count all previous period
        df_previous_all = df_previous.groupby(['t']).agg({
            'revenue': 'sum',
            'accounts': 'sum',
            'product_name': 'first',
            'plan_type': 'first',
        }).reset_index()
        print('                Previous period = ${:>14,.2f} in {} days from {} to {}'.format(
            sum(df_previous_all['revenue']), 
            df_previous_all['t'].nunique(), 
            min(df_previous_all['t']), 
            max(df_previous_all['t'])
        ))

        # Shift previous period onto the current period
        if plan_type == YEARLY_PLAN:
            df_previous_all['t'] = df_previous_all['t'].apply(lambda t: t + pd.DateOffset(years=1))
        else:
            df_previous_all['t'] = df_previous_all['t'].apply(lambda t: t + pd.DateOffset(months=1))
        df_previous_all = df_previous_all.groupby(['t']).agg({
            'revenue': 'sum',
            'accounts': 'sum',
            'product_name': 'first',
            'plan_type': 'first',
        }).reset_index()
        print('        Previous period shifted = ${:>14,.2f} in {} days from {} to {}'.format(
            sum(df_previous_all['revenue']), 
            df_previous_all['t'].nunique(), 
            min(df_previous_all['t']), 
            max(df_previous_all['t'])
        ))

        # Merge into a single table
        df_current_new.rename(columns={
            'revenue': 'current_new_revenue',
            'accounts': 'current_new_accounts',
        }, inplace=True)
        df_current_renewal.rename(columns={
            'revenue': 'current_renewal_revenue',
            'accounts': 'current_renewal_accounts',
        }, inplace=True)
        df_previous_all.rename(columns={
            'revenue': 'previous_all_revenue',
            'accounts': 'previous_all_accounts',
        }, inplace=True)
        
        df = pd.merge(df_current_renewal, df_current_new, on=['t', 'product_name', 'plan_type'], how='inner')
        df = pd.merge(df, df_previous_all, on=['t', 'product_name', 'plan_type'], how='inner')

        # Fill in missing values
        all_dates = pd.date_range(start=current_start_date, end=current_end_date)
        all_date_indexes = pd.Index(all_dates, name='t')
        df = df.set_index('t').reindex(all_date_indexes).fillna({
            'product_name': product_name,
            'plan_type': plan_type,
            'current_new_revenue': 0.0,
            'current_new_accounts': 0.0,
            'current_renewal_revenue': 0.0,
            'current_renewal_accounts': 0.0,
            'previous_all_revenue': 0.0,
            'previous_all_accounts': 0.0,
        }).reset_index()
        df = df.sort_values(by='t')

        # Calculate % renewal rate
        df['renewal_weight'] = 365 * (df['current_renewal_accounts'] + df['previous_all_accounts']) / (sum(df['current_renewal_accounts']) + sum(df['previous_all_accounts']))
        df['renewal_ratio'] = df['current_renewal_accounts'] / df['previous_all_accounts']
        print('Renewal rate: {:.4f}%'.format(100 / 365 * sum(df['renewal_ratio'] * df['renewal_weight'])))
        
        return df
    
    # Compare VPN yearly and monthly
    df_compare_vpn_yearly = compare_current_vs_previous_periods(df_raw, MOZILLA_VPN, YEARLY_PLAN)
    df_compare_vpn_monthly = compare_current_vs_previous_periods(df_raw, MOZILLA_VPN, MONTHLY_PLAN)
    
    # # Display results
    # if not df_compare_vpn_yearly.empty:
    #     print("\nVPN Yearly Comparison:")
    #     print(df_compare_vpn_yearly.head())
    
    # if not df_compare_vpn_monthly.empty:
    #     print("\nVPN Monthly Comparison:")
    #     print(df_compare_vpn_monthly.head())
    
    # Compare all services
    def compare_all_services(df_raw: pd.DataFrame) -> pd.DataFrame:
        df_all = pd.DataFrame()
        for (product_name, plan_type) in PRODUCTS:
            df = compare_current_vs_previous_periods(df_raw, product_name, plan_type)
            if not df.empty:
                df_all = pd.concat([df_all, df], ignore_index=True)
        return df_all
    
    df_compare_all = compare_all_services(df_raw)
    
    # Save results to TSV
    if not df_compare_all.empty:
        df_compare_all.to_csv(OUTPUT_TSV, sep='\t', index=False)
        print(f"\nResults saved to {OUTPUT_TSV}")
    else:
        print("\nNo comparison data was generated")

if __name__ == "__main__":
    main()