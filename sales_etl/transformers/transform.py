if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
from mage_ai.shared.hash import ignore_keys


def apply_promotions(df, promotions):
    """
    Apply promotions to the sales data
    """
    df['discount_amount'] = 0
    for _, promo in promotions.iterrows():
        mask = (df['sales_date'] >= pd.to_datetime(promo['start_date'])) & (df['sales_date'] <= pd.to_datetime(promo['end_date']))
        df.loc[mask, 'discount_amount'] = df.loc[mask, 'price'] * promo['disc_value']
        df.loc[mask, 'discount_amount'] = df.loc[mask, 'discount_amount'].clip(upper=promo['max_disc'])
    return df


def create_sales_trends(df, freq):
    """
    Create sales trends for a given frequency
    """
    trends = df.groupby(df['sales_date'].dt.to_period(freq)).agg({
        'gross_revenue': 'sum',
        'total_discount': 'sum',
        'cogs': 'sum',
        'net_profit': 'sum'
    }).reset_index()
    
    # Convert Period to string for JSON serialization
    trends['sales_date'] = trends['sales_date'].astype(str)
    return trends

def create_ytd_sales(df):
    """
    Create Year-To-Date sales
    """
    df['year'] = df['sales_date'].dt.year
    ytd_sales = df.groupby('year').agg({
        'gross_revenue': 'sum',
        'total_discount': 'sum',
        'cogs': 'sum',
        'net_profit': 'sum'
    }).reset_index()
    
    # Convert year to string for JSON serialization
    ytd_sales['year'] = ytd_sales['year'].astype(str)
    return ytd_sales
















@transformer
#def transform(data, *args, **kwargs):
def transform(data, *args, **kwargs):
    """
    Transform the loaded data to create the data mart
    """
    menu_df = data['menu']
    order_df = data['order']
    promotion_df = data['promotion']

    # Ensure sales_date is in datetime format
    order_df['sales_date'] = pd.to_datetime(order_df['sales_date'], errors='coerce')

    # Remove duplicates from order data
    order_df = order_df.groupby(['order_id', 'menu_id', 'quantity', 'sales_date']).first().reset_index()


    merged_df = pd.merge(order_df, menu_df, on='menu_id')


    merged_df = apply_promotions(merged_df, promotion_df)

    # Calculate metrics
    merged_df['gross_revenue'] = merged_df['quantity'] * merged_df['price']
    merged_df['total_discount'] = merged_df['quantity'] * merged_df['discount_amount']
    merged_df['cogs'] = merged_df['quantity'] * merged_df['cogs']
    merged_df['net_profit'] = merged_df['gross_revenue'] - merged_df['total_discount'] - merged_df['cogs']


    daily_sales = create_sales_trends(merged_df, 'D')

    # Create weekly sales trends
    weekly_sales = create_sales_trends(merged_df, 'W')

    # Create YTD sales
    ytd_sales = create_ytd_sales(merged_df)

    return {
        'daily_sales': daily_sales,
        'weekly_sales': weekly_sales,
        'ytd_sales': ytd_sales
    }



    


