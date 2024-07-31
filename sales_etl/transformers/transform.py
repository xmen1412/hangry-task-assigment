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


def merge_order_menu(order_df, menu_df):
    """
    Merge order and menu data, considering the effective date
    """
    # Sort menu_df by effective_date to ensure we get the most recent price/cogs
    menu_df = menu_df.sort_values('effective_date')
    
    # Merge based on menu_id and effective_date
    merged_df = pd.merge_asof(order_df.sort_values('sales_date'),
                              menu_df,
                              left_on='sales_date',
                              right_on='effective_date',
                              by='menu_id',
                              direction='backward')
    
    return merged_df


# def create_sales_trends(df, freq):
#     """
#     Create sales trends for a given frequency
#     """
#     trends = df.groupby(df['sales_date'].dt.to_period(freq)).agg({
#         'gross_revenue': 'sum',
#         'total_discount': 'sum',
#         'cogs': 'sum',
#         'net_profit': 'sum'
#     }).reset_index()
    
#     # Convert Period to string for JSON serialization
#     trends['sales_date'] = trends['sales_date'].astype(str)
#     return trends

# def create_ytd_sales(df):
#     """
#     Create Year-To-Date sales
#     """
#     df['year'] = df['sales_date'].dt.year
#     ytd_sales = df.groupby('year').agg({
#         'gross_revenue': 'sum',
#         'total_discount': 'sum',
#         'cogs': 'sum',
#         'net_profit': 'sum'
#     }).reset_index()
    
#     # Convert year to string for JSON serialization
#     ytd_sales['year'] = ytd_sales['year'].astype(str)
#     return ytd_sales


def create_brand_sales_trends(df, freq):
    """
    Create brand-specific sales trends for a given frequency
    """
    trends =  df.groupby([df['sales_date'].dt.to_period(freq), 'brand']).agg({
        'gross_revenue': 'sum',
        'total_discount': 'sum',
        'cogs': 'sum',
        'net_profit': 'sum'
    }).reset_index()


    trends['sales_date'] = trends['sales_date'].astype(str)

    return trends

def create_brand_ytd_sales(df):
    """
    Create brand-specific Year-To-Date sales
    """
    df['year'] = df['sales_date'].dt.year
    ytd_sales =  df.groupby(['year', 'brand']).agg({
        'gross_revenue': 'sum',
        'total_discount': 'sum',
        'cogs': 'sum',
        'net_profit': 'sum'
    }).reset_index()

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

    # Standardize date formats
    order_df['sales_date'] = pd.to_datetime(order_df['sales_date']).dt.strftime('%Y-%m-%d')
    menu_df['effective_date'] = pd.to_datetime(menu_df['effective_date']).dt.strftime('%Y-%m-%d')
    promotion_df['start_date'] = pd.to_datetime(promotion_df['start_date']).dt.strftime('%Y-%m-%d')
    promotion_df['end_date'] = pd.to_datetime(promotion_df['end_date']).dt.strftime('%Y-%m-%d')

    # Convert date columns back to datetime for processing
    order_df['sales_date'] = pd.to_datetime(order_df['sales_date'])
    menu_df['effective_date'] = pd.to_datetime(menu_df['effective_date'])
    promotion_df['start_date'] = pd.to_datetime(promotion_df['start_date'])
    promotion_df['end_date'] = pd.to_datetime(promotion_df['end_date'])


    # Merge order and menu data
    merged_df = merge_order_menu(order_df, menu_df)

    # Apply promotions
    merged_df = apply_promotions(merged_df, promotion_df)

    # Calculate metrics
    merged_df['gross_revenue'] = merged_df['quantity'] * merged_df['price']
    merged_df['total_discount'] = merged_df['quantity'] * merged_df['discount_amount']
    merged_df['total_cogs'] = merged_df['quantity'] * merged_df['cogs']
    merged_df['net_profit'] = merged_df['gross_revenue'] - merged_df['total_discount'] - merged_df['total_cogs']


    daily_sales = create_brand_sales_trends(merged_df, 'D')

    # Create weekly sales trends
    weekly_sales = create_brand_sales_trends(merged_df, 'W')

    # Create YTD sales
    ytd_sales = create_brand_ytd_sales(merged_df)

    return {
        'daily_sales': daily_sales,
        'weekly_sales': weekly_sales,
        'ytd_sales': ytd_sales,
        'raw_sales' : merged_df
    }



    


