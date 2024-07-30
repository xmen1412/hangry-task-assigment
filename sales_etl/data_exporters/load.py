from mage_ai.io.file import FileIO
import pandas as pd
import os
import matplotlib.pyplot as plt
from pandas import DataFrame





if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    """
    Export the data to CSV files and create plots.
    """
    # Get the current working directory
    current_dir = os.getcwd()
    print(f"Current working directory: {current_dir}")

    # Specify the directory where you want to save the CSV files and plots
    output_dir = os.path.join(current_dir, 'output')

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    print(f"Created output directory: {output_dir}")



    """Load to Visualisation is optional"""

    # Export daily sales
    export_to_csv(data['daily_sales'], 'daily_sales.csv', output_dir)
    plot_sales(data['daily_sales'], 'Daily Sales Trend', 'daily_sales_plot.png', output_dir)

    # Export weekly sales
    export_to_csv(data['weekly_sales'], 'weekly_sales.csv', output_dir)
    plot_sales(data['weekly_sales'], 'Weekly Sales Trend', 'weekly_sales_plot.png', output_dir)

    # Export YTD sales
    export_to_csv(data['ytd_sales'], 'ytd_sales.csv', output_dir)
    plot_ytd_sales(data['ytd_sales'], 'YTD Sales', 'ytd_sales_plot.png', output_dir)

    print(f"All files should be saved in: {output_dir}")
    print(f"Contents of the output directory: {os.listdir(output_dir)}")



def gcs_loader(df: DataFrame, filename: str, bucket: str):
    """
    a function to load the data to. google cloud platform
    """

    pass


def write_to_bq(df: DataFrame,dataset_id:str,table_id:str ):
    """

    can upload a data to bigquery

    """
    pass

def export_to_csv(df: DataFrame, filename: str, directory: str):
    """
    Export a DataFrame to a CSV file.
    """
    filepath = os.path.join(directory, filename)
    df.to_csv(filepath, index=False)
    print(f"Exported {filename} to {filepath}")
    print(f"File exists: {os.path.exists(filepath)}")
    print(f"File size: {os.path.getsize(filepath)} bytes")

def plot_sales(df: DataFrame, title: str, filename: str, directory: str):
    """
    Create a line plot of sales trends.
    """
    plt.figure(figsize=(12, 6))
    plt.plot(df['sales_date'], df['gross_revenue'], label='Gross Revenue')
    plt.plot(df['sales_date'], df['net_profit'], label='Net Profit')
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Amount')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    filepath = os.path.join(directory, filename)
    plt.savefig(filepath)
    plt.close()
    print(f"Saved plot to {filepath}")

def plot_ytd_sales(df: DataFrame, title: str, filename: str, directory: str):
    """
    Create a bar plot of YTD sales.
    """
    plt.figure(figsize=(12, 6))
    x = range(len(df['year']))
    width = 0.35
    
    plt.bar(x, df['gross_revenue'], width, label='Gross Revenue')
    plt.bar([i + width for i in x], df['net_profit'], width, label='Net Profit')
    
    plt.xlabel('Year')
    plt.ylabel('Amount')
    plt.title(title)
    plt.xticks([i + width/2 for i in x], df['year'])
    plt.legend()
    
    filepath = os.path.join(directory, filename)
    plt.savefig(filepath)
    plt.close()
    print(f"Saved plot to {filepath}")