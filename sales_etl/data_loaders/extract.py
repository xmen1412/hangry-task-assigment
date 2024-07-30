import pandas as pd
from mage_ai.io.file import FileIO

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    """
    Load data from local CSV files
    """
    # Load Order data
    order_df = pd.read_csv('data/order.csv')
    
    # Load Menu data
    menu_df = pd.read_csv('data/menu.csv')
    
    # Load Promotion data
    promotion_df = pd.read_csv('data/promotion.csv')
    
    return {
        'order': order_df,
        'menu': menu_df,
        'promotion': promotion_df
    }

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert 'order' in output, 'Order data is missing'
    assert 'menu' in output, 'Menu data is missing'
    assert 'promotion' in output, 'Promotion data is missing'