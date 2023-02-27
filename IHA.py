import pandas as pd

class IHA:
    def __init__(self, df, date_col = 'Date', flow_col = 'Flow'):

        #check if df is a pandas dataframe
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        
        #check if df has a column named date_col
        if date_col not in df.columns:
            raise TypeError(f'df must have a column named "{date_col}"')
        
        #check if Date is a datetime object
        if df[date_col].dtype.kind != 'M':    #Datetime
            raise TypeError(f'{date_col} must be a datetime object')
        
        #check if df has a column named flow_col
        if flow_col not in df.columns:
            raise TypeError(f'df must have a column named "{flow_col}"')

        #check if flow_col is a numeric object
        if df[flow_col].dtype.kind not in 'iuf':    #integer or unsigned integer or float
            raise TypeError(f'{flow_col} must be a numeric object')
        
        self.df = df
        self.date_col = date_col
        self.flow_col = flow_col

    
    # add functions here