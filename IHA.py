import pandas as pd
import numpy as np

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
        
        #drop all columns except date_col and flow_col
        self.df = df[[date_col, flow_col]].copy()

        #make data_col the index
        self.df = self.df.set_index(date_col)
        
        self.flow_col = flow_col

    """
    Group 2
    """
    def annual_k_days_min_max(self, k, min_max):

        #for each year, calculate the rolling mean of k days and group by years
        mean_grouped_by_year = self.df.groupby(self.df.index.year).rolling(k).mean().groupby(self.df.index.year)

        if min_max == 'min':
            return mean_grouped_by_year.min()
        elif min_max == 'max':
            return mean_grouped_by_year.max()
        else:
            raise TypeError(f'{min_max} must be either "min" or "max"')
        
    def annual_1_day_minima(self):
        return self.annual_k_days_min_max(1, 'min')
    
    def annual_1_day_maxima(self):
        return self.annual_k_days_min_max(1, 'max')
    
    def annual_7_day_minima(self):
        return self.annual_k_days_min_max(7, 'min')
    
    def annual_7_day_maxima(self):
        return self.annual_k_days_min_max(7, 'max')
    
    def annual_90_day_minima(self):
        return self.annual_k_days_min_max(90, 'min')
    
    def annual_90_day_maxima(self):
        return self.annual_k_days_min_max(90, 'max')
    
    """
    Misc
    """
    #number of zero flow days per year
    def zero_flow_days(self):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] == 0].count()) 

    #percentile k per year
    def percentile_k(self, k):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[self.flow_col].quantile(k/100))
    
    #percentile 10 per year
    def percentile_10(self):
        return self.percentile_k(10)

    def percentile_30(self):
        return self.percentile_k(30)

    def percentile_90(self):
        return self.percentile_k(90) 

    def baseflow_index(self): 

        #annual 1 day avg
        annual_1_day_avg = self.df.groupby(self.df.index.year).mean()

        #annual 7 day minima divided by annual 1 day avg
        return self.annual_7_day_minima() / annual_1_day_avg    
        
    """
    Group 3
    """
    #1 day min julian day
    def annual_1_day_min_julian_day(self):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] == x[self.flow_col].min()].index.dayofyear[0])


    #1 day max julian day
    def annual_1_day_max_julian_day(self):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] == x[self.flow_col].max()].index.dayofyear[0])
    
    """
    Group 4
    """
    def low_pulses_per_year(self):
        #for each year, calculate number of low pulses
        pass


