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
    def _annual_k_days_min_max(self, k, min_max):

        #for each year, calculate the rolling mean of k days and group by years
        mean_grouped_by_year = self.df.groupby(self.df.index.year).rolling(k).mean().groupby(self.df.index.year).Flow

        if min_max == 'min':
            return mean_grouped_by_year.min()
        elif min_max == 'max':
            return mean_grouped_by_year.max()
        else:
            raise TypeError(f'{min_max} must be either "min" or "max"')
        
    def annual_1_day_minima(self):
        return self._annual_k_days_min_max(1, 'min')
    
    def annual_1_day_maxima(self):
        return self._annual_k_days_min_max(1, 'max')
    
    def annual_7_day_minima(self):
        return self._annual_k_days_min_max(7, 'min')
    
    def annual_7_day_maxima(self):
        return self._annual_k_days_min_max(7, 'max')
    
    def annual_90_day_minima(self):
        return self._annual_k_days_min_max(90, 'min')
    
    def annual_90_day_maxima(self):
        return self._annual_k_days_min_max(90, 'max')
    
    """
    Misc
    """
    #number of zero flow days per year
    def zero_flow_days(self):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] == 0].count()).Flow

    #percentile k per year
    def _annual_percentile_k(self, k):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[self.flow_col].quantile(k/100))
    
    #percentile 10 per year
    def annual_percentile_10(self):
        return self._annual_percentile_k(10)

    def annual_percentile_30(self):
        return self._annual_percentile_k(30)

    def annual_percentile_90(self):
        return self._annual_percentile_k(90) 

    def baseflow_index(self): 

        #annual 1 day avg
        annual_1_day_avg = self.df.groupby(self.df.index.year).mean().Flow

        #annual 7 day minima divided by annual 1 day avg
        return self.annual_7_day_minima() / annual_1_day_avg    
        
    """
    Group 3
    """
    def _annual_1_day_min_max_julian_day(self, min_max):
        
        if min_max == 'min':
            return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] == x[self.flow_col].min()].index.dayofyear[0])
        elif min_max == 'max':
            return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] == x[self.flow_col].max()].index.dayofyear[0])
        else:
            raise TypeError(f'{min_max} must be either "min" or "max"')
        

    #1 day min julian day
    def annual_1_day_min_julian_day(self):
        return self._annual_1_day_min_max_julian_day('min')

    #1 day max julian day
    def annual_1_day_max_julian_day(self):
        return self._annual_1_day_min_max_julian_day('max')
    
    """
    Group 4
    """
    def high_pulses_per_year(self):
        #for each year, calculate number of high pulses
        #high_pulses = sum_{i}(ind(Qi<=Q10<Qi+1))
        counts = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            count = 0
            quantile10 = flow.quantile(0.10)
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] <= quantile10["Flow"] and flow["Flow"][i+1] > quantile10["Flow"]:
                        count += 1
            counts.append(count)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "High_pulses":counts}).set_index(["Date"]).High_pulses
    
    def low_pulses_per_year(self):
        #for each year, calculate number of low pulses
        #low_pulses = sum(ind(Qi>=Q90>Qi+1))
        counts = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            count = 0
            quantile90 = flow.quantile(0.90)
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] >= quantile90["Flow"] and flow["Flow"][i+1] < quantile90["Flow"]:
                        count += 1
            counts.append(count)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "Low_pulses":counts}).set_index(["Date"]).Low_pulses
    
        
    def mean_duration_high_pulses(self):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] > x[self.flow_col].quantile(0.10)].count()).Flow / self.high_pulses_per_year()
    

    def mean_duration_low_pulses(self):
         return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] < x[self.flow_col].quantile(0.90)].count()).Flow / self.low_pulses_per_year()


    """
    Group 5
    """

    def mean_of_pos_inc_cons_flows(self):
        sums = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            sum = 0
            count = 0
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] < flow["Flow"][i+1]:
                        count += 1
                        sum += flow["Flow"][i+1] - flow["Flow"][i]
            sums.append(sum/count)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "Flow":sums}).set_index(["Date"]).Flow
    
    def mean_of_neg_inc_cons_flows(self):
        sums = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            sum = 0
            count = 0
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] > flow["Flow"][i+1]:
                        count += 1
                        sum += flow["Flow"][i+1] - flow["Flow"][i]
            sums.append(sum/count)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "Flow":sums}).set_index(["Date"]).Flow
    
    def num_flow_reversals(self):
        counts = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            count = 0
            for i in range(1, len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i-1]) == False and np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if (flow["Flow"][i+1]-flow["Flow"][i])*(flow["Flow"][i]-flow["Flow"][i-1]) < 0:
                        count += 1
            counts.append(count)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "Flow":counts}).set_index(["Date"]).Flow

