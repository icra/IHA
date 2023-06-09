import pandas as pd
import numpy as np
from pyrle import Rle
"""
from gtda.time_series import TakensEmbedding
from gtda.homology import VietorisRipsPersistence
TE = TakensEmbedding(time_delay=1, dimension=2)
VRP = VietorisRipsPersistence()
"""

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
            #try to convert to datetime
            try:
                df[date_col] = pd.to_datetime(df[date_col])
            except:
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
    Group 1
    """
    def year_month_median(self):
        auxdf = self.df
        auxdf['Year'] = auxdf.index.year
        auxdf['Month'] = auxdf.index.month

        pivot_table = pd.pivot_table(auxdf, values='Flow', index='Year', columns='Month', aggfunc='median')
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        pivot_table.columns = month_names

        pivot_table = pivot_table.reset_index()
        pivot_table.columns.name = None

        year_median = auxdf.groupby('Year')['Flow'].median()
        pivot_table['Year Median'] = pivot_table['Year'].map(year_median)

        percentile_10 = self.annual_percentile_10()
        percentile_90 = self.annual_percentile_90()
        pivot_table['Year percentile 10'] = pivot_table['Year'].map(percentile_10)
        pivot_table['Year percentile 90'] = pivot_table['Year'].map(percentile_90)



        return pivot_table

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
    
    #number of periods of consecutive 0 flow days (minimum 2 days)
    def freq_zero_flow_periods(self):
        counts = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            count = 0
            no_flow_period = False
            for i in range(len(flow["Flow"])-1):
                if not no_flow_period:
                    if flow["Flow"][i] == 0 and flow["Flow"][i+1] == 0:
                        count += 1
                        no_flow_period = True
                else: 
                    if flow["Flow"][i] != 0:
                        no_flow_period = False
            counts.append(count)
            years.append(year)

        return pd.DataFrame({"Date":years, "zero_flow_periods":counts}).set_index(["Date"]).zero_flow_periods
    
    #mean duration of zero flow events: 
    def mean_zero_flow_periods(self):
        counts = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            count = 0
            sum = 0
            no_flow_period = False
            for i in range(len(flow["Flow"])-1):
                if not no_flow_period:
                    if flow["Flow"][i] == 0 and flow["Flow"][i+1] == 0:
                        sum += 1
                        count += 1
                        no_flow_period = True
                else: 
                    if flow["Flow"][i] != 0:
                        no_flow_period = False
                    else: 
                        sum += 1

            counts.append(sum/count)
            years.append(year)

        return pd.DataFrame({"Date":years, "mean_duration_zero_flow_periods":counts}).set_index(["Date"]).mean_duration_zero_flow_periods
    
    #day of the year on which the first zero-flow event takes place
    def zero_flow_onset(self):
        counts = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            first_zero_row = flow.index.get_loc(flow.index[flow['Flow'] == 0].min())
            counts.append(first_zero_row+1)
            years.append(year)

        return pd.DataFrame({"Date":years, "mean_duration_zero_flow_periods":counts}).set_index(["Date"]).mean_duration_zero_flow_periods

    #middle day of the year of zero-flow events
    def zero_flow_central_point(self):
        days = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            zero_days = []
            for i in range(len(flow["Flow"])-1):
                if flow["Flow"][i] == 0:
                    zero_days.append(i)

            days.append(np.median(zero_days))
            years.append(year)
        return pd.DataFrame({"Date":years, "Flow":days}).set_index(["Date"]).Flow    
    
    #Percentage of dry reaches per year and month
    def perccntage_dry_river_network(self, df_in):
        # Calcular columna auxiliar
        df = df_in
        df['flow_is_zero'] = df['Flow'].apply(lambda x: 1 if x == 0 else 0)

        # Agrupar por año
        grouped_df = df.groupby('year')

        result_data = []
        
        # Iterar por cada grupo de año
        for year, group in grouped_df:
            # Porcentaje de unidades con flujo igual a cero por mes
            monthly_percentages = group.groupby('month')['flow_is_zero'].mean() * 100
            
            # Contar el número de canales con al menos un día de flujo igual a cero en el año
            year_percentage = group['flow_is_zero'].mean() * 100
            
            # Agregar los resultados a la lista
            result_data.append([year] + list(monthly_percentages) + [year_percentage])

        # Crear DataFrame de resultados
        month_names = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        result_df = pd.DataFrame(result_data, columns=['year'] + month_names + ['Year'])
        
        # Imprimir DataFrame de resultados
        return result_df
    
    #percentile k per year
    def _annual_percentile_k(self, k):
        return self.df.groupby(self.df.index.year).apply(lambda x: x[self.flow_col].quantile(k/100))
    
    #percentile 10 per year
    def annual_percentile_10(self):
        return self._annual_percentile_k(10)

    def annual_percentile_50(self):
        return self._annual_percentile_k(50)

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
        quantile90 = self.df["Flow"].quantile(0.90)
        for year, flow in self.df.groupby(self.df.index.year):
            count = 0
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] > quantile90 and flow["Flow"][i+1] <= quantile90:
                        count += 1
            
            year_exists = (self.df.index.year == year+1).any()
            if year_exists:
                filtered_df = self.df.loc[(self.df.index.day == 1) & (self.df.index.month == 1) & (self.df.index.year == year+1)]
                flow_value = filtered_df['Flow'].values[0]
                if np.isnan(flow["Flow"][len(flow["Flow"])-1]) == False and np.isnan(flow_value) == False:
                    if flow["Flow"][len(flow["Flow"])-1] > quantile90 and flow_value <= quantile90:
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
            quantile10 = self.df["Flow"].quantile(0.10)
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] < quantile10 and flow["Flow"][i+1] >= quantile10:
                        count += 1

            year_exists = (self.df.index.year == year+1).any()
            if year_exists:
                filtered_df = self.df.loc[(self.df.index.day == 1) & (self.df.index.month == 1) & (self.df.index.year == year+1)]
                flow_value = filtered_df['Flow'].values[0]
                if np.isnan(flow["Flow"][len(flow["Flow"])-1]) == False and np.isnan(flow_value) == False:
                    if flow["Flow"][i] < quantile10 and flow["Flow"][i+1] >= quantile10:
                            count += 1

            counts.append(count)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "Low_pulses":counts}).set_index(["Date"]).Low_pulses
    
        
    def mean_duration_high_pulses(self):
        quantile90 = self.df["Flow"].quantile(0.90)
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] > quantile90].count()).Flow / self.high_pulses_per_year()
    

    def mean_duration_low_pulses(self):
        quantile10 = self.df["Flow"].quantile(0.10)
        return self.df.groupby(self.df.index.year).apply(lambda x: x[x[self.flow_col] <quantile10].count()).Flow / self.low_pulses_per_year()


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
    
    def median_of_pos_inc_cons_flows(self):
        sums = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            sum = []
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] < flow["Flow"][i+1]:
                        a_sum = flow["Flow"][i+1] - flow["Flow"][i]
                        sum.append(a_sum)
            sums.append(np.median(sum))
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
    
    def median_of_neg_inc_cons_flows(self):
        sums = []
        years = []
        for year, flow in self.df.groupby(self.df.index.year):
            sum = []
            for i in range(len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    if flow["Flow"][i] > flow["Flow"][i+1]:
                        a_sum = flow["Flow"][i+1] - flow["Flow"][i]
                        sum.append(a_sum)
            sums.append(np.median(sum))
            years.append(year)
        return pd.DataFrame({"Date":years, "Flow":sums}).set_index(["Date"]).Flow
    
    def num_flow_reversals(self):
        counts = []
        years = []      
        for year, flow in self.df.groupby(self.df.index.year):
            x = []
            for i in range(0, len(flow["Flow"])-1):
                if np.isnan(flow["Flow"][i-1]) == False and np.isnan(flow["Flow"][i]) == False and np.isnan(flow["Flow"][i+1]) == False:
                    diff = flow["Flow"][i+1]-flow["Flow"][i]
                    x.append(diff)
            x = np.array(x)
            f = np.ones_like(x)
            f[x > 0] = 2
            f[x < 0] = 0
            f_runs = Rle(f)
            f_values = f_runs.values
            i = np.where(f_values == 1)[0]
            if len(i) > 0 and i[0] == 1:
                f_values[0] = f_values[1]
                i = i[1:]
            if len(i) > 0:
                f_values[i] = f_values[i - 1]    
            f2 = []
            f2 = np.array(f2)
            for i in range (0, len(f_runs)):
                aux = np.repeat(f_values[i], f_runs.runs[i])
                f2 = np.concatenate((f2, aux))            
            f_runs2 = Rle(f2)
            counts.append(len(f_runs2)-1)
            years.append(year)
        
        return pd.DataFrame({"Date":years, "Flow":counts}).set_index(["Date"]).Flow

    """
    def persistent_homology(self):
        #returns an array with 0 dimensional and 1 dimensional persistent diagrams for each year.
        PDs = {}
        for year, flow in self.df.groupby(self.df.index.year):
            PDs.update({str(year):[[],[]]})
            cloud = TE.fit_transform([np.array(flow.fillna(0)["Flow"]), [0,1]])
            PD = VRP.fit_transform(cloud)
            for point in PD[0]:
                if point[2] == 0:
                    PDs[str(year)][0].append(np.array([point[0], point[1]]))
                else:
                    PDs[str(year)][1].append(np.array([point[0], point[1]]))
            PDs[str(year)][0] = np.array(PDs[str(year)][0])
            PDs[str(year)][1] = np.array(PDs[str(year)][1])

        return PDs
    """

