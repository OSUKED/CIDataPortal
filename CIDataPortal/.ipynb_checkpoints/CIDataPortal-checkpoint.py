"""
Imports
"""

## Data Manipulation
import pandas as pd
import numpy as np

## Plotting
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

## Datetime
import datetime
import dateutil.parser

## Misc
from ipypb import track
import requests
import collections


"""
Date Handling
"""
def form_ISO8601_dt(dt): 
    """
    This function takes datetimes in a variety of formats and converts them to a string in the ISO-8601 format.
    If only a date is provided it will set the time to 00:00.
    """
    
    class_type_to_formatter = {
        str : pd.to_datetime,
        pd._libs.tslibs.timestamps.Timestamp : lambda pd_ts: pd_ts,
        datetime.date : lambda datetime_date: datetime.datetime.combine(datetime_date, datetime.datetime.min.time()),
        datetime.datetime : lambda datetime_datetime: datetime_datetime,
    }
    
    assert isinstance(dt, tuple(class_type_to_formatter.keys())), f'The date variable provided could not be understood, please use one of: {", ".join([str(x) for x in list(class_type_to_formatter.keys())])}.'
    
    dt_fmt = class_type_to_formatter[type(dt)](dt)
    dt_ISO = dt_fmt.isoformat() + 'Z'
    
    return dt_ISO

str_dt_to_pd_ts = lambda str_dt: pd.to_datetime(str_dt[:-1]).tz_localize(None)

def process_n_check_dt_rng(from_date=None, to_date=None, SP_start=True):
    if not from_date:
        from_date = datetime.date.today()
    from_date = form_ISO8601_dt(from_date)
    
    if not to_date:
        to_date = pd.to_datetime(from_date[:-1]) + pd.Timedelta(hours=23, minutes=30)
    to_date = form_ISO8601_dt(to_date)
    
    assert_err_msg = 'The dates provided should be within two weeks of each other'
    assert str_dt_to_pd_ts(to_date[:-1])-str_dt_to_pd_ts(from_date[:-1]) < pd.Timedelta(weeks=2), assert_err_msg
    
    if SP_start == True:
        from_date = form_ISO8601_dt( str_dt_to_pd_ts(from_date[:-1]) + pd.Timedelta(minutes=30) )
        to_date = form_ISO8601_dt( str_dt_to_pd_ts(to_date[:-1]) + pd.Timedelta(minutes=30) )
    
    return from_date, to_date

def construct_date_range_pairs(from_date, to_date):
    dt_rng_open = pd.date_range(from_date, to_date, freq='14D')
    dt_rng_close = dt_rng_open + pd.Timedelta(days=14, minutes=-30)

    for dt_rng in [dt_rng_open, dt_rng_close]:
        dt_rng.freq = None

    dt_rng_pairs = list(zip(dt_rng_open, dt_rng_close))
    
    return dt_rng_pairs


"""
API ETL Functions
"""
def form_stream_url(stream):
    branch_streams = ['intensity', 'regional', 'generation', 'regional/intensity', 'intensity/factors']
    url_root = 'https://api.carbonintensity.org.uk'

    assert stream in branch_streams, f'{branch} is not a recognised API branch, please use one of: {", ".join(branch_streams)}.'

    stream_url = f'{url_root}/{stream}'

    return stream_url

def stream_params_to_json(stream, from_date=None, to_date=None, SP_start=True):
    from_date, to_date = process_n_check_dt_rng(from_date, to_date, SP_start)
    
    stream_url = form_stream_url(stream)
    url = f'{stream_url}/{from_date}/{to_date}'

    r = requests.get(url)
    r_json = r.json()
    
    return r_json

def expand_cols(df, cols_to_expand=[]):
    for col in cols_to_expand:
        new_df_cols = df[col].apply(pd.Series)

        df[new_df_cols.columns] = new_df_cols
        df = df.drop(columns=col)

    s_cols_to_expand = df.iloc[0].apply(type).isin([collections.OrderedDict, dict, list, tuple])

    if s_cols_to_expand.sum() > 0:
        cols_to_expand = s_cols_to_expand[s_cols_to_expand].index
        df = expand_cols(df, cols_to_expand)

    return df

def emiss_r_json_to_df(r_json):
    df = pd.DataFrame(r_json['data'])
    df = expand_cols(df)

    df.index = pd.to_datetime(df['from'])
    df.index.name = None

    cols_to_keep = list(set(df.columns) - set(['to', 'index', 'from']))
    df = df[cols_to_keep]

    return df

def expand_json_fuel(json_data, data_col='data'):
    fuels_SP = [iterable['generationmix'] 
                for iterable 
                in json_data[data_col]]
    
    fuels_pct_SP = [(pd.DataFrame(fuels)
                     .set_index('fuel')
                     .iloc[:, 0]
                     .to_dict() 
                    )
                    for fuels 
                    in fuels_SP]
    
    return fuels_pct_SP

def gen_r_json_to_df(r_json):
    fuels_pct_SP = expand_json_fuel(r_json)

    df_fuels_pct_SP = pd.DataFrame(fuels_pct_SP)
    df_fuels_pct_SP.index = pd.to_datetime([SP_data['from'] for SP_data in r_json['data']])
    
    return df_fuels_pct_SP

def reg_SP_to_reg_intensity(regions_SP, regions=list(range(1, 19))): 
    intensity_dicts = [region_SP['intensity'] for region_SP in regions_SP['regions']]
    df_intensity = pd.DataFrame(intensity_dicts, index=regions)
    s_intensity = df_intensity['forecast']
    
    return s_intensity

def reg_SP_to_s_reg_fuel_pct(regions_SP, regions=list(range(1, 19))):
    regions_fuels_pct_SP = expand_json_fuel(regions_SP, data_col='regions')
    df_regions_fuels_pct_SP = pd.DataFrame(regions_fuels_pct_SP, index=regions)

    s_reg_fuel_pct = (df_regions_fuels_pct_SP
                      .unstack()
                      .swaplevel(1, 0)
                      .sort_index()
                     )

    return s_reg_fuel_pct

def initialise_regional_fuel_df():
    fuels = sorted(['biomass', 'coal', 'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind'])
    regions = list(range(1, 19))

    multi_idx_arrs = [sorted(regions*len(fuels)), fuels*len(regions)]
    multi_idx = pd.MultiIndex.from_arrays(multi_idx_arrs, names=['region', 'fuel'])

    df_region_fuel_pct = pd.DataFrame(columns=multi_idx)
    return df_region_fuel_pct

def regional_r_json_to_fuel_emiss_df(r_json):
    ## Creating the regional fuel and intensity dataframes
    df_region_fuel_pct = initialise_regional_fuel_df()
    df_region_intensity = pd.DataFrame(columns=list(range(1, 19)))
    
    ## Populating the dataframes
    for SP_data in r_json['data']:
        start_time = pd.to_datetime(SP_data['from'])

        df_region_fuel_pct.loc[start_time] = reg_SP_to_s_reg_fuel_pct(SP_data)
        df_region_intensity.loc[start_time] = reg_SP_to_reg_intensity(SP_data)
        
    return df_region_fuel_pct, df_region_intensity
    
def regional_r_json_to_fuel_df(r_json):
    ## Creating the dataframe
    df_region_fuel_pct = initialise_regional_fuel_df()
    
    ## Populating the dataframe
    for SP_data in r_json['data']:
        start_time = pd.to_datetime(SP_data['from'])
        df_region_fuel_pct.loc[start_time] = reg_SP_to_s_reg_fuel_pct(SP_data)
        
    return df_region_fuel_pct
    
def regional_r_json_to_emiss_df(r_json):
    ## Creating the dataframe
    df_region_intensity = pd.DataFrame(columns=list(range(1, 19)))
    
    ## Populating the dataframe
    for SP_data in r_json['data']:
        start_time = pd.to_datetime(SP_data['from'])
        df_region_intensity.loc[start_time] = reg_SP_to_reg_intensity(SP_data)
        
    return df_region_intensity


"""
API Wrapper Class
"""
class Wrapper:
    def __init__(self):
        self.available_data_streams = ['emissions', 'generation']
        self.available_levels = ['national', 'regional']
        
        self.query_funcs = {
            'national' : {
                'emissions' : lambda from_date, to_date: emiss_r_json_to_df(stream_params_to_json('intensity', from_date, to_date)),
                'generation' : lambda from_date, to_date: gen_r_json_to_df(stream_params_to_json('generation', from_date, to_date)),
            },
            'regional' : {
                'emissions' : lambda from_date, to_date: regional_r_json_to_emiss_df(stream_params_to_json('regional/intensity', from_date, to_date)),
                'generation' : lambda from_date, to_date: regional_r_json_to_fuel_df(stream_params_to_json('regional/intensity', from_date, to_date)),
            },
        }
    
    
    def query_API(self, from_date=None, to_date=None, data_stream='emissions', level='national'):
        """ 
        Queries the National Grid ESO Carbon Intensity API, enabling data to be easily
        retrieved for emissions and fuel generation at both a regional and national level
        
        N.b. the regional emissions data only includes forecast levels as the API
        does not have an observed regional emissions data stream at time of writing

        Parameters
        ----------
        from_date : str
            Start date for the queried data
        to_date : str
            End date for the queried data
        data_stream : str
            One of 'emissions' or 'generation'
        level : str
            spatial aggregation level, one of 
            'national' or 'regional' (DNOs)

        Returns
        -------


        """
        
        # Input checks
        assert data_stream in self.available_data_streams, f"Data stream must be one of: {', '.join(self.available_data_streams)}"
        assert level in self.available_levels, f"Level must be one of: {', '.join(self.available_levels)}"
        
        query_func = self.query_funcs[level][data_stream]
        
        if from_date is None or to_date is None:
            df = query_func(from_date, to_date)

        elif (pd.to_datetime(to_date) - pd.to_datetime(from_date)) > pd.Timedelta(weeks=2):
            df = pd.DataFrame()
            dt_rng_pairs = construct_date_range_pairs(from_date, to_date)

            for pair_from_date, pair_to_date in track(dt_rng_pairs):
                df_pair = query_func(pair_from_date, pair_to_date)
                df = df.append(df_pair)

        else:
            df = query_func(from_date, to_date)
        
        return df
    
    def __repr__(self):
        str_repr = """
        Thank you for downloading the CIDataPortal package, now you've 
        initialised the wrapper you can start querying the API.
        
        Example:
        df = wrapper.query_API()
        
        """
        
        return str_repr