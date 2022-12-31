import re
import numpy as np
from itertools import chain


import pandas as pd
from dask.distributed import Client
import dask
import dask.dataframe as dd

import geohash as gh

def get_airbnb_locations(path="./data/raw/Mesh_Ids_Data_Itapema.csv"):
    
    # Read .csv
    airbnb_locations_raw = \
        pd.read_csv(path) # add dtype
    
    # clean and format data
    airbnb_locations = clean_airbnb_locations(airbnb_locations_raw)
    
    return airbnb_locations

def clean_airbnb_locations(df):
    
    # Drop duplicates, keeping the last one aquired by the scraper
    df.sort_values('aquisition_date', ascending=False, inplace=True)
    df.drop_duplicates(subset='airbnb_listing_id', inplace=True)
    
    return df

def replace_lat_lon(df1, df2):
    """ Replaces latitude and longitude of df1 with the ones in df2
    
        Returns df1
    """
    
    # Drop the original latitude and longitude columns
    df1.drop(labels = ['latitude', 'longitude'], axis=1, inplace = True)
    
    # Merge with df2 latitude and longitude
    df1 = df1.merge(df2[['airbnb_listing_id','latitude','longitude']], left_on = 'ad_id', right_on = 'airbnb_listing_id')
    df1.drop('airbnb_listing_id', axis=1, inplace = True)
    
    return df1

def include_geohash(df, p):
    
    # Encodes lat/lon with a geohash of precision p
    df[f'geohash{p}'] = df.apply(lambda x: gh.encode(x.latitude, x.longitude, precision=p), axis=1)

    return df

def make_comma_separable(x):
    """ Convert unformatted string to a comma separable format """
    
    #print(x)
    #print(type(x))
    x = re.sub(r"[\[\]]", "", x) # remove '[' and ']'
    x = re.sub(r"[\{\}]", "", x) # remove '{' and '}'
    
    x = re.sub(r"", "", x) # remove '{' and '}'
    
    x = x.replace('"', '') # remove '"'
    
    
    
    if '\\' in x:
        x = x.replace(',,,', ',;,')
        x = x.replace(',', '')
        x = x.replace(';', ',')
        x = x.replace('\\\\', '|')
        x = x.replace('\\', '')
        x = x.replace('|', '\\')
        x = x.encode('utf-8').decode('unicode-escape')
        
    x = x.replace(', ', ',')    
    
    return x

def add_full_text(x):
    # convert single expressions to the complete category string
    x = x.replace(",lockbox", ",Self check-in com lockbox")
    
    return x

def create_one_hot_encoding(df, columns_to_encode):
    # Creates one hot encoding for a list of columns
    # return a df with the encoded columns and a list of the encoded columns names
    encoded_columns = []
    
    for col in columns_to_encode:
        df[col] = df[col].fillna("")
        df[col] = df[col].str.replace("{n,u,l,l}","")
        df[col] = df[col].apply(make_comma_separable)
        df[col] = df[col].apply(add_full_text)
        
        one_hot = df[col].str.get_dummies(sep=',').astype(np.bool_) # creates one hot encoding
        encoded_columns.append(one_hot.columns.values.tolist())
        
        df = df.drop(col, axis = 1) # original column can now be dropped
        
        df = df.combine_first(one_hot)
        # df = df.merge(one_hot, left_index=True, right_index=True)
        
    encoded_columns = list(chain.from_iterable(encoded_columns))
    encoded_columns = list(dict.fromkeys(encoded_columns)) # removes duplicate column names 
    return df, encoded_columns

def get_airbnb_listings(path="./data/raw/Details_Data.csv"):
    
    dtype = {'aquisition_date': str,
            'url': str,
            'ad_name': str,
            'ad_description': str,
            'ad_id': np.int_,
            'space': str,
            'house_rules': str,
            'amenities': str,
            'safety_features': str,
            'number_of_bathrooms': pd.UInt8Dtype(),
            'number_of_bedrooms': pd.UInt8Dtype(),
            'number_of_beds': pd.UInt8Dtype(),
            'latitude': np.float64,
            'longitude': np.float64,
            'star_rating': np.float16,
            'additional_house_rules': str,
            'owner': str,
            'check_in': str,
            'check_out': str,
            'number_of_guests': pd.UInt8Dtype(),
            'is_superhost': np.bool_,
            'number_of_reviews': pd.UInt16Dtype(),
            'cohosts': str,
            'cleaning_fee': np.float16,
            'can_instant_book': np.bool_,
            'owner_id': np.int_,
            'listing_type': str,
            'index': pd.UInt64Dtype(),
            'localized_star_rating': str,  # decimal separator is comma ',', need to change to dot '.'
            'response_time_shown': str,
            'response_rate_shown': str,
            'guest_satisfaction_overall': np.float16,
            'picture_count': pd.UInt8Dtype(),
            'min_nights': pd.UInt64Dtype(),
            'ano': pd.UInt16Dtype(),
            'mes': pd.UInt8Dtype(),
            'dia': pd.UInt8Dtype()}
    
    # Read .csv
    airbnb_listings_raw = pd.read_csv(path, 
                                        na_values='',
                                        dtype=dtype)

    # Clean and format data
    airbnb_listings = clean_airbnb_listings(airbnb_listings_raw)
    
    # Create one-hot-encodings
    columns_to_encode = ['safety_features', 'house_rules', 'amenities']
    airbnb_listings, encoded_columns = create_one_hot_encoding(airbnb_listings, columns_to_encode)
    
    return airbnb_listings, encoded_columns

def clean_airbnb_listings(df):
    
    # replace ',' to '.' on localized_star_rating, then convert to float
    df['localized_star_rating'] = \
        pd.to_numeric(
            df['localized_star_rating'].str.replace(',', '.'), downcast='float'
            )
    
    # format aquisition date as datetime
    df['aquisition_date'] = \
        pd.to_datetime(df['aquisition_date'], format="%Y-%m-%d %H:%M:%S")
    
    # Drop duplicates, keeping the last one aquired by the scraper
    # DISCLAIMER: doing so removes some information on listings, for example, 'min_nights' 
    # for all of the latest aquired listings is <NA>, even though it wasn't in the beginning
    df.sort_values('aquisition_date', ascending=False, inplace=True)
    df.drop_duplicates(subset='ad_id', inplace=True)
    
    return df


### AIRBNB PRICE AND AVAILABILITY
def get_airbnb_price_av(path="./data/raw/Price_AV_Itapema-001.csv"):
    
    # After some inspection the following features were confirmed being all NA: 
    # av_for_checkout, index and bookable, therefore they will not be used
    data_columns = ["airbnb_listing_id", "date", "price",
                    "price_string", "minimum_stay", "available",
                    "aquisition_date", "av_for_checkin", #"av_for_checkout",
                    #"index", "bookable", 
                    "ano", "mes", "dia"] 

    dtypes=    {"airbnb_listing_id": np.uint64, 
                 "date": str,
                 "price": np.float_, 
                 "price_string": str,
                 "minimum_stay": np.uint16,
                 "available": np.bool_,
                 "aquisition_date": str,
                 "av_for_checkin": str, #np.bool_,
                 #"av_for_checkout": bool,
                 #"index": str,
                 #"bookable": str,
                 "ano": np.uint16,
                 "mes": np.uint8,
                 "dia": np.uint8 }
    
    date_columns = ['date', 'aquisition_date']
    
    airbnb_price_raw = dd.read_csv(path,
                                    usecols=data_columns,
                                    dtype=dtypes, 
                                    parse_dates=date_columns)
    
    airbnb_price_av = clean_airbnb_price_av(airbnb_price_raw)
    
    return airbnb_price_av

def clean_airbnb_price_av(df):
    
    # Column 'price' has a small amount of NaN, we can safely drop it
    df = df.dropna(subset='price')
    
    # Column 'av_for_checkin' has small amount of NaN, we can safely drop it
    df = df.dropna(subset='av_for_checkin')
    
    # There are a lot of typos in the 'price_string', creating unrealistic prices.
    # Drop any 'price' greater than R$100.000 to parcially solve it.
    df = df[df['price'] < 100000]
    
    return df

def calculate_availability_rate(df):
    
    # Calculate how many days were Available or Not available for each listing
    series_av = (df
                .groupby('airbnb_listing_id')['available']
                .value_counts()
                .compute())
    
    # Transform previous value_counts series into a Dataframe
    df_av = (series_av
                .unstack(-1)                    
                .fillna(0)                       
                .add_prefix('available_')
                .reset_index())
    
    # Calculates availability rate
    df_av['availability_rate'] = df_av['available_True']/(df_av['available_True'] + df_av['available_False'])
    
    df_av_subset = df_av[['airbnb_listing_id', 'availability_rate']]
    
    return df_av_subset

def include_availability(df1, df2):
    
    # Includes df2 availability_rate colum into df1
    df1 = df1.merge(df2, left_on='ad_id', right_on='airbnb_listing_id')
    
    return df1

def dd_mode():
    """ Creates a Dask Dataframe Aggregation to calculate mode """

    def chunk(s):
        # for the comments, assume only a single grouping column, the 
        # implementation can handle multiple group columns.
        #
        # s is a grouped series. value_counts creates a multi-series like 
        # (group, value): count
        return s.value_counts()


    def agg(s):
    #     print('agg',s.apply(lambda s: s.groupby(level=-1).sum()))
        # s is a grouped multi-index series. In .apply the full sub-df will passed
        # multi-index and all. Group on the value level and sum the counts. The
        # result of the lambda function is a series. Therefore, the result of the 
        # apply is a multi-index series like (group, value): count
        return s.apply(lambda s: s.groupby(level=-1).sum())

        # faster version using pandas internals
        s = s._selected_obj
        return s.groupby(level=list(range(s.index.nlevels))).sum()


    def finalize(s):
        # s is a multi-index series of the form (group, value): count. First
        # manually group on the group part of the index. The lambda will receive a
        # sub-series with multi index. Next, drop the group part from the index.
        # Finally, determine the index with the maximum value, i.e., the mode.
        level = list(range(s.index.nlevels - 1))
        return (
            s.groupby(level=level)
            .apply(lambda s: s.reset_index(level=level, drop=True).idxmax())
        )

    mode = dd.Aggregation('mode', chunk, agg, finalize)
    
    return mode

def calculate_avg_price(df, avg_func):
    
    df_avg_price = df.groupby(['airbnb_listing_id']).agg({'price': avg_func}).compute()
    df_avg_price.rename(columns={'price':'mode_price'}, inplace=True)

    return df_avg_price

def include_avg_price(df1, df2):
    
    # Includes df2 availability_rate colum into df1
    df1 = df1.merge(df2, left_on='ad_id', right_on='airbnb_listing_id')
    
    return df1

def get_least_available_airbnb_listings(df):
    
    # Select only the least available listings
    least_available_df = df.loc[df['availability_rate'] <= 0.1]
    
    return least_available_df

def get_apartments(df):
    
    # It was observed that the most common listing type is the apartment
    df_apartments = df.loc[\
                           df['listing_type']=='Espaço inteiro: apartamento']

    df_apartments.reset_index(drop = True, inplace = True)
    
    return df_apartments

def get_top_booked_airbnb_listings(df, min_reviews):
    
    # Select only the top booked listings
    top_booked_df = df.loc[df['number_of_reviews'] >= min_reviews]
    
    return top_booked_df

# VIVAREAL LISTINGS
def get_vivareal_listings(path="./data/raw/VivaReal_Itapema.csv"):
    
    # Read .csv
    vivareal_listings_raw = pd.read_csv(path)
    
    vivareal_listings = get_price_sqm(vivareal_listings_raw)
    
    vivareal_listings = clean_vivareal_listings(vivareal_listings)
    
    return vivareal_listings

def get_price_sqm(df):
    
    df['price/total_sqm'] = df['sale_price']/df['total_area']
    df['price/usable_sqm'] = df['sale_price']/df['usable_area']
    
    return df

def clean_vivareal_listings(df):
    
    # Drop duplicates, keeping only one
    df.drop_duplicates(subset='listing_id', inplace=True)

    # Location columns need to be cleaned
    location_columns = ["address_city", "address_neighborhood", \
                        "address_street", "address_street_number"]
    
    df[location_columns] = (df[location_columns] \
                            .apply(lambda x: \
                                    x.astype(str).str.lower()))
         
    df[location_columns] = \
         (df[location_columns] \
              .apply(lambda x: \
                     x.str.normalize('NFKD').str \
                     .encode('ascii', errors='ignore').str.decode('utf-8')))  
    
    # Price/total_sqm of over R$100.000,00 will be dropped
    df = df[df['price/total_sqm'] < 100000]
            
    return df

def get_lots_selling(df):
    
    df_lots_selling = \
        df[ \
            (   (df['unit_type']=='RESIDENTIAL_ALLOTMENT_LAND') \
                | \
                (df['unit_type']=='ALLOTMENT_LAND')
            ) \
            & \
           (df['business_types']=="[\"SALE\"]")]
    
    return df_lots_selling

def get_apartments_selling(df):
    
    df_apartments_selling = \
        df[(df['unit_type']=='APARTMENT')  \
            & \
           (df['business_types']=="[\"SALE\"]")]
    
    return df_apartments_selling

def groupby_neighborhood(df):  
    
    df_neighborhood = (df.groupby('address_neighborhood')
        .agg({'listing_id':'count', 'price/total_sqm': 'mean', 'price/usable_sqm': 'mean'})
        .reset_index()
        .rename(columns={'listing_id':'Listings count', 'price/total_sqm':'Mean price/Total sqm', 'price/usable_sqm':'Mean price/Usable sqm', 'address_neighborhood':'neighborhood'})
        .sort_values('Mean price/Usable sqm', ascending=False)
        )
       
    return df_neighborhood

def groupby_geohash(df, precision):
    new_df = (df.groupby(f'geohash{precision}')
        .agg({'ad_id':'count', 'mode_price': 'mean', 'number_of_reviews': 'sum', 'availability_rate': 'mean'})
        .reset_index()
        .rename(columns={'ad_id':'Listings count', 'mode_price':'Mean price', 'number_of_reviews':'Sum of reviews', 'availability_rate':'Mean availability', f'geohash{precision}': 'geohash'})
        .sort_values('Sum of reviews', ascending=False)
        )
    
    return new_df