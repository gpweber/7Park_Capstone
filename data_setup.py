import pandas as pd
import numpy as np


def ZRI_format(ZRI, time_unit = 'Month', window_size = 1, future_time = 1):
    '''
    Takes in a ZRI dataframe (in the wide format directly from bigquery) and specifications for the final shape of 
    the data to use in a machine learning model. 
    time_unit specifies the unit of time to use in the model, default is Month, must be Month, Quarter or Year
    window_size specifies the number of time units in the past to use. Default is 1
    future_time specifies the number of time unites in the future to predict. In this formulation the current ZRI will be the target. 
    '''
    #Columns which specify a time in the ZRI wide format
    time_columns = [x for x in ZRI.columns if ('20' in x)]

    #Any column that isn't a year in the ZRI wide format
    other_columns = list(filter(lambda x: x not in year_columns, ZRI.columns))

    #Turns the RegionName from an in integer to a 5-digit string
    ZRI = ZRI.assign(RegionName =  ZRI['RegionName'].astype(str).apply(FixID))

    #Makes a long format of the ZRI, with every month for each region a separate row
    ZRI_long = ZRI.melt(id_vars = other_columns,
                              value_vars = time_columns)\
                             .rename({'value':'ZRI','variable':'Date'},axis = 1)

    #Adds extra time columns to help with grouping later on
    ZRI_long['Month'] = ZRI_long.loc[:,'Date'].apply(lambda x: int(x[-2:]))
    ZRI_long['Year'] = ZRI_long.loc[:,'Date'].apply(lambda x: int(x[1:5]))
    ZRI_long['Quarter'] = ZRI_long.loc[:,'Month'].apply(lambda x: 1+(x-1)//3)

    #Adds the index of the final observations: 'tyyyyrrrrr', the first digit or two are time_unit, then year then regionID
    if time_unit == 'Year':
        ZRI_long = ZRI_long.assign(new_index =  ZRI_long.Year.astype(str) 
                                      + ZRI_long.RegionName.astype(str))
    else:
        ZRI_long = ZRI_long.assign(new_index = ZRI_long[time_unit].astype(str) 
                   + ZRI_long.Year.astype(str)
                   + ZRI_long.RegionName.astype(str))

    #groups by the new index and aggregates
    ZRI_long = ZRI_long.groupby('new_index').mean().reset_index()

    #creates new columns equal to the previous time_units ZRI, also stepped back by future_time
    for i in range(0,window_size):
        column_name = f'ZRI - {i}'
        next_indices = ZRI_long.new_index.apply(lambda x: past_index(x, time_unit = time_unit, units_back = future_time+i))
        ZRI_long[column_name] = [try_index(ZRI_long,j) for j in next_indices]
        #NOTE this is incredibly slow at the moment. Think about better ways to handle the above line
    return(ZRI_long)

def try_index(df,index):
    '''
    Tries to access data in a dataframe. If it isn't there retun a nan value 
    (this happens when there isn't a time index, i.e. it goes too far back)
    '''
    past_ZRI = df.ZRI[df.new_index == index]
    if not past_ZRI.empty:
        return(past_ZRI.iloc[0])
    return(np.nan)


def past_index(target_index, time_unit, units_back):
    #Returns new_index of previous 
    #Need an empty logic statement

    #NEED TO DO A BETTER JOB WITH THE INDEXING, IT MIGHT LOOP THROUGH MULTIPLE TIMES ETC>>>>>
    months = list(map(str,range(1,13)))
    quarters = list(map(str, range(1,5)))
    region = target_index[-5:]
    year = target_index[-9:-5]

    if time_unit == 'Year':
        return(str(int(year)-units_back)+region)
    elif time_unit == 'Month':
        prev_time = int(target_index[0:-9])-units_back
        if prev_time<=0:
            year = str(int(year)-1)
        return(months[prev_time-1]+year+region)
    else:
        if time_unit != 'Quarter':
            raise ValueError('time_unit must be Month, Quarter, or Year')

        prev_time = int(target_index[0:-9])-units_back

        if prev_time<=0:
            year = str(int(year)-1)

        return(quarters[prev_time-1]+year+region)
        



    
def FixID(geoid):
    '''
    Returns a 5 digit string of an integer representation of an FIPS geoID
    '''

    geoid = str(int(geoid))
    geoid = '0'*(5-len(geoid)) + geoid
    return(geoid)


