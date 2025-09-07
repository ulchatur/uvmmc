import pandas as pd
import numpy as np


def read_infile_cls(source_df, product, inference):
    # df = source_df[source_df['Recurring'] == recurring]
    # df.reset_index(drop=True, inplace=True)
    df = source_df

    # establish conditons for when a client has revenue greater than 250k and greater than 10k (will be used to define client size)
    if inference == False:
        rev_train = list(df.filter(like='csr_revenue_year_2').columns)
        rev_test = list(df.filter(like='csr_revenue_year_1').columns)
    else:
        rev_train = list(df.filter(like='csr_revenue_year_1').columns)
        rev_test = list(df.filter(like='csr_revenue_year_1').columns)

    train_condition_df = df.groupby('client_number')[rev_train].sum().reset_index()
    test_condition_df = df.groupby('client_number')[rev_test].sum().reset_index()
    
    large_client_train_condition = train_condition_df[rev_train].sum(axis=1) > 250000
    large_client_test_condition = test_condition_df[rev_test].sum(axis=1) > 250000
    
    mid_client_train_condition = train_condition_df[rev_train].sum(axis=1) > 10000
    mid_client_test_condition = test_condition_df[rev_test].sum(axis=1) > 10000
    
    df['large_client_train_condition'] = large_client_train_condition
    df['large_client_test_condition'] = large_client_test_condition
    
    df['mid_client_train_condition'] = mid_client_train_condition
    df['mid_client_test_condition'] = mid_client_test_condition

    df.reset_index(drop=True, inplace=True)
    print(df.shape)

    df_keys = df.loc[:, ['client_number', 'Recurring']]

    #Remove columns with 0 variance and their correspoinding year/qtr columns
    x = df.dtypes
    x = x[x != 'object'].index
    
    temp = df[x]
    y = temp.apply(lambda x: len(np.unique(x)) == 1)
    
    y = y[y == True].index
    print(len(y))
    
    y = [y for y in y if "year_0" not in y]
    print(len(y))
    
    y = ["_".join(y.split("_")[:-2]) for y in y]


    df.reset_index(drop=True, inplace=True)
    print(df.shape)
    return(df, df_keys)
        