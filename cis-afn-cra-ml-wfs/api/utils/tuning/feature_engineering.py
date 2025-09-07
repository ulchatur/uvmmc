import pandas as pd
import numpy as np

def feature_engg(trn_df, tst_df, df_cat=None):
    drop_cols = []
    upd_cols_trn = {}
    upd_cols_tst = {}
    
    cat_cols = []
    
    for col in trn_df.columns:
        skew_pct = sum(trn_df[col] != 0)/len(trn_df[col])
        #print(col, ":", np.round(skew_pct*100, 4))
    
        if (skew_pct < 0.05) or (len(np.unique(trn_df[col])) <= 5) or (len(np.unique(tst_df[col])) <= 5):
            drop_cols.append(col)
            
        elif (skew_pct >= 0.05) & (skew_pct < 0.4):
    
            cat_cols.append(col + "_cat")
    
            if (sum(trn_df[col] < 0) > 0 & sum(tst_df[col] < 0) > 0):
                #Trn DF
                conditions_trn = [trn_df[col] < 0, trn_df[col] == 0, trn_df[col] > 0]
                choices_trn = [-1, 0, 1]
    
                # Tst DF
                conditions_tst = [tst_df[col] < 0, tst_df[col] == 0, tst_df[col] > 0]
                choices_tst = [-1, 0, 1]
            else:
                #Trn DF
                conditions_trn = [trn_df[col] <= 0, trn_df[col] > 0]
                choices_trn = [0, 1]
    
                # Tst DF
                conditions_tst = [tst_df[col] <= 0, tst_df[col] > 0]
                choices_tst = [0, 1]
                       
            #upd_cat_ord[col + "_cat"] = CategoricalDtype(choices)        
            try:
                upd_cols_trn[col + "_cat"] = np.select(conditions_trn, choices_trn)
            except:
                print("Error creating categorical variables")
    
            #upd_cat_ord[col + "_cat"] = CategoricalDtype(choices)        
            try:
                upd_cols_tst[col + "_cat"] = np.select(conditions_tst, choices_tst)
            except:
                print("Error creating categorical variables")
    
        elif ((skew_pct > 0.4) & (skew_pct <= 0.6)):
            cat_cols.append(col + "_cat")
    
            if ((sum(trn_df[col] < 0) > 0) & (sum(tst_df[col] < 0) > 0) & (sum(trn_df[col] == 0) > 0) & (sum(tst_df[col] == 0) > 0)):
                #print(col)
    
                #Trn DF
                conditions_trn = [
                                trn_df[col] < 0, 
                                trn_df[col] == 0, 
                                (trn_df[col] > 0) & (trn_df[col] <= trn_df[col].quantile(0.99)),
                                (trn_df[col] > trn_df[col].quantile(0.99))
                             ]
    
                choices_trn = [-1, 0, 1, 2]
    
                # Tst DF
                conditions_tst = [
                        tst_df[col] < 0, 
                        tst_df[col] == 0, 
                        (tst_df[col] > 0) & (tst_df[col] <= trn_df[col].quantile(0.99)),
                        (tst_df[col] > trn_df[col].quantile(0.99))
                     ]
        
                choices_tst = [-1, 0, 1, 2]
    
            elif ((sum(trn_df[col] <= 0) > 0) & \
                  (sum((trn_df[col] > 0) & (trn_df[col] <= trn_df[col].quantile(0.99))) > 0) & \
                  (sum(trn_df[col] > trn_df[col].quantile(0.99)) > 0) & \
                  (sum(tst_df[col] <= 0) > 0) & \
                  (sum((tst_df[col] > 0) & (tst_df[col] <= trn_df[col].quantile(0.99))) > 0) & \
                  (sum(tst_df[col] > trn_df[col].quantile(0.99)) > 0)):
                #Trn DF
                conditions_trn = [
                                trn_df[col] <= 0, 
                                (trn_df[col] > 0) & (trn_df[col] <= trn_df[col].quantile(0.99)),
                                (trn_df[col] > trn_df[col].quantile(0.99))
                             ]
    
                choices_trn = [0, 1, 2]
    
                # Tst DF
                conditions_tst = [
                        tst_df[col] <= 0, 
                        (tst_df[col] > 0) & (tst_df[col] <= trn_df[col].quantile(0.99)),
                        (tst_df[col] > trn_df[col].quantile(0.99))
                     ]
        
                choices_tst = [0, 1, 2]
            else:
                conditions_trn = [
                trn_df[col] <= 0, 
                trn_df[col] > 0
                     ]
    
                choices_trn = [0, 1]
    
                # Tst DF
                conditions_tst = [
                        tst_df[col] <= 0, 
                        tst_df[col] > 0
                     ]
        
                choices_tst = [0, 1]

            #upd_cat_ord[col + "_cat"] = CategoricalDtype(choices)
            
            try:
                upd_cols_trn[col + "_cat"] = np.select(conditions_trn, choices_trn)
            except:
                print("Error creating trn categorical variables")
    
            #upd_cat_ord[col + "_cat"] = CategoricalDtype(choices)
            
            try:
                upd_cols_tst[col + "_cat"] = np.select(conditions_tst, choices_tst)
            except:
                print("Error creating tst categorical variables")
    
        else:
            #drop_cols.append(col)
    
            # Trn DF
            try:
                upd_cols_trn[col + "_log"] = np.log(abs(trn_df[col]) + 0.001) * ((trn_df[col] + 0.0001)/(abs(trn_df[col] + 0.0001)))
                #upd_cols_trn[col + "_log"] = trn_df[col]
            except:
                print("Error creating log transformed variables")
    
            # Tst DF
            try:
                upd_cols_tst[col + "_log"] = np.log(abs(tst_df[col]) + 0.001) * ((tst_df[col] + 0.0001)/(abs(tst_df[col] + 0.0001)))
                #upd_cols_tst[col + "_log"] = trn_df[col]
            except:
                print("Error creating log transformed variables")
    
    
    trn_df_xfm = pd.DataFrame(upd_cols_trn)
    tst_df_xfm = pd.DataFrame(upd_cols_tst)

    trn_df_xfm.reset_index(drop=True, inplace=True)
    tst_df_xfm.reset_index(drop=True, inplace=True)

    if df_cat is not None:
        # reset indices here?
        df_cat.reset_index(drop=True, inplace=True)
        trn_df_xfm = pd.concat([trn_df_xfm, df_cat], axis=1)
        tst_df_xfm = pd.concat([tst_df_xfm, df_cat], axis=1)
    
        for col in df_cat.columns:
            cat_cols.append(col)
    
    for col in cat_cols:
        trn_df_xfm[col] = pd.Categorical(trn_df_xfm[col])
        tst_df_xfm[col] = pd.Categorical(tst_df_xfm[col])
    
    return(trn_df_xfm, tst_df_xfm, cat_cols)
    