import re
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def map_and_explode_features(df, mapping_dict):
    df_copy = df.copy()
    
    df_copy['mapped_features'] = df_copy['feature'].map(
        lambda x: list(mapping_dict.get(x, [x])) if isinstance(mapping_dict.get(x, x), set) 
        else [mapping_dict.get(x, x)]
    )
    
    df_exploded = df_copy.explode('mapped_features')
    
    df_exploded['feature_individual'] = df_exploded['mapped_features']
    df_exploded = df_exploded.drop('mapped_features', axis=1)
    
    df_exploded = df_exploded.reset_index(drop=True)
    
    return df_exploded


def extract_feature_number(value):
    match = re.search(r'feature_(\d+)', value)
    if match:
        return f'feature_{match.group(1)}'
    else:
        return value
    
    
def get_feat_imp_percentile_df(feature_importance, trn_df_xfm, correlated_groups, feature_dict_trn, recurring, product, client_size, source_df):
    logger.info(f'LENGTH FEAT IMP DF: {len(feature_importance)}')
    logger.info(f'LENGTH TRN_DF_XFM: {len(trn_df_xfm.columns)}')

    feature_importance_df = pd.DataFrame({
        'feature': trn_df_xfm.columns,
        'importance': feature_importance
    }).sort_values('importance', ascending=False)
    
    
    # Extract individual features from the grouped feature
    logger.info('created df')
    correlated_groups = {key.replace(' ', '_'): value for key, value in correlated_groups.items()}
    mapped_dict = {key: correlated_groups.get(value, value) for key, value in feature_dict_trn.items()}
    feature_importance_df['feature'] = feature_importance_df['feature'].apply(extract_feature_number)
    feature_importance_df = map_and_explode_features(feature_importance_df, mapped_dict)
    feature_importance_df['Recurring'] = recurring
    feature_importance_df['Product'] = product
    feature_importance_df['client_size'] = client_size
    
    # Get percentiles of top 3 features
    logger.info('created feature importance df')
    top_3_features = feature_importance_df.iloc[:3]
    top_3_feature_names = top_3_features['feature_individual'].unique().tolist()
    top_3_feature_names.append('client_number')

    features_source_df = source_df[source_df['Recurring'] == recurring][top_3_feature_names]

    # add percentiles for the important features at the client level
    for col in features_source_df.columns:
        features_source_df[f'{col}_percentile'] = features_source_df[col].rank(pct=True) * 100

    features_source_df.drop('client_number_percentile', axis = 1, inplace = True)

    top_3_feature_names.remove('client_number')
    features_source_df.drop(top_3_feature_names, axis = 1, inplace = True)

    # add model details to the percentile df
    features_source_df['recurring'] = recurring
    features_source_df['product'] = product
    features_source_df['client_size'] = client_size
    features_source_df.reset_index(drop = True, inplace = True)
    
    # Reorder features_source_df
    
    percentile_cols = features_source_df.filter(like='percentile').columns.tolist()
    features_source_df = features_source_df[['client_number', 'product', 'recurring', 'client_size'] + percentile_cols]
    
    return feature_importance_df, features_source_df, top_3_feature_names