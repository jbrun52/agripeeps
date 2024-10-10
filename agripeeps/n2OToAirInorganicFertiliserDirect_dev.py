from enum import Enum
from pathlib import Path
import pandas as pd 
import function as fct
import logging
from sentier_data_tools.iri import ProductIRI
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def assert_climate(wet_climate :str):
    # If wet_climate is None, set climate_key to 'default'
    if wet_climate is None:
        climate_key = 'default'
    else:
        # Ensure wet_climate is either 'wet' or 'dry'
        if wet_climate not in ['wet', 'dry']:
            logging.error(f"Invalid wet_climate value: {wet_climate}. Expected 'wet', 'dry', or None.")
            return None  # Return None in case of an error
        climate_key = wet_climate
        
    return climate_key
    
def get_emission_factors(
    product_IRI: ProductIRI,
    climate_key = 'default',
):
    #read
    df_emission_factors = pd.read_csv("../docs/EF.csv", sep=';')
    #format
    df_emission_factors_formatted = fct.format_df(df_emission_factors, ['crop_iri','fert_iri'])
    unique_IRI_crop = list(pd.unique(df_emission_factors_formatted['crop_iri']))
    crop_match_IRI = fct.find_match_IRI(product_IRI, unique_IRI_crop)
    
    #filter
    emission_factors_filtered = df_emission_factors[df_emission_factors['climate_type'] == climate_key]
    emission_factors_filtered = emission_factors_filtered[emission_factors_filtered['crop_iri'] == crop_match_IRI]

    return emission_factors_filtered

def get_emission(emission_factor : pd.DataFrame(), N_total:float):
    emission_factor['emission [kg_N20/ha]'] = emission_factor.apply(lambda x : x['emission_factor']*(28+16)/28*N_total , axis = 1)
    return emission_factor


def _run(product_IRI: ProductIRI, N_total: float, wet_climate: str = None):
    climate_key = assert_climate(wet_climate)
    emission_factors = get_emission_factors(product_IRI, climate_key)
    df_emission = get_emission(emission_factors, N_total)

    logging.info(df_emission)
    return df_emission #only for testing 

# wet_climate = "wet", "dry", None
def run(product_IRI: ProductIRI, fertilizer_n_per_ha, climate: str = None):
    return _run(product_IRI, fertilizer_n_per_ha, climate)