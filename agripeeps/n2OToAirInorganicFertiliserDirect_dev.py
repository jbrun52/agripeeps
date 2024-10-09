# Adapted from HESTIA https://gitlab.com/hestia-earth/hestia-engine-models/-/blob/develop/hestia_earth/models/ipcc2019/n2OToAirInorganicFertiliserDirect.py?ref_type=heads
from enum import Enum
from pathlib import Path

def assert_climate(wet_climate :str):
    # If wet_climate is None, set climate_key to 'default'
    if wet_climate is None:
        climate_key = 'def'
    else:
        # Ensure wet_climate is either 'wet' or 'dry'
        if wet_climate not in ['wet', 'dry']:
            logging.error(f"Invalid wet_climate value: {wet_climate}. Expected 'wet', 'dry', or None.")
            return None  # Return None in case of an error
        climate_key = wet_climate
        
    return climate_key

"""
def get_fert_type_keys(climate_key : str):
    dict_filtered = {}
    if climate_key == 'wet' :
    ...
    else :
        fert_type_key = 'default'
        dict_filtered[fert_type_key] 

    return dict_filtered


def ecoClimate_factors(factors: dict, input_term_type: TermTermType = None, wet_climate: str = None):
    if wet_clime not in ['wet','dry']
    assert wet_climate in ['wet','dry', None]
    factors_key = 'default' if wet_climate is None else wet_climate
    return (factors[factors_key].get(input_term_type, factors[factors_key]), wet_climate is None)
"""
def get_N2O_factors(
    climate_key: str = 'def',
):
    df_N2O_factors = pd.read_csv(Path(__file__).parent / "n2o_emissions.csv")
    N2O_factors_filtered = df_N2O_factors[df_N2O_factors['climate_type'] == climate_key]

    return N2O_factors_filtered

def _emission(value: float, min: float, max: float, sd: float):
    emission = {}
    emission['value'] = [value]
    emission['min'] = [min]
    emission['max'] = [max]
    emission['sd'] = [sd]
    return emission


def _run(N_total: float, wet_climate: str = None):
    climate_key = assert_climate(wet_climate)
    converted_N_total = N_total * (28+16)/28
    df_factors = get_N2O_factors(climate_key=climate_key)

    value = converted_N_total * factors['value']
    min = converted_N_total * factors['min']
    max = converted_N_total * factors['max']
    sd = converted_N_total * (factors['max'] - factors['min'])/4
    return [_emission(value, min, max, sd)]

# wet_climate = "wet", "dry", None
def run(fertilizer_n_per_ha, climate: str):
    return _run(fertilizer_n_per_ha, climate)