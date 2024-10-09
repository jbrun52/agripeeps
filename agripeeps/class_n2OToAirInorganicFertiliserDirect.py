# Adapted from HESTIA https://gitlab.com/hestia-earth/hestia-engine-models/-/blob/develop/hestia_earth/models/ipcc2019/n2OToAirInorganicFertiliserDirect.py?ref_type=heads
from enum import Enum
from pydantic import BaseModel

class n2OToAirInorganicFertiliserDirect(BaseModel):
    def __init__(self):
        self.N2O_factors = {
            # All N inputs in dry climate
            'dry': {
                'value': 0.005,
                'min': 0,
                'max': 0.011
            },
            'wet': {
                # Synthetic fertiliser inputs in wet climate
                self.TermTermType.INORGANICFERTILISER: {
                    'value': 0.016,
                    'min': 0.013,
                    'max': 0.019
                },
                # Other N inputs in wet climate
                self.TermTermType.ORGANICFERTILISER: {
                    'value': 0.006,
                    'min': 0.001,
                    'max': 0.011
                },
                self.TermTermType.CROPRESIDUE: {
                    'value': 0.006,
                    'min': 0.001,
                    'max': 0.011
                }
            },
            'default': {
                'value': 0.01,
                'min': 0.001,
                'max': 0.018
            },
            'flooded_rice': {
                'value': 0.004,
                'min': 0,
                'max': 0.029
            }
        }

    class TermTermType(Enum):
        INORGANICFERTILISER = 1
        ORGANICFERTILISER = 2
        CROPRESIDUE = 3
    
    def _is_wet(self, ecoClimateZone: str = None):
        return get_ecoClimateZone_lookup_value(ecoClimateZone, 'wet') == 1 if ecoClimateZone else None
    
    def ecoClimate_factors(self, factors: dict, input_term_type: 'n2OToAirInorganicFertiliserDirect.TermTermType' = None, wet_climate: str = None):
        is_wet = wet_climate == "wet"
        factors_key = 'default' if is_wet is None else 'wet' if is_wet else 'dry'
        return (factors[factors_key].get(input_term_type, factors[factors_key]), wet_climate is None)
    
    def get_N2O_factors(self, input_term_type: 'n2OToAirInorganicFertiliserDirect.TermTermType', wet_climate: str = None):
        return self.ecoClimate_factors(self.N2O_factors, input_term_type, wet_climate)
    
    def _emission(self, value: float, min: float, max: float, sd: float, aggregated: bool = False):
        emission = {
            'value': [value],
            'min': [min],
            'max': [max],
            'sd': [sd],
            'methodModelDescription': 'Aggregated version' if aggregated else 'Disaggregated version'
        }
        return emission
    
    def _run(self, N_total: float, wet_climate: str = None):
        converted_N_total = N_total * (28 + 16) / 28
        factors, aggregated = self.get_N2O_factors(self.TermTermType.INORGANICFERTILISER, wet_climate=wet_climate)

        value = converted_N_total * factors['value']
        min_value = converted_N_total * factors['min']
        max_value = converted_N_total * factors['max']
        sd = converted_N_total * (factors['max'] - factors['min']) / 4
        
        return [self._emission(value, min_value, max_value, sd, aggregated=aggregated)]
    
    def run(self, fertilizer_n_per_ha: float, wet_climate: str = None):
        return self._run(fertilizer_n_per_ha, wet_climate)

