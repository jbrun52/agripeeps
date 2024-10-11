#start 
#Main class
from typing import Optional
from pydantic import BaseModel
import pandas as pd
from datetime import date
import create_data
import logging
import itertools
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG, 
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

import warnings
warnings.filterwarnings("ignore")

from sentier_data_tools import (
    DatasetKind,
    Dataset,
    Demand,
    Flow,
    FlowIRI,
    GeonamesIRI,
    ModelTermIRI,
    ProductIRI,
    SentierModel,
)
import DirectFertiliserEmission as dfe

## Attention : I would like demand to come from user input, I need mapping from natural language to IRI for product and geonames

class UserInput(BaseModel):
    product_iri: ProductIRI
    unit : ProductIRI
    amount: float
    crop_yield_val : Optional[float] = None
    fertilizer_amount : Optional[float] = None
    climate_type : Optional[str] = None
    spatial_context: GeonamesIRI = GeonamesIRI("https://sws.geonames.org/2782113")
    year : Optional[str] = '2018'
    begin_date: Optional[date] = None
    end_date: Optional[date] = None
    
    class Config:
        arbitrary_types_allowed = True

class RunConfig(BaseModel):
    num_samples: int = 1000

class Crop(SentierModel):
    def __init__(self, user_input: UserInput, run_config: RunConfig):
        self.aliases = {ProductIRI(
            "http://data.europa.eu/xsp/cn2024/100500000080"
            ): "corn",
            ProductIRI(
            "http://data.europa.eu/xsp/cn2024/060011000090"
            ): "crop",
            ProductIRI(
            "http://data.europa.eu/xsp/cn2024/310200000080"
            ): "mineral_fertilizer",
            ProductIRI(
            "https://vocab.sentier.dev/model-terms/crop_yield"
            ): "crop_yield",
            ProductIRI(
            "http://purl.org/dc/terms/date"
            ) : 'year',
            ProductIRI(
            "https://vocab.sentier.dev/model-terms/nitrogen_n2o_emission_factor"
            ) : 'emission_factor'}
        # Assuming user_input maps to demand in SentierModel
        super().__init__(demand=user_input, run_config=run_config)

    def run_create_data(self) :
        create_data.reset_db()
        create_data.create_yield_local_datastorage()
        create_data.create_fertiliser_local_datastorage()
        create_data.create_emissionfactors_local_datastorage()

    def select_right_value_from_df(self, df, strategy = "first"):
        if strategy == "first":
            return df.values[0]
    
    def get_all_input(self) :
        # get all BOM tables and PARAMETERS
        agridata_bom = self.get_model_data(
                product=self.demand.product_iri, kind=DatasetKind.BOM
                )
        agridata_bom_exact = self.merge_datasets_to_dataframes(agridata_bom['exactMatch'])
        agridata_param = self.get_model_data(
                product=self.demand.product_iri, kind=DatasetKind.PARAMETERS
            )
        agridata_param_exact = self.merge_datasets_to_dataframes(agridata_param['exactMatch'])
        agridata_param_broader = self.merge_datasets_to_dataframes(agridata_param['broader'])
        #Define climate
        logging.info("Getting climate")
        if self.demand.climate_type is None:
            self.climate_key = 'default'
        else:
            if self.demand.climate_type not in ['wet', 'dry']:
                logging.error(f"Invalid climate type value: {self.demand.climate_type}. Expected 'wet', 'dry', or None.")
            self.climate_key = self.demand.climate_type
            
        #Define fertilizer amount
        if self.demand.fertilizer_amount is None :            
            if self.mineral_fertilizer in [ProductIRI(col) for col in agridata_bom_exact.columns] and agridata_bom_exact.location == self.demand.spatial_context:
                if self.demand.year in list(agridata_bom_exact.dataframe.year):
                    self.fertilizer_amount = self.select_right_value_from_df(agridata_bom_exact.dataframe[agridata_bom_exact.dataframe.year == self.demand.year])
                else:
                    logging.error(f"year not available : {agridata_bom_exact}")
        else : 
            self.fertilizer_amount = self.demand.fertilizer_amount
        
        logging.info(f"fertilizer amount: {self.fertilizer_amount}")
        
        #Define yield
        if self.demand.crop_yield_val is None :
            if self.crop_yield in [ProductIRI(col) for col in agridata_param_exact.columns] and agridata_param_exact.location == self.demand.spatial_context:
                if self.demand.year in list(agridata_param_exact.dataframe.year):
                    self.crop_yield_val = self.select_right_value_from_df(agridata_param_exact.dataframe[i.dataframe.year == self.demand.year])
                    logging.info(f"Set input data: {self.crop_yield}")
                else:
                    logging.error(f"year not available : {agridata_param_exact}")
        else :
            self.crop_yield_val = self.demand.crop_yield_val

        logging.info(f"crop yield: {self.crop_yield_val}")

        #Getting emission factor
        if self.emission_factor in [ProductIRI(col) for col in agridata_param_exact.columns]:
            if self.climate_key in agridata_param_exact.columns:
                self.emission_factor_val = agridata_param_exact.database.query(f"climate_type == {self.climate_key} and fert_type in ['default','inorganic']")
                logging.info(f"Emission factor: {self.emission_factor_val}")
        else : 
            print("couldn't find exact match")
            #if self.climate_key in agridata_param_broader.columns:
            climate_key_vals = ['default','inorganic']
            self.emission_factor_val = agridata_param_broader.query(f"climate_type == @self.climate_key and fert_type.isin(@climate_key_vals)")
            
            logging.info(f"Emission factor: {self.emission_factor_val} for broader concept")
        
                                
        
    def get_emissions(self):
        #self.emission_per_ha = dfe.run(self.demand.product_iri, self.fertilizer_amount, self.emission_factor_val, self.climate_key)

        df_emissions = self.emission_factor_val
        df_emissions["fertiliser_input"] = self.fertilizer_amount
        df_emissions["N2O emission"] = (28+16)/28 * df_emissions["fertiliser_input"] * df_emissions['emission_factor']
        # this should be handled by unit conversion at some point
        df_emissions["N2O emission per ha"] = df_emissions["N2O emission"] * 10000
        self.emission_per_ha = df_emissions
        logging.info("Getting emission from fertilizer")
        
    def run(self):
        self.run_create_data()
        self.get_all_input()
        self.get_emissions()
        return self.emission_per_ha
      