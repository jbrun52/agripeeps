#start 
#Main class
from typing import Optional
from pydantic import BaseModel
import pandas as pd
from datetime import date
import create_data
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from sentier_data_tools import (
    DatasetKind,
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
    crop_yield : Optional[float] = None
    fertilizer_amount : Optional[float] = None
    climate_type : Optional[str] = None
    spatial_context: GeonamesIRI = GeonamesIRI("https://sws.geonames.org/6295630/")
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
            ): "mineral_fertiliser",
            ProductIRI(
            "https://vocab.sentier.dev/model-terms/crop_yield"
            ): "crop_yield"}
        # Assuming user_input maps to demand in SentierModel
        super().__init__(demand=user_input, run_config=run_config)

    def run_create_data(self) :
        logging.info(f"create data")
        create_data.create_example_local_datastorage()
        
    def get_all_input(self) -> None :
        #Define climate
        logging.info("Getting climate")
        if self.demand.climate_type is None:
            self.climate_key = 'default'
        else:
            if self.demand.climate_type not in ['wet', 'dry']:
                logging.error(f"Invalid climate type value: {self.demand.climate_type}. Expected 'wet', 'dry', or None.")
            self.climate_key = self.demand.climate_type
            
        logging.info("Getting crop yield and fertilizer amount")

        if self.demand.fertilizer_amount is None :
            agridata_bom = self.get_model_data(
                product=self.crop, kind=DatasetKind.BOM
            )
    
            for i in agridata_bom["exactMatch"]:
                if self.mineral_fertiliser in [ProductIRI(col["iri"]) for col in i.columns]:
                    Crop.mineral_fertiliser_data = i.dataframe
                    self.fertilizer_amount = 70 #to be modified
                    #logging.info(f"Set input data: {self.mineral_fertiliser.display()}")
        else : 
            self.fertilizer_amount = self.demand.fertilizer_amount

        if self.demand.crop_yield is None :
            agridata_param = self.get_model_data(
                product=self.crop, kind=DatasetKind.PARAMETERS
            )
            for i in agridata_param["exactMatch"]:
                if self.crop_yield in [ProductIRI(col["iri"]) for col in i.columns]:
                    Crop.crop_yield_data = i.dataframe
                    self.crop_yield = 7 #to be modified
                    #logging.info(f"Set input data: {self.crop_yield}")
        else :
            self.crop_yield = self.demand.crop_yield
            
        return agridata_bom
        
        
    def get_emissions(self) :
        self.emission_per_ha = dfe.run(self.demand.product_iri, self.fertilizer_amount, self.climate_key)
        logging.info("Getting emission from fertilizer")
        
    def run(self):
        if self.demand.crop_yield is None or self.demand.fertilizer_amount is None : #to eventually be modified
            self.run_create_data()
        self.get_all_input()
        self.get_emissions()

        
