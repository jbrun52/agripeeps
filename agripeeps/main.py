#start 
#Main class
from typing import Optional
from pydantic import BaseModel
import pandas as pd
from datetime import date
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from sentier_data_tools.iri import ProductIRI #, GeonamesIRI
from sentier_data_tools.model import SentierModel
import n2OToAirInorganicFertiliserDirect_dev as n2o

## Attention : I would like demand to come from user input, I need mapping from natural language to IRI for product and geonames

class UserInput(BaseModel):
    product_iri: ProductIRI
    unit : str
    #properties: Optional[list]
    amount: float
    crop_yield : Optional[float] = None
    fertilizer_amount : Optional[float] = None
    #spatial_context: GeonamesIRI = GeonamesIRI("https://sws.geonames.org/6295630/")
    begin_date: Optional[date] = None
    end_date: Optional[date] = None
    
    class Config:
        arbitrary_types_allowed = True

class RunConfig(BaseModel):
    num_samples: int = 1000

class Crop(SentierModel):
    def __init__(self, user_input: UserInput, run_config: RunConfig):
        self.aliases = {}
        # Assuming user_input maps to demand in SentierModel
        super().__init__(demand=user_input, run_config=run_config)

    def get_master_db(self) -> None :
        #self.masterDB = pd.read_csv('../docs/MasterDB.csv')
        logging.info("Getting master db")
        pass
        
    def get_all_input(self) -> float :
        if self.demand.crop_yield is None :
            self.demand.crop_yield = 7.0 #to be modified as a function of self.masterDB
        if self.demand.fertilizer_amount is None :
            self.demand.fertilizer_amount = 70 #To be modified as a function of self.masterDB
        logging.info("Getting crop yield and fertilizer amount")
        
    def get_emissions(self) :
        self.fertilizer_n_per_ha_wet = n2o.run(self.demand.product_iri, self.demand.fertilizer_amount, 'wet')
        self.fertilizer_n_per_ha_dry = n2o.run(self.demand.product_iri, self.demand.fertilizer_amount, 'dry')
        logging.info("Getting emission from fertilizer")
        
    def run(self):
        self.get_master_db()
        self.get_all_input()
        self.get_emissions()

        
