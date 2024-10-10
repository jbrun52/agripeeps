#start 
#Main class
from typing import Optional
from pydantic import BaseModel
import pandas as pd
from datetime import date
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

from sentier_data_tools.iri import ProductIRI #, GeonamesIRI
from sentier_data_tools import SentierModel
import n2OToAirInorganicFertiliserDirect as n2o

## Attention : I would like demand to come from user input, I need mapping from natural language to IRI for product and geonames

class UserInput(BaseModel):
    product_iri: ProductIRI
    unit : str
    #properties: Optional[list]
    amount: float
    crop_yield : Optional[float]
    fertilizer_amount : Optional[float]
    #spatial_context: GeonamesIRI = GeonamesIRI("https://sws.geonames.org/6295630/")
    begin_date: Optional[date] = None
    end_date: Optional[date] = None

class RunConfig(BaseModel):
    num_samples: int = 1000

class Crop(SentierModel):
    def get_master_db(self) -> None :
        #self.masterDB = pd.read_csv('../docs/MasterDB.csv')
        logging.info("Getting master db")
        pass

    def get_all_input(self) -> float :
        if self.user_input.crop_yield is None :
            self.user_input.crop_yield = 7.0 #to be modified as a function of self.masterDB
        if self.user_input.fertilizer_amount is None :
            self.user_input.fertilizer_amount = 70 #To be modified as a function of self.masterDB
        logging.info("Getting crop yield and fertilizer amount")
        
    def get_emissions(self) :
        self.fertilizer_n_per_ha_wet = n2o.run(self.user_input.fertilizer_amount, 'wet')
        self.fertilizer_n_per_ha_dry = n2o.run(self.user_input.fertilizer_amount, 'dry')
        logging.info("Getting emission from fertilizer")
        
    def run(self):
        self.get_master_db()
        self.get_all_input()
        self.get_emissions()
        print('wet', self.fertilizer_n_per_ha_wet)
        print('dry', self.fertilizer_n_per_ha_dry)
        
"""
class SentierModel:
    def __init__(self, user_input: UserInput, run_config: RunConfig):
        self.user_input = user_input
        self.run_config = run_config
        if self.user_input.begin_date is None:
            self.user_input.begin_date = date(date.today().year - 5, 1, 1)
        if self.user_input.end_date is None:
            self.user_input.end_date = date(date.today().year + 5, 1, 1)

    def get_master_db(self) -> None :
        self.masterDB = pd.read_csv('../docs/MasterDB.csv')

    def get_all_input(self) -> float :
        if self.user_input.yield is None :
            self.user_input.yield = 7.0 #to be modified
        if self.user_input.fertilizer_amount is None :
            self.user_input.fertilizer_amount = 70 #To be modified

    def get_emission(self) -> float :
        #developed by Oliver
        SentierModel.nitrogen_emission = 9.0 #example : to be modified

    def get_model_data(self) -> list[pd.DataFrame]:
        pass

    def prepare(self) -> None:
        self.get_model_data()
        self.data_validity_checks()
        self.resample()

    def run(self) -> list[Demand]:
        pass
"""