#start 
#Main class
from typing import Optional
from pydantic import BaseModel
import pandas as pd
from datetime import date
import logging
logging.basicConfig(
    filename="app.log",
    level=logging.DEBUG, 
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)

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
            yield_input = False
        if self.demand.fertilizer_amount is None :
            self.demand.fertilizer_amount = 70 #To be modified as a function of self.masterDB
            fert_input = False
        logging.info("Getting crop yield and fertilizer amount")

    #this can be implemented when we decide how to determine the spatial context without user input
            #spatial_input = False
    
    #first user message: model parameters
    message = ("Based on your input, the following model parameters were selected:\n"
                   f"Crop type: {product_iri}\n"
                   f"Functional unit: Production of {amount}{unit} of selected crop \n" #maybe later mapped onto natural language
                   f"Region: {spatial_context if spatial_input == True else 'not specified, default: '}\n"
                   f"Fertiliser: {fertilizer_amount if fert_input == True else 'not specified, default value: '}\n"
                   f"Beginning date: {begin_date if begin_input == True else 'not specified, default: '{begin_date}}\n"
                   f"End date: {end_date if begin_input == True else 'not specified, default: '{end_date}}")
        logging.info(f"User output {message}")
        return(message)
    
        
    def get_emissions(self) :
        self.fertilizer_n_per_ha_wet = n2o.run(self.demand.product_iri, self.demand.fertilizer_amount, 'wet')
        self.fertilizer_n_per_ha_dry = n2o.run(self.demand.product_iri, self.demand.fertilizer_amount, 'dry')
        logging.info("Getting emission from fertilizer")
        
    def run(self):
        self.get_master_db()
        self.get_all_input()
        self.get_emissions()

#Output to user: N2O amounts per crop per climate
n2o_user_output = (f"N2O emissions to air for {amount}{unit} of {product_iri} : {} \n"
                   f"Climate conditions:"{climate})
logging.info(n2o_user_output)
return(n2o_user_output)

#Function to transform logs saved into csv
import csv

def log_to_csv(log_file, csv_file):
    with open(log_file, 'r') as log_f, open(csv_file, 'w', newline='') as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(['Time', 'Level', 'Message'])  # Write CSV header
        
        for line in log_f:
            parts = line.strip().split(' - ', 2)  # Split log line into components
            if len(parts) == 3:
                writer.writerow(parts)  # Write log components into CSV

# Convert the log file to a CSV file
log_to_csv('app.log', 'log_output.csv')
