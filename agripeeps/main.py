#start 
#Main class
from typing import Optional
from pydantic import BaseModel
import pandas as pd
from datetime import date
import create_data
import logging
import itertools
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
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
            ) : 'year'}
        # Assuming user_input maps to demand in SentierModel
        super().__init__(demand=user_input, run_config=run_config)

    def run_create_data(self) :
        create_data.reset_db()
        create_data.create_yield_local_datastorage()
        create_data.create_fertiliser_local_datastorage()

    def get_all_input(self) :
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
            agridata_bom = self.get_model_data(
                product=self.demand.product_iri, kind=DatasetKind.BOM #, location=self.demand.spatial_context
                )
            #logging.info(agridata_bom)
            Crop.list_mineral_fertilizer_data = []
            for i in agridata_bom["exactMatch"]:
                #print(self.mineral_fertilizer)
                
                if self.mineral_fertilizer in [ProductIRI(col["iri"]) for col in i.columns] and i.location == self.demand.spatial_context and self.demand.year in list(i.dataframe.year):
                    i.dataframe = i.dataframe[i.dataframe.year == self.demand.year]
                    Crop.list_mineral_fertilizer_data.append(i)
                        
                    self.fertilizer_amount = i.dataframe.mineral_fertilizer.values[0] #to be modified
                    
            if len(Crop.list_mineral_fertilizer_data) !=1 :
                logging.error(f"filtering gone wrong matches found : {len(Crop.list_mineral_fertilizer_data)}")
        else : 
            self.fertilizer_amount = self.demand.fertilizer_amount
        logging.info(f"fertilizer amount: {self.fertilizer_amount}")
        
        #Define yield
        if self.demand.crop_yield_val is None :
            agridata_param = self.get_model_data(
                product=self.demand.product_iri, kind=DatasetKind.PARAMETERS
            )
            Crop.list_yield_data = []
            for i in agridata_param["exactMatch"]:
                if self.crop_yield in [ProductIRI(col["iri"]) for col in i.columns] and i.location == self.demand.spatial_context and self.demand.year in list(i.dataframe.year):
                    i.dataframe = i.dataframe[i.dataframe.year == self.demand.year]
                    Crop.list_yield_data.append(i)
                    
                    self.crop_yield_val = i.dataframe.crop_yield.values[0]
                    logging.info(f"Set input data: {self.crop_yield}")
        else :
            self.crop_yield_val = self.demand.crop_yield_val

        logging.info(f"crop yield: {self.crop_yield_val}")
        
    def get_emissions(self) :
        self.emission_per_ha = dfe.run(self.demand.product_iri, self.fertilizer_amount, self.climate_key)
        logging.info("Getting emission from fertilizer")
        
    def run(self):
        self.run_create_data()
        self.get_all_input()
        self.get_emissions()
        
"""
    def get_master_db(self) -> None :
        logging.info(self.crop)
        agridata_bom = self.get_model_data(
            product=self.corn, kind=DatasetKind.BOM #, location=self.demand.spatial_context
        )
        logging.info(agridata_bom)
        for i in agridata_bom["exactMatch"]:
            #print(self.mineral_fertilizer)
            if self.mineral_fertilizer in [ProductIRI(col["iri"]) for col in i.columns]:
                self.mineral_fertilizer_data = i.dataframe
                logging.info(f"Set input data: {i.name}")

        # agridata_param = self.get_model_data(
        #     product=self.crop, kind=DatasetKind.PARAMETERS
        # )
        # for i in agridata_param["exactMatch"]:
        #     if self.crop_yield in [ProductIRI(col["iri"]) for col in i.columns]:
        #         self.crop_yield_data = i.dataframe
        #         logging.info(f"Set input data: {self.crop_yield}")
                
        #self.masterDB = pd.read_csv('../docs/MasterDB.csv')
        logging.info("Getting master db")
        
        return agridata_bom
"""   
   

    # def get_model_data(
    #     self,
    #     product: VocabIRI,
    #     kind: DatasetKind,
    #     location: GeonamesIRI = None
    # ) -> dict:
    #     logging.log(logging.INFO, f"{location}")
    #     results = {
    #         "exactMatch": list(
    #             Dataset.select().where(
    #                 Dataset.kind == kind,
    #                 Dataset.product == str(product),
    #                 Dataset.location == location
    #             )
    #         ),
    #         "broader": list(
    #             Dataset.select().where(
    #                 Dataset.kind == kind,
    #                 Dataset.product << product.broader(raw_strings=True),
    #                 Dataset.location == GeonamesIRI(location)
    #             )
    #         ),
    #         "narrower": list(
    #             Dataset.select().where(
    #                 Dataset.kind == kind,
    #                 Dataset.product << product.narrower(raw_strings=True),
    #                 Dataset.location == GeonamesIRI(location)
    #             )
    #         ),
    #     }
    #     for df in itertools.chain(*results.values()):
    #         df.dataframe.apply_aliases(self.aliases)

    #     return results
