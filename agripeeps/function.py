#find match
from sentier_data_tools.iri import ProductIRI
import pandas as pd
from datetime import date
import logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def find_match_IRI(product_IRI: ProductIRI, unique_IRI_list : list, allow_broader:bool = True):
    if product_IRI in unique_IRI_list :
        print('yaas')
        closest_match_IRI = product_IRI
        logging.log(logging.INFO, f"Exact match found for {product_IRI.display()}")
    else :
        if allow_broader :
            for broader_IRI in product_IRI.broader():
                if broader_IRI in unique_IRI_list:
                    closest_match_IRI = broader_IRI
                    logging.log(logging.INFO, f"Found broader match for {broader_IRI.display()}")
                    break                   
        else: 
            logging.ERROR('Exact match not found, please set allow_broader to True to find closest match')
    return closest_match_IRI

def format_df(df : pd.DataFrame(), productIRI_columns_list):
    df[productIRI_columns_list] = df[productIRI_columns_list].map(lambda x : ProductIRI(x))
    return df




    