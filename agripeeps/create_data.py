from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger

import faostat
import country_converter as coco

import sentier_data_tools as sdt

df_lookup_geonameid = pd.read_csv("geonames.tsv", sep='\t')
lookup_geonameid = df_lookup_geonameid[["ISO3", "geonameid"]].set_index("ISO3").to_dict()['geonameid']
years = []

def create_mineral_fertilizer_data():
    
    df_fertiliser = pd.read_csv("https://raw.githubusercontent.com/ludemannc/FUBC_1_to_9_2022/refs/heads/main/results/FUBC_1_to_9_data.csv")
    crops = ['Rice', 'Wheat', 'Maize', 'Potatoes']
    df_fertiliser['N_t_ha'] = df_fertiliser['N_k_t'] / df_fertiliser['Crop_area_k_ha']    
    df_fertiliser = df_fertiliser.query("Crop in @crops")
    df_fertiliser = df_fertiliser[['Crop', 'Year', 'ISO3_code', 'N_t_ha']]
    df_fertiliser["Year"] = df_fertiliser["Year"].str[0:4]
    df_fertiliser["Country"] = df_fertiliser.ISO3_code.apply(lambda code: "https://sws.geonames.org/" + str(lookup_geonameid[code]))
    df_fertiliser['CropIRI'] = df_fertiliser.Crop.apply(lambda name: crop_IRIs[name])
    df_fertiliser['N_kg_m2'] = df_fertiliser['N_t_ha'] * 1000 / 10000    
    years = df_fertiliser["Year"].str[0:4].unique()
    df_fertiliser = df_fertiliser[["Year", "Country", "CropIRI", "N_kg_m2"]].set_axis(fertiliser_COLUMNS, axis=1)
    return df_fertiliser

def create_crop_yields_data():
    crops = ['Rice', 'Wheat', 'Maize (corn)', 'Potatoes']
    data = get_FAO_data(
            dataset_code="QCL",
            element="Yield",
            items=crops,
            list_year=years,
        )
    cc = coco.CountryConverter()
    data["ISO3"] = cc.pandas_convert(series=data["Area Code"], src="FAOcode", to='ISO3')
    data = data.query("ISO3 != 'not found'")
    data = data[["ISO3", "Item", "Year", "Value"]]
    data["Value"] = data["Value"].astype("float")/10/10000 #convert from 100g/ha to kg/m2    
    data["Country"] = data.ISO3.apply(lambda code: "https://sws.geonames.org/" + str(lookup_geonameid[code]))
    data['CropIRI'] = data.Item.apply(lambda name: crop_IRIs[name])    
    data = data[["Year", "Country", "CropIRI", "Value"]].set_axis(yield_COLUMNS, axis=1)
    return data

def get_FAO_data(dataset_code, element, items, list_year):
    element_values = faostat.get_par(dataset_code, "element")
    element_number = element_values[element]
    item_values = faostat.get_par(dataset_code, "items")
    items_number = [item_values[item] for item in items]
    data = faostat.get_data_df(
        dataset_code,
        pars={"element": element_number,
              "item": items_number,
              "year": list_year},
        coding={"area_cs": "ISO3"}
    )
    return data


def create_example_local_datastorage(reset: bool = True):
    if reset:
        sdt.reset_local_database()

    crop_yields = create_crop_yields_data()
    fertiliser = create_mineral_fertilizer_data()

    metadata = sdt.Datapackage(
        name="agricultural base data from FAO and FUBC",
        description="",
        contributors=[
            {
                "title": "Oliver Hurtig",
                "role": "author",
                "path": ""
            },
            {
                "title": "FAO",
                "role": "data provider",
                "path": ""
            },
             {
                "title": "Ludemann et al.",
                "role": "data provider",
                 "path": ""
            },
        ],
        homepage="https://fao.org/",
    ).metadata()
    metadata.pop("version")

    sdt.Dataset(
            name=f"fertiliser input on fields",
            dataframe=fertiliser,
            product="http://data.europa.eu/xsp/cn2024/060011000090",
            columns=[{"iri": x, "unit": y} for x, y in zip(fertiliser_COLUMNS, fertiliser_UNITS)],
            metadata=metadata,
            kind=sdt.DatasetKind.BOM,
            location="https://sws.geonames.org/6255148/",
            version=1,
            valid_from=date(2000, 1, 1),
            valid_to=date(2028, 1, 1),
        ).save()

    sdt.Dataset(
            name=f"crop yields",
            dataframe=crop_yields,
            product="http://data.europa.eu/xsp/cn2024/060011000090",
            columns=[{"iri": x, "unit": y} for x, y in zip(yield_COLUMNS, yield_UNITS)],
            metadata=metadata,
            kind=sdt.DatasetKind.PARAMETERS,
            location="https://sws.geonames.org/6255148/",
            version=1,
            valid_from=date(2000, 1, 1),
            valid_to=date(2028, 1, 1),
        ).save()

crop_IRIs = {
    "Wheat": "http://data.europa.eu/xsp/cn2024/100100000080",
    "Rice": "http://aims.fao.org/aos/agrovoc/c_6599",	
    "Potatoe": "http://data.europa.eu/xsp/cn2024/071010000080",
    "Potatoes": "http://data.europa.eu/xsp/cn2024/071010000080",
    "Maize": "http://data.europa.eu/xsp/cn2024/100500000080",
    "Maize (corn)": "http://data.europa.eu/xsp/cn2024/100500000080"
}

fertiliser_COLUMNS = [
    "http://purl.org/dc/terms/date",
    "http://purl.org/dc/terms/Location",
    "http://data.europa.eu/xsp/cn2024/060011000090",
    "http://data.europa.eu/xsp/cn2024/310200000080"
]

fertiliser_UNITS = [
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://vocab.sentier.dev/units/unit/KiloGM-PER-M2",
]

yield_COLUMNS = [
    "http://purl.org/dc/terms/date",
    "http://purl.org/dc/terms/Location",
    "http://data.europa.eu/xsp/cn2024/060011000090",
    "https://vocab.sentier.dev/model-terms/crop_yield"
]

yield_UNITS = [
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://vocab.sentier.dev/units/unit/KiloGM-PER-M2",
]