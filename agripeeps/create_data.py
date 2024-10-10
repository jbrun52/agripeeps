from datetime import date
from pathlib import Path

import pandas as pd
from loguru import logger
import function as fct

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
    df_fertiliser = df_fertiliser[["Year", "Country", "CropIRI", "N_kg_m2"]]
    df_fertiliser["Datasource"] = "FAO"
    return df_fertiliser


def create_fertiliser_local_datastorage(reset: bool = True):
    if reset:
        sdt.reset_local_database()

    fertiliser = create_mineral_fertilizer_data()

    metadata = sdt.Datapackage(
        name="agricultural fertiliser input data from FUBC",
        description="",
        contributors=[
            {
                "title": "Oliver Hurtig",
                "role": "author",
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

    #display(fertiliser)
    for crop in fertiliser.CropIRI.unique():
        for country in fertiliser.Country.unique():
            df = fertiliser.query(f"CropIRI == @crop and Country == @country")[["Datasource", "Year", "N_kg_m2"]]
            df = df.set_axis(fertiliser_COLUMNS, axis=1)
            sdt.Dataset( 
                    name=f"fertiliser input on fields for {crop} in {country}",
                    dataframe=df,
                    product=crop,
                    columns=[{"iri": x, "unit": y} for x, y in zip(fertiliser_COLUMNS, fertiliser_UNITS)],
                    metadata=metadata,
                    kind=sdt.DatasetKind.BOM,
                    location=country,
                    version=1,
                    valid_from=date(2000, 1, 1),
                    valid_to=date(2030, 12, 31),
                ).save()


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
    data["Datasource"] = "FAO"
    data = data[["Datasource", "Year", "Country", "CropIRI", "Value"]]  
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

def create_yield_local_datastorage(reset: bool = True):
    if reset:
        sdt.reset_local_database()

    metadata = sdt.Datapackage(
        name="crop yield from FAO",
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
        ],
        homepage="https://fao.org/",
    ).metadata()
    metadata.pop("version")

    crop_yields = create_crop_yields_data()
    for crop in crop_yields.CropIRI.unique():
        for country in crop_yields.Country.unique():
            df = crop_yields.query(f"CropIRI == @crop and Country == @country")[["Datasource", "Year", "Value"]]
            df = df.set_axis(yield_COLUMNS, axis=1)

            sdt.Dataset(
                    name=f"crop yields",
                    dataframe=df,
                    product=crop,
                    columns=[{"iri": x, "unit": y} for x, y in zip(yield_COLUMNS, yield_UNITS)],
                    metadata=metadata,
                    kind=sdt.DatasetKind.PARAMETERS,
                    location=country,
                    version=1,
                    valid_from=date(2000, 1, 1),
                    valid_to=date(2028, 1, 1),
                ).save()

def create_emissionfactors_local_datastorage(reset: bool = True):
    if reset:
        sdt.reset_local_database()

    metadata = sdt.Datapackage(
        name="N2O emission factors",
        description="",
        contributors=[
            {
                "title": "Oliver Hurtig",
                "role": "author",
                "path": ""
            },
             {
                "title": "IPCC",
                "role": "data provider",
                "path": ""
            },
        ],
        homepage="https://ipcc.org/",
    ).metadata()
    metadata.pop("version")
    df_emission_factors = pd.read_csv("../docs/EF.csv", sep=';')
    df = fct.format_df(df_emission_factors, ['crop_iri','fert_iri'])
    df = df.drop("fert_iri",axis=1)
    for crop in df.crop_iri.unique():
        df_filt = df.query("crop_iri == @crop").drop("crop_iri",axis=1)
        df_filt = df_filt.set_axis(ef_COLUMNS, axis=1)
        display(df_filt)

        sdt.Dataset(
                name=f"N2O emission factors from IPCC",
                dataframe=df_filt,
                product=crop,
                columns=[{"iri": x, "unit": y} for x, y in zip(ef_COLUMNS, ef_UNITS)],
                metadata=metadata,
                kind=sdt.DatasetKind.PARAMETERS,
                location="https://sws.geonames.org/6295630/",
                version=1,
                valid_from=date(2000, 1, 1),
                valid_to=date(2028, 1, 1),
            ).save()

crop_IRI = "http://data.europa.eu/xsp/cn2024/060011000090"
geo_IRI = "http://purl.org/dc/terms/Location"

crop_IRIs = {
    "Wheat": "http://data.europa.eu/xsp/cn2024/100100000080",
    "Rice": "http://aims.fao.org/aos/agrovoc/c_6599",	
    "Potatoe": "http://data.europa.eu/xsp/cn2024/071010000080",
    "Potatoes": "http://data.europa.eu/xsp/cn2024/071010000080",
    "Maize": "http://data.europa.eu/xsp/cn2024/100500000080",
    "Maize (corn)": "http://data.europa.eu/xsp/cn2024/100500000080"
}

ef_COLUMNS = [
    "climate_type",
    "fert_type",
    "value_type",
    "https://vocab.sentier.dev/model-terms/nitrogen_n2o_emission_factor",
]

ef_UNITS = [
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://vocab.sentier.dev/units/unit/KiloGM-PER-KiloGM"
]

fertiliser_COLUMNS = [
    "https://vocab.sentier.dev/model-terms/generic/company",
    "http://purl.org/dc/terms/date",
    "http://data.europa.eu/xsp/cn2024/310200000080"
]

fertiliser_UNITS = [
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://vocab.sentier.dev/units/unit/KiloGM-PER-M2",
]

yield_COLUMNS = [
    "https://vocab.sentier.dev/model-terms/generic/company",
    "http://purl.org/dc/terms/date",
    "https://vocab.sentier.dev/model-terms/crop_yield"
]

yield_UNITS = [
    "https://www.w3.org/2001/XMLSchema#string",
    "https://www.w3.org/2001/XMLSchema#string",
    "https://vocab.sentier.dev/units/unit/KiloGM-PER-M2",
]