# Convert names to geonames IRI

data from http://download.geonames.org/export/dump/countryInfo.txt

copied to geonames.tsv

code would be:

```
df = pd.read_csv("geonames.tsv", sep='\t')
lookup = df[["ISO3", "geonameid"]].set_index("ISO3").to_dict()['geonameid']
```

Could then be used to replace values in a table like this:

```
df_fertiliser = pd.read_csv("https://raw.githubusercontent.com/ludemannc/FUBC_1_to_9_2022/refs/heads/main/results/FUBC_1_to_9_data.csv")
df_fertiliser["geonameid"] = df_fertiliser.ISO3_code.apply(lambda code: lookup[code])
df_fertiliser
```