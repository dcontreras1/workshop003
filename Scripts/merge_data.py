import pandas as pd

def load_and_standardize(file_path, year):
    df = pd.read_csv(file_path)

    rename_dicts = {
        "2015": {
            "Country": "country",
            "Happiness Score": "happiness_score",
            "Economy (GDP per Capita)": "gdp_per_capita",
            "Family": "social_support",
            "Health (Life Expectancy)": "life_expectancy",
            "Freedom": "freedom",
            "Generosity": "generosity",
            "Trust (Government Corruption)": "corruption"
        },
        "2016": {
            "Country": "country",
            "Happiness Score": "happiness_score",
            "Economy (GDP per Capita)": "gdp_per_capita",
            "Family": "social_support",
            "Health (Life Expectancy)": "life_expectancy",
            "Freedom": "freedom",
            "Generosity": "generosity",
            "Trust (Government Corruption)": "corruption"
        },
        "2017": {
            "Country": "country",
            "Happiness.Score": "happiness_score",
            "Economy..GDP.per.Capita.": "gdp_per_capita",
            "Family": "social_support",
            "Health..Life.Expectancy.": "life_expectancy",
            "Freedom": "freedom",
            "Generosity": "generosity",
            "Trust..Government.Corruption.": "corruption"
        },
        "2018": {
            "Country or region": "country",
            "Score": "happiness_score",
            "GDP per capita": "gdp_per_capita",
            "Social support": "social_support",
            "Healthy life expectancy": "life_expectancy",
            "Freedom to make life choices": "freedom",
            "Generosity": "generosity",
            "Perceptions of corruption": "corruption"
        },
        "2019": {
            "Country or region": "country",
            "Score": "happiness_score",
            "GDP per capita": "gdp_per_capita",
            "Social support": "social_support",
            "Healthy life expectancy": "life_expectancy",
            "Freedom to make life choices": "freedom",
            "Generosity": "generosity",
            "Perceptions of corruption": "corruption"
        }
    }

    df = df.rename(columns=rename_dicts[year])
    df["year"] = int(year)

    # Normalización y limpieza
    df["country"] = df["country"].str.lower().str.strip()

    columns = ["country", "year", "happiness_score", "gdp_per_capita",
               "social_support", "life_expectancy", "freedom",
               "generosity", "corruption"]

    return df[columns]

files = {
    "2015": "data/2015.csv",
    "2016": "data/2016.csv",
    "2017": "data/2017.csv",
    "2018": "data/2018.csv",
    "2019": "data/2019.csv"
}

# Unificar datasets
all_data = [load_and_standardize(path, year) for year, path in files.items()]
combined_df = pd.concat(all_data, ignore_index=True)

# Imputar nulos en 'corruption' con la mediana
if combined_df['corruption'].isnull().sum() > 0:
    median_value = combined_df['corruption'].median()
    combined_df['corruption'].fillna(median_value, inplace=True)

# Normalización nombre de algunos países
country_corrections = {
    "united states of america": "united states",
    "south korea": "korea, republic of",
    "russia": "russian federation",
    "tanzania": "tanzania, united republic of"
}

combined_df["country"] = combined_df["country"].replace(country_corrections)

combined_df.drop(columns=["generosity"], inplace=True)

combined_df.drop_duplicates(inplace=True)
combined_df.reset_index(drop=True, inplace=True)

# Guardar el archivo final
combined_df.to_csv("data/merged.csv", index=False)

print("Dataset unificado, limpiado y transformado guardado como data/merged.csv")