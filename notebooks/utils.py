import numpy as np
import pandas as pd
from typing import Tuple

def load_prep_us_data(cases_path, deaths_path) -> pd.DataFrame:
    df_cases = pd.read_csv(cases_path)
    df_death = pd.read_csv(deaths_path)

    cases_id_vars = ['UID',
     'iso2',
     'iso3',
     'code3',
     'FIPS',
     'Admin2',
     'Province_State',
     'Country_Region',
     'Lat',
     'Long_',
     'Combined_Key']

    death_id_vars = ['UID',
     'iso2',
     'iso3',
     'code3',
     'FIPS',
     'Admin2',
     'Province_State',
     'Country_Region',
     'Lat',
     'Long_',
     'Combined_Key',
     'Population']

    df_cases_m = pd.melt(df_cases, id_vars=cases_id_vars)
    df_cases_m["variable"] = pd.to_datetime(df_cases_m["variable"])
    df_cases_m.rename(columns = {"variable": "date", "value": "num_cases"}, inplace=True)

    df_death_m = pd.melt(df_death, id_vars=death_id_vars)
    df_death_m["variable"] = pd.to_datetime(df_death_m["variable"])
    df_death_m.rename(columns = {"variable": "date", "value": "num_death"}, inplace=True)

    df_us = pd.merge(df_cases_m[["Province_State", "Admin2", "Combined_Key", "date", 
                                 "num_cases"]],
                     df_death_m[["Province_State", "Admin2", "Combined_Key", "date", 
                                 "Population", "num_death"]],
                     left_on = ["Province_State", "Admin2", "Combined_Key", "date"],
                     right_on =  ["Province_State", "Admin2", "Combined_Key", "date"])
    
    return df_us


def return_donor_treatment_pools(df: pd.DataFrame, donors: list, 
                                 treatments: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    df_donors = df[df.Admin2.isin(set(donors) - set(treatments))].reset_index(drop=True)

    df_treatments = df[df.Admin2.isin(treatments)].reset_index(drop=True)

    df_treatment_agged = df_treatments.groupby(["date"])[["num_cases"]].sum().reset_index()
    
    return df_donors, df_treatment_agged



def get_ca_donor_counties(df, treatment_counties, decision_date, num_cases_thresh) -> list:    

    df_CA = df[df.Province_State == "California"].reset_index(drop=True)
    
    relevant_counties = df_CA[(df_CA["date"] == decision_date) & 
                              (df_CA["num_cases"] >= num_cases_thresh)]["Admin2"].tolist()
    
    donor_counties = list(set(relevant_counties) - set(treatment_counties))
    
    return donor_counties


def prep_donor_df(df) -> pd.DataFrame:
    df_pivot = df[["date", "Admin2", "num_cases"]].pivot_table(columns = "date", 
                                                               index = "Admin2").T
    df_pivot = df_pivot.reset_index().drop(columns = ["level_0"])

    df_pivot.columns.name = None
    
    return df_pivot

def prep_treatment_df(df) -> pd.DataFrame:
    df_pivot = df.pivot_table(columns = "date").reset_index(drop=True).T
    return df_pivot