'''
Measures for computing individual and system productivity by gender and
seniority.
Input: Assumes a pandas DataFrame containing the columns:
PaperId, AuthorId, Date, Gender, ScientificAge, Seniority
Output: List with the columns:
Date, All, [Genders] [Seniorities]
'''
import pandas as pd


def assign_bands(n, intervals):
    for i,j in intervals.values():
        if i==0:
            out = f'{i}-{j}'
        if i < n <= j:
            out = f'{i}-{j}'
    return out


def compute_sys_productivity(df, period, earliest, latest, genders, intervals):
    dates = pd.date_range(start=earliest, end=latest, freq='M')

    results = [['Date','All']+[i for i in genders.values()]+[f'{i[0]}-{i[1]}' for i in intervals.values()]]

    for start in dates:
        # Last 6 months so current period = past 6 months productivity
        start_m, end_m = start.to_period('M')-period, start.to_period('M')
        window = df[(df['Date'] >= start_m) & (df['Date'] <= end_m)]

        uniq_authors_A = len(set(window['AuthorId']))
        authorships_A = window.shape[0]
        productivity_A = authorships_A / uniq_authors_A

        temp = []
        for k,v in genders.items():
            df_G = window[window['Gender']==k]['AuthorId']
            uniq_authors_G = len(set(df_G))
            authorships_G = df_G.shape[0]
            productivity_G = authorships_G / uniq_authors_G
            temp.append(productivity_G)

        temp2 = []
        for k,v in intervals.items():
            label = f'{v[0]}-{v[1]}'
            df_S = window[window['Seniority']==label]['AuthorId']
            uniq_authors_S = len(set(df_S))
            authorships_S = df_S.shape[0]
            productivity_S = authorships_S / uniq_authors_S
            temp2.append(productivity_S)

        results.append([str(end_m),productivity_A]+temp+temp2)
    return results


def compute_ind_productivity(df, period, earliest, latest, genders, intervals):
    dates = pd.date_range(start=earliest, end=latest, freq='M')
    results = [['Date','All']+[i for i in genders.values()]+[f'{i[0]}-{i[1]}' for i in intervals.values()]]

    for start in dates:
        # Last 6 months so current period = past 6 months productivity
        start_m, end_m = start.to_period('M')-period, start.to_period('M')
        start_5y = end_m-59
        window = df[(df['Date'] >= start_m) & (df['Date'] <= end_m)]
        window_5y = df[(df['Date'] >= start_5y) & (df['Date'] <= end_m)]

        uniq_authors_A = len(set(window_5y['AuthorId']))
        authorships_A = window.shape[0]
        productivity_A = authorships_A / uniq_authors_A

        temp = []
        for k,v in genders.items():
            df_G = window_5y[window_5y['Gender']==k]['AuthorId']
            uniq_authors_G = len(set(df_G))
            authorships_G = window[window['Gender']==k].shape[0]
            productivity_G = authorships_G / uniq_authors_G
            temp.append(productivity_G)

        temp2 = []
        for k,v in intervals.items():
            label = f'{v[0]}-{v[1]}'
            df_S = window_5y[window_5y['Seniority']==label]['AuthorId']
            uniq_authors_S = len(set(df_S))
            authorships_S = window[window['Seniority']==label].shape[0]
            productivity_S = authorships_S / uniq_authors_S
            temp2.append(productivity_S)

        results.append([str(end_m),productivity_A]+temp+temp2)
    return results
