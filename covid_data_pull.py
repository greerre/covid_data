import pandas as pd
import numpy
import math


## filepath to save location ##
output_filepath = 'YOUR FILE PATH HERE'

## LOAD DATA SETS ##
## NYT data containing all reported cases and deaths by FIPS code ##
covid_df = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv')

## Government data containing population by county, which is needed for FIPS ##
pop_df = pd.read_csv('https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/co-est2019-alldata.csv', encoding = 'ISO-8859-1')

## MIT Election lab data on election returns by FIPS ##
poli_df = pd.read_csv('https://dataverse.harvard.edu/api/access/datafile/3641280?format=original&amp;gbrecs=true')


## CLEAN DATA ##
## translate gov data into fips code ##
def format_fips(row):
    row['fips'] = float(
                    '{0:0=2d}'.format(row['STATE']) +
                    '{0:0=3d}'.format(row['COUNTY'])
                )
    return row

pop_df = pop_df.apply(format_fips, axis=1)

## New York is not broken by county in the NYT file and has no FIPS ##
## Assign a dummy fips for matching ##
covid_df['fips'] = numpy.where(covid_df['county'] == 'New York City', 99999.0, covid_df['fips'])
pop_df['fips'] = numpy.where(
                            (
                                numpy.isin(
                                    pop_df['CTYNAME'],
                                    [
                                        'Kings County',
                                        'Queens County',
                                        'New York County',
                                        'Bronx County',
                                        'Richmond County'
                                    ]
                                ) &
                                (pop_df['STNAME'] == 'New York')
                            ),
                            99999.0,
                            pop_df['fips']
)


## Calculate political affiliation of each county ##
## Kinda lazy ... assuming all counties had only 1 candidate / party and at least 1 vote for republican ##
poli_df = poli_df[
            (poli_df['year'] == 2016) &
            (poli_df['office'] == 'President') &
            (poli_df['party'] == 'republican')

]

poli_df['pct_republican'] = poli_df['candidatevotes'] / poli_df['totalvotes']

## avoid potential dupes from my lazy approach ##
poli_df = poli_df.groupby(['FIPS'])['pct_republican'].mean().reset_index()
poli_df = poli_df.rename(columns={'FIPS' : 'fips'})

## merge data sets ##
## add 2019 estiamted county populations to the covid data ##
covid_df = pd.merge(
    covid_df,
    pop_df.groupby(['fips'])['POPESTIMATE2019'].sum().reset_index()[[
        'fips',
        'POPESTIMATE2019'
    ]],
    on=['fips'],
    how='left'
)

## add politcal data ##
covid_df = pd.merge(
    covid_df,
    poli_df,
    on=['fips'],
    how='left'
)



## calculate basic metrics ##
covid_df['case_penetration'] = covid_df['cases'] / covid_df['POPESTIMATE2019']


## add rate metrics ##
covid_df['days_since_first_case'] = covid_df.groupby(['fips']).cumcount() + 1
def add_rates (row):
    ## days since 100th case ##
    covid_100_df = covid_df.copy()
    covid_100_df = covid_100_df[
        (covid_100_df['fips'] == row['fips']) &
        (covid_100_df['cases'] >= 100)
    ]
    row['days_since_100th_case'] = row['days_since_first_case'] - covid_100_df['days_since_first_case'].min()
    ## days since .05% penetration ##
    covid_05_df = covid_df.copy()
    covid_05_df = covid_05_df[
        (covid_05_df['fips'] == row['fips']) &
        (covid_05_df['case_penetration'] >= .0005)
    ]
    row['days_since_05pct_penetration'] = row['days_since_first_case'] - covid_05_df['days_since_first_case'].min()
    ## day over day ##
    covid_dod_df = covid_df.copy()
    covid_dod_df = covid_dod_df[
        (covid_dod_df['fips'] == row['fips']) &
        (covid_dod_df['days_since_first_case'] == row['days_since_first_case'] - 1)
    ]
    row['case_growth_dod_abs'] = row['cases'] - covid_dod_df['cases'].max()
    row['case_growth_dod_rel'] = row['cases'] / covid_dod_df['cases'].max() - 1
    ## week over week ##
    covid_wow_df = covid_df.copy()
    covid_wow_df = covid_wow_df[
        (covid_wow_df['fips'] == row['fips']) &
        (covid_wow_df['days_since_first_case'] == row['days_since_first_case'] - 7)
    ]
    row['case_growth_wow_abs'] = row['cases'] - covid_wow_df['cases'].max()
    row['case_growth_wow_rel'] = row['cases'] / covid_wow_df['cases'].max() - 1
    ## time to double ##
    ## convert weekly rate to daily ##
    daily_rate = (1 + row['case_growth_wow_rel']) ** (1 / 7) - 1
    ## then calc days to double ##
    try:
        row['days_to_double'] = math.log(2) / math.log(1 + daily_rate)
    except:
        row['days_to_double'] = numpy.nan
    return row


covid_df = covid_df.apply(add_rates, axis=1)

covid_df.to_csv('{0}/covid_data.csv'.format(output_filepath))
