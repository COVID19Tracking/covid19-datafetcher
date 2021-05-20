import os
import pandas as pd
import re
import sys
from datetime import datetime

'''
This script parses PRs testing file
https://bioportal.salud.gov.pr/api/administration/reports/minimal-info-unique-tests
This is a json file, with a record for each test.
A record looks like this:
{
  "collectedDate": "8/26/2020",
  "reportedDate": "8/26/2020",
  "ageRange": "10 to 19",
  "testType": "Total Antibodies",
  "result": "Negative",
  "city": "Lares",
  "createdAt": "12/24/2020 8:23"
}

Dates:
  Collection Date
  Report Date (test result? test repored somewhere?)
  Record creation date.
Tests:
  This file contains (as far as we've seen) 3 types of tests: PCR, antibody, antigen


This script parses the file, and generates an aggregated time series.
It deals with some data mishaps: past and future dates, missing dates.
Test types are unified and assigned a single test type value (pcr, antigen, antibody), same with results.

The script stops if there were unknown and unparsed values

'''


PCR_TESTS = ['MOLECULAR','Molecular']
ANTIGEN_TESTS = ['ANTIGENO', 'Antigens']
ANTIBODY_TESTS = ['SEROLOGICAL', 'Serological', 'Serological IgG Only', 'Total Antibodies']

POSITIVE_RESULT = ['Positive', 'Positive IgG Only', 'Positive IgM and IgG',
                   'Positive IgM Only', 'Positive 2019-nCoV', 'COVID-19 Positive',
                   'SARS-CoV-2 Positive', 'Presumptive Positive',
                   'SARS-CoV-2 Presumptive Positive']
NEGATIVE_RESULT = ['Negative', 'Not Detected', 'COVID-19 Negative',
                   'SARS-CoV-2 Negative']



def aggregate(df, field):
    # Start handling the timeseries
    dated = df[[field, 'testType_parsed', 'result_parsed', 'createdAt_parsed']].groupby([
        field, 'testType_parsed', 'result_parsed']).count().unstack().unstack()
    dated = dated.droplevel(axis=1, level=0)
    dated.columns = dated.columns.map("-".join)

    # fix dates after today
    print("Shape before striping future dates: ", dated.shape)
    dated = dated.iloc[dated.index <= datetime.now()]
    print("Shape after striping future dates: ", dated.shape)

    daterange = pd.date_range(start = dated.index.min(), end=dated.index.max(), freq='d')
    dated = dated.reindex(daterange)
    dated = dated.fillna(0)

    # add Total
    for c in ['Antigen', 'Molecular', 'Serology']:
        dated[c] = dated.filter(like=c).sum(axis=1)

    return dated

def cleanup(df):
    # Handle dates
    date_fields = ['collectedDate', 'reportedDate', 'createdAt']
    for c in date_fields:
        df[c + "_parsed"] = pd.to_datetime(df[c], errors='coerce')

    print("parsed dates")

    # fill missing dates
    df['collectedDate_parsed'] = df['collectedDate_parsed'].fillna(df['reportedDate_parsed'])
    df['collectedDate_parsed'] = df['collectedDate_parsed'].fillna(df['createdAt_parsed'])
    df['reportedDate_parsed'] = df['reportedDate_parsed'].fillna(df['createdAt_parsed'])

    # Handle funny dates:
    # Everything before 2020 will be filled with the createdAt date
    fix_me = ['collectedDate_parsed', 'reportedDate_parsed']
    for c in fix_me:
        df[c] = df.apply(
        lambda x: x['createdAt_parsed'] if x[c] < datetime(2020, 1, 1) else x[c], axis=1)

    # Strip hours
    for c in fix_me:
        df[c] = df[c].dt.normalize()

    print("fixed dates")

    # Unify test type names
    df.loc[df["testType"].isin(PCR_TESTS),'testType_parsed'] = 'Molecular'
    df.loc[df["testType"].isin(ANTIGEN_TESTS),'testType_parsed'] = 'Antigen'
    df.loc[df["testType"].isin(ANTIBODY_TESTS),'testType_parsed'] = 'Serology'

    # Unify results
    df.loc[df['result'].isin(NEGATIVE_RESULT),'result_parsed'] = 'Negative'
    df.loc[df['result'].isin(POSITIVE_RESULT), 'result_parsed'] = 'Positive'

    # strip the "not tested" ones
    df = df[df['result'] != 'Not Tested']
    #df = df[~df['result'] == 'Not Tested']
    df['result_parsed'] = df['result_parsed'].fillna('Other')

    return df


def main(filename):
    print("Filename = ", filename)

    df = pd.read_json(filename)
    clean_df = cleanup(df)

    # check that we don't have unassigned lines
    fields = ['testType_parsed', 'result_parsed', 'collectedDate_parsed', 'reportedDate_parsed']
    fields_with_nulls = []
    for c in fields:
        if not clean_df[clean_df[c].isna()].empty:
            # so sad
            fields_with_nulls.append(c)

    if fields_with_nulls:
        # so sad
        import pdb
        pdb.set_trace()
        # we don't know what to do here, need to do some guess work


    # by fields
    fields = ['collectedDate_parsed', 'reportedDate_parsed']
    for c in fields:
        dated = aggregate(clean_df, c)
        dated = dated.cumsum()
        dated['date'] = dated.index
        print("Storing " + c)
        dated.to_json(c + ".json", orient='records')


if __name__ == "__main__":
    print(sys.argv)
    filename = None
    if len(sys.argv) > 1:
        filename = sys.argv[1]

    main(filename)
