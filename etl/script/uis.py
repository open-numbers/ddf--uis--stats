# -*- coding: utf-8 -*-

"""
script to extract DDF CSV from SDMX format data from UIS.

as of 2016-07, the SDMX file from UIS follows SDMX 2.0 standard, which
is not the latest standard and we don't have a python library for this
version. However if later on UIS adapt to 2.1 or newer version, we should
try this python library: https://github.com/dr-leo/pandaSDMX

in this script I will parse the XML with lxml and xmltodict.
"""

import pandas as pd
import numpy as np
import os
import xmltodict
from lxml import etree
import time

from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file

# configuration of file paths
dsd_file = '../source/education_dsd.xml'
data_file = '../source/education.xml'
out_dir = '../../'


def _read_dsd(dsd_file):
    """parse dsd_file using xmltodict, result will be json-like dict."""
    with open(dsd_file) as f:
        dsd = xmltodict.parse(f.read())

    return dsd


def _read_data(data_file):
    with open(data_file) as f:
        data = etree.parse(f)

    root = data.getroot()

    # get all namespaces from the xml
    nsmap = root.nsmap.copy()
    # change None to a meaningful name, so that I can use later.
    nsmap['xmlns'] = nsmap.pop(None)

    all_data = {}

    all_ser = root.xpath('.//xmlns:Series', namespaces=nsmap)

    for item in all_ser:
        # each series tag contains a time series for a given indicator
        # and country. here we loop though all series and group them into the
        # all_data dict, where keys are indicators and values are a dict of
        # {location: series} for that indicator.
        item_dict = xmltodict.parse(etree.tostring(item))

        attrs = {}
        ser = []

        # getting series attributes: location and indicator id
        for i in item_dict['Series']['SeriesKey']['Value']:
            if i['@concept'] == 'EDULIT_IND':
                attrs['key'] = to_concept_id(i['@value'])
            if i['@concept'] == 'LOCATION':
                attrs['location'] = to_concept_id(i['@value'])

        # get observation data.
        obs = item_dict['Series']['Obs']
        if isinstance(obs, list):
            for o in item_dict['Series']['Obs']:
                ser.append([o['Time'], o['ObsValue']['@value']])
        else:
            ser.append([obs['Time'], obs['ObsValue']['@value']])

        if attrs['key'] not in all_data.keys():
            all_data[attrs['key']] = {attrs['location']: ser}
        else:
            # there should be no duplicates in locations
            assert attrs['location'] not in all_data[attrs['key']].keys()
            all_data[attrs['key']][attrs['location']] = ser

    # concat the list of series for each indicator
    for k, v in all_data.items():
        to_concat = []
        for loc, ser in v.items():
            df = pd.DataFrame(ser, columns=['time', k])
            df['location'] = loc
            to_concat.append(df)

        all_data[k] = pd.concat(to_concat, ignore_index=True)

    return all_data


def extract_concepts_continuous(data, dsd):
    indicators = dsd['message:Structure']['message:CodeLists']['CodeList'][0]

    indi_info_list = []

    for i in indicators['Code']:
        indi_info = []
        indi_info.append(i['@value'])

        if '@parentCode' in i.keys():
            # parent code, no use for now but I just keep it.
            indi_info.append(i['@parentCode'])
        else:
            indi_info.append(np.nan)

        # description
        if isinstance(i['Description'], dict):
            indi_info.append(i['Description']['#text'])
        else:
            indi_info.append(i['Description'][0]['#text'])

        indi_info_list.append(indi_info)

    # construct concept dataframe
    conc = pd.DataFrame(indi_info_list, columns=['concept', 'drillup', 'name'])
    conc['concept'] = conc['concept'].map(to_concept_id)
    conc['drillup'] = conc['drillup'].map(to_concept_id)
    conc['concept_type'] = 'measure'

    # only keep those concepts in data file.
    conc = conc.set_index('concept')
    conc = conc.loc[data.keys()]

    # drop the 'drillup' column for now.
    conc = conc.sort_index()[['name', 'concept_type']]

    return conc.reset_index()


def extract_concepts_discrete():
    """manually create discrete concept file."""
    disc = pd.DataFrame([['name', 'Name', 'string'],
                         ['time', 'Year', 'time'],
                         ['location', 'Location', 'entity_domain']
                        ], columns=['concept', 'name', 'concept_type'])

    return disc


def extract_entities_location(dsd):
    locs = dsd['message:Structure']['message:CodeLists']['CodeList'][1]

    loc_list = []

    for c in locs['Code']:
        cinfo = [c['@value'], c['Description'][0]['#text']]

        loc_list.append(cinfo)

    loc_df = pd.DataFrame(loc_list, columns=['location', 'name'])
    loc_df['location'] = loc_df['location'].map(to_concept_id)

    return loc_df


def extract_datapoints(data):
    for k, df in data.items():
        df = df.dropna(how='any')
        df = df[df[k] != 'NaN']  # remove nans

        df = df[['location', 'time', k]].sort_values(by=['location', 'time'])

        yield k, df


if __name__ == '__main__':
    print('reading source files...')
    dsd = _read_dsd(dsd_file)
    data = _read_data(data_file)

    print('creating concept files...')
    conc = extract_concepts_continuous(data, dsd)
    path = os.path.join(out_dir, 'ddf--concepts--continuous.csv')
    conc.to_csv(path, index=False)

    disc = extract_concepts_discrete()
    path = os.path.join(out_dir, 'ddf--concepts--discrete.csv')
    disc.to_csv(path, index=False)

    print('creating entities files...')
    location = extract_entities_location(dsd)
    path = os.path.join(out_dir, 'ddf--entities--location.csv')
    location.to_csv(path, index=False)

    print('creating datapoint files...')
    for k, df in extract_datapoints(data):
        path = os.path.join(out_dir, 'ddf--datapoints--{}--by--location--time.csv'.format(k))

        df.to_csv(path, index=False)

    print('creating index file...')
    create_index_file(out_dir)

    print('Done.')
