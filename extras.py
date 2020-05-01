from datetime import datetime
from utils import request_and_parse, extract_attributes, \
   map_attributes, Fields, request_and_parse_multi

''' This file contains extra handling needed for some states
To make it work, the method must be called "handle_{state_abbreviation:lower_case}" 
The parameters are:
- query result
- state mappings
'''

def handle_ct(res, mapping):
    # res is a list of dict, one per day
    if not res:
        return {}
    
    sorted_res = sorted(res, key=lambda x: x['date'], reverse = True)
    latest = sorted_res[0]
    mapped = map_attributes(latest, mapping, 'CT')
    return mapped

def handle_fl(res, mapping):
    '''Need to add the non-FL residents to the totals:
    they separate it for death and hosp"
    '''
    mapped = extract_attributes(res, mapping, 'FL')
    extra_hosp = 0
    extra_death = 0
    try:
        extra_hosp = res['features'][0]['attributes']['SUM_C_HospYes_NonRes']
        extra_death = res['features'][0]['attributes']['SUM_C_NonResDeaths']
    except Exception as ex:
        print("Failed Florida extra processing: ", str(e))
    
    mapped[Fields.HOSP.name] += extra_hosp
    mapped[Fields.DEATH.name] += extra_death

    return mapped

def handle_vt(res, mapping):
    state = 'VT'
    tagged = extract_attributes(res, mapping, state)
    
    # get the hosp_pui and add to HOSP
    try:
        tagged[Fields.CURR_HOSP.name] += res['features'][0]['attributes']['hosp_pui']
    except Exception as e:
        # TODO: handle exceptions 
        print(str(e))
    return tagged
            
def handle_tx(res, mapping):
    '''Texas splits the data between multiple data sources, so a single query is not enough
    This method will do the rest of the queries
    '''
    tagged = extract_attributes(res, mapping, 'TX')
    current_hosp_url = 'https://services1.arcgis.com/d9sLvPecHnb8pMfE/arcgis/rest/services/TSA_BedAvailability_ViewTest/FeatureServer/0/query'
    current_hosp_query = {"where": "1=1", 
                          'outStatistics': '[{"statisticType":"sum","onStatisticField":"Sum_Total_Lab_COVID","outStatisticFieldName":"value"}]',
                         'f': 'json'}
    try:
        res = request_and_parse(current_hosp_url, current_hosp_query)
        value = res['features'][0]['attributes']['value']
    except Exception as ex:
        print(str(ex))
    
    tagged[Fields.CURR_HOSP.name] = value
    return tagged

def handle_pa(res, mapping):
    '''PA has different data sources for positive/death and for hospitalization that's displayd on the dashboard
    The main source has a different number for hosp.
    
    Also need to sub ECMO to Vent number
    '''
    state = 'PA'
    tagged = extract_attributes(res, mapping, state)

    hosp_url = 'https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Adam_County_Summary_V3/FeatureServer/0/query'
    hosp_query = {
        'where': '1=1',
        'outStatistics': [{"statisticType":"sum","onStatisticField":"COVID_19_Patient_Counts_Total_2", "outStatisticFieldName": "total_hosp"}],
        'f': 'json'
    }
    ecmo_val = 0
    hosp_val = None
    try:
        hosp_res = request_and_parse(hosp_url, hosp_query)
        hosp_val = hosp_res['features'][0]['attributes']['total_hosp']
        ecmo_val = res['features'][0]['attributes']['SUM_COVID19ECMO']
    except Exception as ex:
        print(str(ex))
    
    tagged[Fields.CURR_HOSP.name] = hosp_val
    tagged[Fields.CURR_VENT.name] += ecmo_val
    return tagged

def handle_nm(res, mapping):
    data = res['data']
    mapped = map_attributes(data, mapping, 'NM')
    return mapped

def handle_ne(res, mapping):
    tagged = {}
    if 'features' in res and len(res['features']) > 0:
        attributes = res['features']
        for attr in attributes:
            # expecting {attributes: {lab_status: NAME, COUNT_EXPR0: VALUE}}
            name = attr['attributes']['lab_status']
            value = attr['attributes']['COUNT_EXPR0']
            if name in mapping:
                tagged[mapping[name]] = value
    
    return tagged

def handle_in_outdated(res, mapping):
    tagged = {}
    if 'features' in res and len(res['features']) > 0:
        attributes = res['features']
        for attr in attributes:
            # expecting {attributes: {Measure: NAME, Counts: VALUE}}
            name = attr['attributes']['Measure']
            value = attr['attributes']['Counts']
            if name in mapping:
                tagged[mapping[name]] = value
    
    return tagged

def handle_la(res, mapping):
    state_tests = 'SUM_State_Tests'
    state = 'LA'
    tagged = extract_attributes(res, mapping, state)
    try:
        # TODO: extract this into a separate function
        val = res['features'][0]['attributes'][state_tests]
        tagged[Fields.TOTAL.name] += val
    except Exception as e:
        print(str(e))
    return tagged

def handle_il(res, mapping):
    state = 'IL'
    state_name = 'Illinois'
    mapped = {}
    try:        
        for county in res['characteristics_by_county']['values']:
            if county['County'] == state_name:
                mapped = map_attributes(county, mapping, state)

        last_update = res['LastUpdateDate']
        y = last_update['year']
        m = last_update['month']
        d = last_update['day']
        timestamp = datetime(y,m,d).timestamp()
        mapped[Fields.TIMESTAMP.name] = timestamp
    except Exception as e:
        print(str(e))
    return mapped

def handle_de(res, mapping):
    state = 'DE'
    tagged = extract_attributes(res, mapping, state)

    #query_url = arcgis_urls[state]
    query_url = 'https://services1.arcgis.com/PlCPCPzGOwulHUHo/arcgis/rest/services/DEMA_COVID_County_Boundary_Time_VIEW/FeatureServer/0/query'
    query_params = {
        'where': 'NAME=\'Statewide\'',
        'outFields': 'NegativeCOVID,Recovered,Hospitalizations,CriticalCond,Last_Updated',
        'f': 'json'
    }
    tagged_extra = {}
    try:
        res_extra = request_and_parse(query_url, query_params)
        extra_val = res_extra['features'][0]['attributes']
        tagged_extra = map_attributes(extra_val, mapping, state)
    except Exception as ex:
        print(str(ex))
    
    tagged.update(tagged_extra)
    return tagged
