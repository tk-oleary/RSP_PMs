## CONGESTION_METRICS_EDA.PY
# Script calculates measures on an RSP project links, corridor links, and regionwide
# Uses punchlink.csv and extra_links_70029.csv to get info on congestion and EDA usage

print('Starting "congestion_metrics_EDA.py"...')

#IMPORT LIBRARIES
import os, sys
import pandas as pd, numpy as np
import datetime as dt
import csv 

## ------------------------
## INPUTS
## ------------------------
dir = sys.argv[1] + '\\Database'    # -- model run location
rsp_id = sys.argv[2]                # -- run name

# EDA volumes on links
eda_link_vol_file = dir+'\\rsp_evaluation\\results\\extra_links_70029.csv'
# punch file for VMT/VHT calcs
punch = dir + '\\data\\punchlink.csv'


## --------------------
## PARAMETERS
## --------------------

# This script uses the params dictionary to pull parameters from bca_parameters.csv
# The following parameters are pulled from bca_parameters.csv: 
#   - volume/capacity threshold that defines 'congested': params['vc_threshold']
#   - 5-year Interstate K+A rate: params['SAFE_ikarate']
#   - 5-year Non-Interstate K+A Rate: params['SAFE_nikarate']
#   - hwy assignment annualization factor: params['ann_factor']
#
# For more info, see bca_parameters.csv (File contains descriptions of each parameter.)

#bring in parameters file as a dictionary
params={}
with open(os.getcwd()+'\\bca_parameters.csv', 'r') as file:
    csvreader = csv.reader(file)
    next(csvreader) #skip first row of headers
    for row in csvreader:
        params[row[0]] = float(row[1]) #first entry (row[0]) is parameter name, second (row[1]) is value


#z17 zones for 7 counties (maximum zone value)
z17 = 2926


## ----------------
## OUTPUTS 
## ----------------

out_dir = dir + '\\rsp_evaluation\\results'


## ----------------
## EXECUTE 
## ----------------

print('Grabbing punchlink file and performing congestion calculations...')
## -- Read in punch link files -- ##
df = pd.read_csv(punch)
df.rename(columns={'i_node':'inode', 'j_node':'jnode'}, inplace=True)


if 'RSP00' not in rsp_id: ## 'RSP00' is no-build scenario-- other rsp's will incorporate project and corridor results

    clink_csv = dir + '\\rsp_evaluation\\inputs\\geography\\'

    #create project links dataframe
    projlinks = os.path.join(dir+'\\Select_Link\\'+os.listdir(dir+'\\Select_Link')[0])
    projlinks = pd.read_csv(projlinks, skiprows=1, names=['inode','jnode'])
    projlinks['inode'] = projlinks['inode'].str.replace("l=", "").astype(int)
    projlinks['jnode'] = projlinks['jnode'].astype(int)
    projlinks['projlink'] = 1

    # --

    #create corridor links dataframe
    corrlinks = dir+'\\rsp_evaluation\\inputs\\geography\\rsp_corridor_70029.csv'
    corrlinks = pd.read_csv(corrlinks)
    corrlinks = corrlinks[['INODE', 'JNODE']].copy()
    corrlinks.rename(columns={'INODE':'inode','JNODE':'jnode'}, inplace=True)
    corrlinks['corrlink'] = 1


    df = pd.merge(df, projlinks, how='left', on=['inode', 'jnode'])
    df = pd.merge(df, corrlinks, how='left', on=['inode', 'jnode'])


## -- Create necessary variables -- ##
links = df[(df['zone'] > 0) & (df['zone'] <= z17)].copy()                          ##-- limit to 7 counties
##--- volume in VEQs for v/c ratio ---
links.eval('volau = avauv + avh2v + avh3v + avbqv + avlqv + avmqv + avhqv + busveq', inplace=True)
##--- volume in Vehicles ---
links.eval('vehicles = avauv + avh2v + avh3v + avbqv + avlqv + avmqv/2 + avhqv/3 + busveq/3', inplace=True)
links['hTruck'] = links['avhqv']/3


    # ----- Calculate link VMT and VHT ------

## -- Link capacity calculations -- ##
links['hours'] = 2
links.loc[links['timeperiod'] == 1, 'hours'] = 5
links.loc[(links['timeperiod'] == 2) | (links['timeperiod'] == 4), 'hours'] = 1
links.loc[links['timeperiod'] == 5, 'hours'] = 4
links.eval('capacity = lan * emcap * hours', inplace=True)

## -- Arterial Speed Adjustment due to LOS C used in VDF -- ##
links['freeMPH'] = np.where((links['ftime'] > 0), (links['len']/(links['ftime']/60)), 20)  
links['MPH'] = 0
links.loc[links['timau'] > 0, 'MPH'] = links['len'] / (links['timau']/60)
links.loc[links['vdf'] == 1, 'MPH'] = links['freeMPH'] * (1/((np.log(links['freeMPH']) * 0.249) + 0.153 * (links['volau'] / (links['capacity']*0.75))**3.98))
links['congested'] = 0
links.loc[(links['capacity'] > 0) & (links['volau'] / links['capacity'] >= params['vc_threshold']), 'congested'] = 1

## -- Link Performance Metrics -- ##
links.eval('AllVMT = vehicles * len', inplace=True)
links.eval('CongestedVMT = AllVMT * congested', inplace=True)   
links['AllVHT'] = np.where((links['MPH'] > 0), (links['AllVMT']/links['MPH']), 0)  ##-- use adjusted arterial speeds
links.eval('CongestedVHT = AllVHT * congested', inplace=True)
links.eval('HTruckVMT = hTruck * len', inplace=True)
links.eval('CongestedHTruckVMT = HTruckVMT * congested', inplace=True)
links['HTruckVHT'] = np.where((links['MPH'] > 0), links['HTruckVMT']/links['MPH'], 0)  ##-- use adjusted arterial speeds
links.eval('CongestedHTruckVHT = HTruckVHT * congested', inplace=True)

## -- Link PMs K+A -- ##
#non-interstate rate
links['annual_ka'] = links['AllVMT'] * params['ann_factor'] * params['SAFE_nikarate'] / 100000000
#interstate rate
links.loc[links['vdf'].isin([2,3,4,5,8]), 'annual_ka'] = links['AllVMT'] * params['ann_factor'] * params['SAFE_ikarate'] / 100000000

# lane miles
links['lanemi'] = links['lan'] * links['len']

## commented out for now-- don't know how to generate this

## ----------- Calculate EDA VMT -----------------
#given EDA link volume csv has been generated
eda_link_vol = pd.read_csv(eda_link_vol_file)

#cleanup column names
cols = eda_link_vol.columns.tolist()
colmap = {}
for c in cols:
    d = c.replace(' ', '')
    d = d.replace('@', '')
    colmap[c] = d
eda_link_vol.rename(columns=colmap, inplace=True)
#merge values to links
links = pd.merge(links, eda_link_vol, how='left', on=['inode', 'jnode'])
#calculate eda vmt
links['edavmt'] = links['ejvol'] * links['len']


## --------- Summarize By Project, Corridor, Region, Time of Day -----------------
if 'RSP00' not in rsp_id:
    print('Calculating project-specific and corridor-specific congestion metrics...')
    # project link measures
    projlinks = links.loc[links['projlink']==1]
    projlinks_result = projlinks.groupby('timeperiod').agg({'AllVMT':'sum', 'CongestedVMT':'sum',
                                                    'AllVHT':'sum', 'CongestedVHT':'sum',
                                                    'HTruckVMT':'sum', 'CongestedHTruckVMT':'sum',
                                                    'HTruckVHT':'sum', 'CongestedHTruckVHT':'sum',
                                                    'annual_ka':'sum', 'len':'sum', 'lanemi':'sum'
                                                    })
    projlinks_result.reset_index(inplace=True)
        #make summary column
    tot = projlinks_result.agg({'AllVMT':'sum', 'CongestedVMT':'sum',
                            'AllVHT':'sum', 'CongestedVHT':'sum',
                            'HTruckVMT':'sum', 'CongestedHTruckVMT':'sum',
                            'HTruckVHT':'sum', 'CongestedHTruckVHT':'sum',
                            'annual_ka':'sum', 'len':'average', 'lanemi':'average'})
    tot['timeperiod'] = 'Total'
    projlinks_result = pd.concat([projlinks_result, tot.to_frame().T], ignore_index=True, sort=True)
    edavmt = projlinks.loc[projlinks['timeperiod']==1, 'edavmt'].sum()      #timeperiod==1 b/c edavmt is a daily value
    projlinks_result.loc[projlinks_result['timeperiod']=='Total', 'edavmt'] = edavmt
    projlinks_result['analysis_level'] = 'project'

    #--

    # corridor link measures
    clinks = links.loc[links['corrlink']==1]
    clinks_result = clinks.groupby('timeperiod').agg({'AllVMT':'sum', 'CongestedVMT':'sum',
                                                    'AllVHT':'sum', 'CongestedVHT':'sum',
                                                    'HTruckVMT':'sum', 'CongestedHTruckVMT':'sum',
                                                    'HTruckVHT':'sum', 'CongestedHTruckVHT':'sum',
                                                    'annual_ka':'sum', 'len':'sum', 'lanemi':'sum',
                                                    })
    clinks_result.reset_index(inplace=True)
        #make summary column
    tot = clinks_result.agg({'AllVMT':'sum', 'CongestedVMT':'sum',
                            'AllVHT':'sum', 'CongestedVHT':'sum',
                            'HTruckVMT':'sum', 'CongestedHTruckVMT':'sum',
                            'HTruckVHT':'sum', 'CongestedHTruckVHT':'sum',
                            'annual_ka':'sum', 'len':'average', 'lanemi':'average'})
    tot['timeperiod'] = 'Total'
    clinks_result = pd.concat([clinks_result, tot.to_frame().T], ignore_index=True, sort=True)
    edavmt = clinks.loc[clinks['timeperiod']==1, 'edavmt'].sum()      #timeperiod==1 b/c edavmt is a daily value
    clinks_result.loc[clinks_result['timeperiod']=='Total', 'edavmt'] = edavmt
    clinks_result['analysis_level'] = 'corridor'

#--
print('Summarizing data...')
#7-county measures
region_result = links.groupby('timeperiod').agg({'AllVMT':'sum', 'CongestedVMT':'sum',
                                                 'AllVHT':'sum', 'CongestedVHT':'sum',
                                                 'HTruckVMT':'sum', 'CongestedHTruckVMT':'sum',
                                                 'HTruckVHT':'sum', 'CongestedHTruckVHT':'sum',
                                                 'annual_ka':'sum', 'len':'sum', 'lanemi':'sum',
                                                 })
region_result.reset_index(inplace=True)
    #make summary column
tot = region_result.agg({'AllVMT':'sum', 'CongestedVMT':'sum',
                         'AllVHT':'sum', 'CongestedVHT':'sum',
                         'HTruckVMT':'sum', 'CongestedHTruckVMT':'sum',
                         'HTruckVHT':'sum', 'CongestedHTruckVHT':'sum',
                         'annual_ka':'sum', 'len':'average', 'lanemi':'average'})
tot['timeperiod'] = 'Total'
region_result = pd.concat([region_result, tot.to_frame().T], ignore_index=True, sort=True)
edavmt = links.loc[links['timeperiod']==1, 'edavmt'].sum()      #timeperiod==1 b/c edavmt is a daily value
region_result.loc[region_result['timeperiod']=='Total', 'edavmt'] = edavmt
region_result['analysis_level'] = '7-county region'


## ------ Create Final Table, and Export -------

if 'RSP00' not in rsp_id:
    final = pd.concat([projlinks_result, clinks_result, region_result], ignore_index=True, sort=True)
    final.loc[final['timeperiod']=='Total', 'edavmtshare'] = final['edavmt'] / final['AllVMT']
    columnlist = final.columns.tolist()
    reorderedcolumns = columnlist[:-2]+columnlist[-1:]+columnlist[-2:-1]
    final = final.reindex(columns=reorderedcolumns)
    # today = dt.date.today().strftime('%Y-%m-%d')

if 'RSP00' in rsp_id:
    final = region_result

print('Exporting...')
final.to_csv(out_dir+f'\\RSP_congestion_factors.csv', index=False)
links.to_csv(out_dir+f'\\RSP_congestion_factors_links.csv', index=False)
print(f'Done! Exported files to {out_dir}, named "congestion_factors.csv" and "congestion_factors_links.csv"!')