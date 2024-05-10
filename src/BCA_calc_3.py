##############################################################################
# BCA Calculations - Emme Punch, PMs, and BCA Calcs
##############################################################################
# Tim O'Leary - 2023/08/28
# ------------------------------
# This script converts trip distribution tables (parquet files) into pandas dataframes and analyzes trips by purpose, mode, and time of day.
#
# NOTE: NEEDS TO USE PYTHON ENVIRONMENT INSTALLED WITH EMME (TO PULL NETWORK DATA USING MODELLER API)
#
# ---
#
# There is a series of Emme macros that perform benefit-cost calculations that we are attempting to translate
# to python for the Transportation Project Analysis Tool and for various grant applications that require a 
# benefit-cost analysis. 
#
# This script translates those macros into Python scripts, occasionally borrowing lines from other scripts
# like 'highway_project_metrics.py' and 'rsp_emissions.py' since the macros were made with the previous trip-based model version.
#
# This Jupyter notebook acts as a temporary workspace, and eventually will be copy-pasted 
# into a '.py' file to run in-bulk for several modeling scenarios.
##############################################################################

#import packages
print('BEGIN HIGHWAY/TRANSIT LINK METRICS.')
import pandas as pd, numpy as np
import os 
import sys
import csv
# import fnmatch
########################################
## --- INPUT FILES AND PARAMETERS --- ##
########################################+



#PARAMETERS CSV -- most parameters are called from here. open for descriptions
parameters_dir = sys.argv[1]+'\\bca_parameters.csv'

#current working directory
cwd = sys.argv[2]   #cmap_trip-based_model folder

#scenario number
scen=700
scen_year=2050      #for present/future value calcs
curr_year = 2023    #for present/future value calcs

#rsp number -- update to parameter, or list for iteration
rsp_id = sys.argv[3]    #e.g., 'RSP35'

#bring in parameters file
parameters={}
with open(parameters_dir, 'r') as file:
    csvreader = csv.reader(file)
    next(csvreader) #skip first row of headers
    for row in csvreader:
        parameters[row[0]] = float(row[1]) #first entry (row[0]) is parameter name, second (row[1]) is value

#add present value
parameters['pv_deprec_rate'] = 1 / (1+parameters['discount_rate'])**(scen_year - curr_year)

#z17 zones for 7 counties (maximum zone value)
z17 = 2926

#RSP feature class -- change to parameter
rsp_shp = r'S:\AdminGroups\PlanDevelopment\Capital projects\Project_info\GIS\Projects\RSP_November2016.shp'

#MHN feature class -- change to parameter
mhn_fc = r'V:\Modeling\Networks\mhn_c21q2.gdb\hwynet_arc'

#EDA volumes on links -- change to parameter
eda_link_vol_file = r'C:\Users\toleary\OneDrive - Chicago Metropolitan Agency for Planning\Desktop\extra_links_70029.csv'

# PROJECT LINKS 
#project links
plinks_txt = cwd + '\\Database\\Select_Link\\{}_proj_links.txt'.format(rsp_id)

#parameters not currently used
#clinks_txt = cwd + '\\Database\\rsp_evaluation\\inputs\\rsp{0}_{1}.txt'.format(rsp_id, distance.lower().replace(' ', ''))

print('  -- Initialize Emme and set things up...')
## INITIALIZE EMME
#point to emme project
empfile = [os.path.join(run, file) for file in os.listdir(cwd) if file.endswith('.emp')][0]


#initialize emme desktop
import inro.emme.desktop.app as _app
desktop = _app.start_dedicated(
    visible=True,
    user_initials="cmap",
    project=empfile
)

import inro.modeller as _m
modeller = _m.Modeller(desktop=desktop)
emmebank = modeller.emmebank

###################################
## ------- OUTPUT FILES -------- ##
###################################

#punch moves out
punchmoves_out = cwd+'\\Database\\rsp_evaluation\\results\\punchmoves_out.csv'
#summary highway table out
hwysummary_out = cwd+'\\Database\\rsp_evaluation\\results\\hwysummary_out.csv'

#punch transit out
punchtransit_out = cwd+'\\Database\\rsp_evaluation\\results\\punchtransit_out.csv'
#summary transit table out
trntsummary_out = cwd+'\\Database\\rsp_evaluation\\results\\trntsummary_out.csv'

#bca summary out
bcasummary_out = cwd+'\\Database\\rsp_evaluation\\results\\bcasummary_out.csv'

#define all the emme tools, will be called later in script

net_calc = modeller.tool('inro.emme.network_calculation.network_calculator')

print('  -- Done.')


################################
## -- import roadway links -- ##
################################

print('EXTRACT ROADWAY LINK ATTRIBUTES')

# df_list = []

# #list of attributes to extract
# desired_links = '''\
#     "length+lanes+vdf+\
#     @zone+@emcap+timau+\
#     @ftime+@avauv+@avh2v+\
#     @avh3v+@avbqv+@avlqv+\
#     @avmqv+@avhqv+@busveq+\
#     @atype+@imarea+\
#     @speed+@m200+@h200+\
#     @slcl1+@slcl2+@slcl3+\
#     @slcl4+@slcl5+@slcl6+\
#     @slcl7+@slvol+@ejcl1+\
#     @ejcl2+@ejcl3+@ejcl4+\
#     @ejvol+@avtot+@pvht"\
# '''


# #iterate through each time period (1-8)
# for tp in range(1,9):
#     print(f'  -- Obtaining link data for time period {tp}...')

#     spec_linkdata = f'''
#     {{
#         "expression": {desired_links},
#         "selections": {{"link":"all"}},
#         "type": "NETWORK_CALCULATION"
#     }}
#     '''
#     #network calculation to export attributes
#     linkdata_tp = net_calc(specification=spec_linkdata, scenario=emmebank.scenario(tp), full_report=True)
    
#     header = linkdata_tp['table'][0]
#     data = linkdata_tp['table'][1:]

#     linkdata_tp_df = pd.DataFrame(data=data, columns=header)
#     linkdata_tp_df['timeperiod'] = tp
    
#     df_list.append(linkdata_tp_df)

# linkdata = pd.concat(df_list, ignore_index=True)

linkdata = pd.read_csv(cwd+'\\Database\\data\\punchlink.csv')

################################
## -- import transit links -- ##
################################

print('EXTRACT TRANSIT LINK ATTRIBUTES.')
df_list = []

desired_attributes = '''\
    "length+hdw+voltr+\
    @tot_capacity+@seated_capacity+\
    us1+@tot_vcr+@seated_vcr+@zone"\
'''

#transit time periods (x21, x23, x25, x27, where x=1st digit of scenario year)
t = str(scen)[0]
trnt_scen = [int(t+'21'), int(t+'23'), int(t+'25'), int(t+'27')]
timeperiods = trnt_scen
#iterate through each time period
for tp in timeperiods:
    print('  -- Obtaining transit link data for scenario {}...'.format(tp))

    spec_trlinkbus = f'''{{
        "expression": {desired_attributes},
        "aggregation": null,
        "selections": {{
            "link": "all",
            "transit_line": "mode=B or mode=E or mode=P or mode=Q or mode=L"
        }},
        "type": "NETWORK_CALCULATION"
    }}'''

    spec_trlinkrail = f'''{{
        "expression": {desired_attributes},
        "aggregation": null,
        "selections": {{
            "link": "all",
            "transit_line": "mode=C or mode=M"
        }},
        "type": "NETWORK_CALCULATION"
    }}'''

    tr_punchbus = net_calc(specification=spec_trlinkbus, scenario=emmebank.scenario(tp), full_report=True)
    tr_punchrail = net_calc(specification=spec_trlinkrail, scenario=emmebank.scenario(tp), full_report=True)
    
    #punchbus to dataframe
    header = tr_punchbus['table'][0]
    data = tr_punchbus['table'][1:]
    tr_punchbus_df = pd.DataFrame(data=data, columns=header)
    tr_punchbus_df['timeperiod'] = tp
    tr_punchbus_df['mode'] = 'bus'

    #punchrail to dataframe
    header = tr_punchrail['table'][0]
    data = tr_punchrail['table'][1:]
    tr_punchrail_df = pd.DataFrame(data=data, columns=header)
    tr_punchrail_df['timeperiod'] = tp
    tr_punchrail_df['mode'] = 'rail'

    #append dataframes to list
    df_list.append(tr_punchbus_df)
    df_list.append(tr_punchrail_df)

#concatenate all the dataframes together
trlinkdata = pd.concat(df_list, ignore_index=True)

print('  -- Done.')




####################################
## ---- TRANSIT LINK METRICS ---- ##
####################################

print('ANALYZE TRANSIT DATA.')

#clean up column names
trcols = trlinkdata.columns.tolist()
cols_to_rename = [a for a in trcols if a.startswith('@')]
coldict = dict([[a, a[1:]] for a in cols_to_rename])
trlinkdata.rename(columns=coldict, inplace=True)

#defines scenario numbers, number of hours, and description -- will be used in calcs later
timeperiod_hours = {
    721: {'hours':12, 'desc': 'Night (6pm-6am)'},
    723: {'hours': 3, 'desc': 'AM (6am-9am)'},
    725: {'hours': 7, 'desc': 'Midday (9am-4pm)'},
    727: {'hours': 2, 'desc': 'PM (4pm-6pm)'}
}


#pmt, pht, vmt, vht -- calculated on all transit segments, regardless of mode
metrics = {
    'trnt_pmt': 'voltr * len',                  ## - pmt: passenger miles traveled
    'trnt_pht': 'voltr * us1 / 60',          ## - pht: passenger hours traveled
    'trnt_vmt_1hr': 'len * 60 / hdwy',          ## - vmt: vehicle (bus or rail) miles traveled
    'trnt_vht_1hr': 'us1 / 60 * 60 / hdwy'   ## - vht: vehicle (bus or rail) hours traveled
}


for m in metrics:
    print(f'  -- Calculating metric {m} for bus and rail...')
    trlinkdata.eval(f'{m} = {metrics[m]}', inplace=True)                    ## - does calculations in metrics dictionary
    trlinkdata.loc[trlinkdata['mode']=='bus', f'bus_{m}'] = trlinkdata[m]   ## - separate column pulling out values that are 'bus' mode: b,e,p,q, or l (cta bus or pace)
    trlinkdata.loc[trlinkdata['mode']=='rail', f'rail_{m}'] = trlinkdata[m] ## - separate column pulling out values that are 'rail' mode: m or c (metra or L)


## --
## -- OPERATING COST
## --

print('  -- Calculating operating costs... ')
#oper costs = (vmt per hour) * (# hours) * (op cost per vmt) * (annualization factor)
#by bus, then rail
for tp in trlinkdata['timeperiod'].unique().tolist():
    segment = trlinkdata.loc[trlinkdata['timeperiod']==tp]
    #bus costs:
    segment.eval(f"bus_opcost = bus_trnt_vmt_1hr * {timeperiod_hours[tp]['hours']} * {parameters['OC_bus']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
    #rail costs:
    segment.eval(f"rail_opcost = rail_trnt_vmt_1hr * {timeperiod_hours[tp]['hours']} * {parameters['OC_rail']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace = True)
    #add back to original dataframe:
    trlinkdata.loc[segment.index, 'bus_opcost'] = segment['bus_opcost']
    trlinkdata.loc[segment.index, 'rail_opcost'] = segment['rail_opcost']
#remove null values
trlinkdata.loc[trlinkdata['bus_opcost'].isnull(), 'bus_opcost'] = 0
trlinkdata.loc[trlinkdata['rail_opcost'].isnull(), 'rail_opcost'] = 0
#total operating cost (bus+rail)    
trlinkdata.eval('total_trnt_op_cost = bus_opcost + rail_opcost', inplace=True)


## --
## -- TOTAL EMISSIONS COST (borrowed from rsp_emissions.py)
## --


# needs moves outputs. may not be completed here
trlinkdata['total_trnt_emissions_cost'] = 0

## --
## -- NOISE -- 
## --

print('  -- Calculating noise costs... ')
# nothing yet. bus is included in roadway measures
# rail I think intentionally excluded from BCA guidance (noise benefit encouraged to be used in mode shift applications)
trlinkdata['total_trnt_noise_cost'] = 0


## -- 
## -- VALUE OF TIME --
## --

print('  -- Calculating value of time costs... ')
## NOTE -- right now we don't have pvt_vehicles separated by work and non-work-- need partial demand transit assignment for that
## for now, i'm just calculating a percentage (based on parquet files)-- work trips are 57% of total trips and PHT, non-work is 43% of total trips PHT

#bus in-vehicle time costs
trlinkdata.eval(f"bus_vot = (0.57*{parameters['VOT_inv_work']} + 0.43*{parameters['VOT_inv_nw']}) * bus_trnt_pht * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
trlinkdata.loc[trlinkdata['bus_vot'].isnull(), 'bus_vot'] = 0   #-- remove nulls

#rail in-vehicle time costs
trlinkdata.eval(f"rail_vot = (0.57*{parameters['VOT_inv_work']} + 0.43*{parameters['VOT_inv_nw']}) * rail_trnt_pht * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
trlinkdata.loc[trlinkdata['rail_vot'].isnull(), 'rail_ivt_cost'] = 0    #-- remove nulls

#total in-vehicle time costs
trlinkdata.eval('total_trnt_vot = bus_vot + rail_vot', inplace=True)

## -- 
## -- EXPORT DATA -- 
## --

# ## -- Export non-aggregated table --
# print('  -- Exporting non-aggregated table for QA/QC...')
# trlinkdata.to_csv(punchtransit_out)
# print(f'  -- Exported successfully. Located at {punchtransit_out}')


## -- Create summary table --
print('  -- Creating and exporting summary table...')

modes = ['bus_', 'rail_']
metricscolumns = [a+b for a in modes for b in metrics]

metrics_agg = dict([[a, 'sum'] for a in metricscolumns])
trnt_summary = trlinkdata.groupby('timeperiod').agg(metrics_agg)

trnt_summary.to_csv(trntsummary_out)
print(f'  -- Exported successfully. Located at {trntsummary_out}')


## -- Create BCA table --
print('  -- Creating transit BCA table...')
bcasummary_trnt = trlinkdata[['total_trnt_vot', 'total_trnt_op_cost', 'total_trnt_noise_cost', 'total_trnt_emissions_cost']].sum()
bcasummary_trnt['geog'] = 'region'
print('  -- Success. Will be merged with roadway BCA later.')
print('Done.')


########################################
### -- BEGIN ROADWAY LINK METRICS -- ###
########################################

print('ANALYZE ROADWAY DATA.')

## ------ CLEAN DATASET ------ ##

# -- rename columns, removing @ character
collist = linkdata.columns.tolist()
cols_to_rename = [a for a in collist if a.startswith('@')]
coldict = dict([[a, a[1:]] for a in cols_to_rename])

linkdata.rename(columns=coldict, inplace=True)

# -- drop unnecessary columns
#linkdata.drop(labels='result', axis=1, inplace=True)
# -- drop unnecessary rows (limit dataset to links within 7 counties)
linkdata = linkdata[(linkdata['zone'] > 0) & (linkdata['zone'] <= z17)].copy()

# -- most columns come through as float64
# -- changing columns that should be integers
for x in ['i_node', 'j_node', 'timeperiod', 'lan', 'vdf', 'zone', 'imarea', 'atype']:
    linkdata[x] = linkdata[x].astype(int)


## ----- ADD PROJECT LINKS TO DATAFRAME ----- ##
## -- create project links dataframe -- ## 
projlinks = pd.read_csv(plinks_txt, skiprows=1, names=['inode','jnode'])
projlinks['inode'] = projlinks['inode'].str.replace("l=", "").astype(int)
projlinks['jnode'] = projlinks['jnode'].astype(int)
projlinks.rename(columns={'inode':'i_node','jnode':'j_node'}, inplace=True)
projlinks['projlink'] = 1

## merge project links to dataframe
links = pd.merge(linkdata, projlinks, how='left', on=['i_node', 'j_node'])
links.loc[~(links['projlink']==1), 'projlink'] = 0
## -- PERFORMANCE MEASURES CALCULATIONS -- ##

## -- Volumes and volume equivalents -- ##

print('  -- Performing volume calculations...')


##--- total volume in vehicle equivalents (for v/c ratio)---
links.eval('volau = avauv + avh2v + avh3v + avbqv + avlqv + avmqv + avhqv + busveq', inplace=True)


# -- create short-haul truck volume equivalents
#subtract long-haul from total mtruck and htruck volume equivalents
links['m200'] = links[['m200', 'avmqv']].min(axis=1)
links['avmqv'] = np.maximum(links['avmqv'] - links.m200, 0)
links['h200'] = links[['h200', 'avhqv']].min(axis=1)
links['avhqv'] = np.maximum(links['avhqv'] - links.h200, 0)


##--- volume in # vehicles (for VMT/VHT) ---
links.eval('vehicles = avauv + avh2v + avh3v + avbqv + avlqv + avmqv/2 + avhqv/3 + busveq/3', inplace=True)
links['sov'] = links['avauv']
links['hov2'] = links['avh2v']
links['hov3'] = links['avh3v']
links.eval('pvt_vehicles = sov + hov2 + hov3', inplace=True)
links['bplate'] = links['avbqv']
links['ltruck'] = links['avlqv']
links['mtruck'] = links['avmqv']/2
links['htruck'] = links['avhqv']/3
links['mtrucklh'] = links['m200']/2
links['htrucklh'] = links['h200']/3
links['bus'] = links['busveq']/3

## -- Link capacity calculations -- ##

print('  -- Performing capacity calculations...')

## -- Hours per time period (to divide volumes into volumes per hour)
links['hours'] = 2
links.loc[links['timeperiod'] == 1, 'hours'] = 5
links.loc[(links['timeperiod'] == 2) | (links['timeperiod'] == 4), 'hours'] = 1
links.loc[links['timeperiod'] == 5, 'hours'] = 4
links.eval('capacity = lan * emcap * hours', inplace=True)


## -- Arterial speed adjustment due to LOS C used in VDF (for VHT calculations)
links['fmph'] = np.where((links['ftime'] > 0), (links['len']/(links['ftime']/60)), 20)  
links['mph'] = 0
links.loc[links['timau'] > 0, 'mph'] = links['len'] / (links['timau']/60)
links.loc[links['vdf'] == 1, 'mph'] = links['fmph'] * (1/((np.log(links['fmph']) * 0.249) + 0.153 * (links['volau'] / (links['capacity']*0.75))**3.98))
links['congested'] = 0
links.loc[(links['capacity'] > 0) & (links['volau'] / links['capacity'] >= parameters['vc_threshold']), 'congested'] = 1


## -- Lane miles
links.eval('lanemi = lan * len', inplace=True)

## --
## -- Vehicle miles/hrs traveled, person miles/hrs traveled, congested vmt/vht/pmt/pht 
## --

##vehicle types to calculate vmt/cvmt, vht/cvht
#auto categories
auto_cols = [
    'sov', 'hov2', 
    'hov3', 'pvt_vehicles'
]

#freight categories
freight_cols = [
    'bplate', 'ltruck', 
    'mtruck', 'htruck', 
    'bus', 'mtrucklh', 'htrucklh'
]

# #select link class categories
# sl_cols = [
#     'slcl1', 'slcl2', 'slcl3', 
#     'slcl4', 'slcl5', 'slcl6', 
#     'slcl7', 'slvol', 'ejcl1', 
#     'ejcl2', 'ejcl3', 'ejcl4', 
#     'ejvol'
# ]

## -- VMT, Congested VMT calculations
print('  -- Calculating VMT/congested VMT, VHT/congested VHT...')
#auto vmt/cvmt
for c in auto_cols:
    links.eval(f'{c}_vmt = {c} * len', inplace=True)
    links.eval(f'{c}_cvmt = {c}_vmt * congested', inplace=True)
#freight vmt/cvmt
for c in freight_cols: 
    links.eval(f'{c}_vmt = {c} * len', inplace=True)
    links.eval(f'{c}_cvmt = {c}_vmt * congested', inplace=True)
# #select link vmt/cvmt
# for c in sl_cols:   
#     links.eval(f'{c}_vmt = {c} * len', inplace=True)
#     links.eval(f'{c}_cvmt = {c}_vmt * congested', inplace=True)
#all vmt
links.eval('all_vmt = vehicles * len', inplace=True)

## -- VHT, Congested VHT calculations
#auto vht/cvht
for c in auto_cols:
    links[f'{c}_vht'] = np.where((links['mph'] > 0), links[f'{c}_vmt']/links['mph'], 0) ##-- use adjusted arterial speeds
    links.eval(f'{c}_cvht = {c}_vht * congested', inplace=True)
#freight vht/cvht
for c in freight_cols:
    links[f'{c}_vht'] = np.where((links['mph'] > 0), links[f'{c}_vmt']/links['mph'], 0) ##-- use adjusted arterial speeds
    links.eval(f'{c}_cvht = {c}_vht * congested', inplace=True) 
# # select link vht/cvht
# for c in sl_cols:
#     links[f'{c}_vht'] = np.where((links['mph'] > 0), links[f'{c}_vmt']/links['mph'], 0) ##-- use adjusted arterial speeds
#     links.eval(f'{c}_cvht = {c}_vht * congested', inplace=True)
#all vht
links['all_vht'] = np.where((links['mph'] > 0), links['all_vmt']/links['mph'], 0)


## -- Person-miles and person-hours traveled calculations
print('  -- Calculating PMT/congested PMT, PHT/congested PHT...')
#occupancy info, for hov calculations
occupancy = {
    'sov': 1, 
    'hov2': 2, 
    'hov3': parameters['occupancy_hov3'],
    'pvt_vehicles': 0 #dummy number. will calculate pvt_vehicles as sum of sov, hov2, and hov3 later
}
#PMT, Congested PMT calculations (for auto only)
for c in auto_cols:
    links.eval(f'{c}_pmt = {c} * {occupancy[c]} * len', inplace=True)  # 'occupancy' is dict type that determines # people per vehicle
    links.eval(f'{c}_cpmt = {c}_pmt * congested', inplace=True)
links.eval('pvt_vehicles_pmt = sov_pmt + hov2_pmt + hov3_pmt', inplace=True)        # pvt_veh = sum(sov, hov)
links.eval('pvt_vehicles_cpmt = sov_cpmt + hov2_cpmt + hov3_cpmt', inplace=True)    # pvt_veh = sum(sov, hov)
#PHT, Congested PHT (for auto only)
for c in auto_cols:
    links[f'{c}_pht'] = np.where((links['mph'] > 0), links[f'{c}_pmt']/links['mph'], 0)  ##-- use adjusted arterial speeds
    links.eval(f'{c}_cpht = {c}_pht * congested', inplace=True)
links.eval('pvt_vehicles_pht = sov_pht + hov2_pht + hov3_pht', inplace=True)        #pvt_veh = sum(sov, hov)
links.eval('pvt_vehicles_cpht = sov_cpht + hov2_cpht + hov3_cpht', inplace=True)    #pvt_veh = sum(sov, hov)


## --
## -- SAFETY PERFORMANCE MEASURES: DEATH/SERIOUS INJURY AND PROPERTY DAMAGE-- ##
## --

print('  -- Calculating safety/crashes calculations... ')
## K+A -- calculations are per 100,000,000 VMT
#non-interstate rate
links['annual_ka'] = links['all_vmt'] / 100000000 * parameters['ann_factor'] * parameters['SAFE_nikarate']
links['annual_crash'] = links['all_vmt'] / 100000000 * parameters['ann_factor'] * parameters['SAFE_nicrashrate']
#interstate rate
links.loc[links['vdf'].isin([2,3,4,5,8]), 'annual_ka'] = links['all_vmt'] / 100000000 * parameters['ann_factor'] * parameters['SAFE_ikarate']
links.loc[links['vdf'].isin([2,3,4,5,8]), 'annual_crash'] = links['all_vmt'] / 100000000 * parameters['ann_factor'] * parameters['SAFE_nicrashrate']
#dollar amount
links['annual_ka_dollar'] = links['annual_ka'] * parameters['SAFE_ka'] * parameters['pv_deprec_rate']
links['annual_crash_dollar'] = links['annual_crash'] * parameters['SAFE_pdo'] * parameters['pv_deprec_rate']
links.eval('total_r_safety_cost = annual_ka_dollar + annual_crash_dollar', inplace=True)


## --
## -- RELIABILITY COSTS -- ##
## --
print('  -- Calculating reliability costs... ')
#nothing yet -- reviewing methodology. dummy value for now
links['total_r_reliability_cost'] = 0


## --
## -- EMISSIONS COSTS -- ##
## -- borrowed (and lightly edited) from rsp_emissions.py
## --

print('  -- Calculating emissions costs... ')
#emissions values are not link-based, will be added to bca_summary.csv instead of links df

# change years if necessary!
ghgrates = pd.read_csv(r"M:\GHG Estimation Package\aa_GHG_VMT\rates\GHG query output\GHG running 2050.csv") #GHG in CO2 equivalents
pmrates = pd.read_csv(r"M:\GHG Estimation Package\aa_GHG_VMT\rates\PM query output\PM running 2050.csv")    #PM2.5
noxrates = pd.read_csv(r'M:\GHG Estimation Package\aa_GHG_VMT\rates\NOx query output\NOx running 2050.csv') #NOx
vocrates = pd.read_csv(r'M:\GHG Estimation Package\aa_GHG_VMT\rates\VOC query output\VOC running 2050.csv') #VOCs


# speed bins
def speedclassify(mph):
    if mph < 2.5:
        avgSpeedBinID = 1
    elif 2.5 <= mph < 7.5:
        avgSpeedBinID = 2
    elif 7.5 <= mph < 12.5:
        avgSpeedBinID = 3
    elif 12.5 <= mph < 17.5:
        avgSpeedBinID = 4
    elif 17.5 <= mph < 22.5:
        avgSpeedBinID = 5
    elif 22.5 <= mph < 27.5:
        avgSpeedBinID = 6
    elif 27.5 <= mph < 32.5:
        avgSpeedBinID = 7
    elif 32.5 <= mph < 37.5:
        avgSpeedBinID = 8
    elif 37.5 <= mph < 42.5:
        avgSpeedBinID = 9
    elif 42.5 <= mph < 47.5:
        avgSpeedBinID = 10
    elif 47.5 <= mph < 52.5:
        avgSpeedBinID = 11
    elif 52.5 <= mph < 57.5:
        avgSpeedBinID = 12
    elif 57.5 <= mph < 62.5:
        avgSpeedBinID = 13
    elif 62.5 <= mph < 67.5:
        avgSpeedBinID = 14
    elif 67.5 <= mph < 72.5:
        avgSpeedBinID = 15
    elif mph >= 72.5:
        avgSpeedBinID = 16

    return avgSpeedBinID

links['avgSpeedBinID'] = links['mph'].apply(speedclassify)


# road types
links.loc[(links.vdf.isin([1, 6])) & (links['atype'] < 9), 'roadTypeID'] = 5
links.loc[(links.vdf.isin([1, 6])) & (links['atype'] >= 9), 'roadTypeID'] = 3

links.loc[~(links.vdf.isin([1, 6])) & (links['atype'] < 9), 'roadTypeID'] = 4
links.loc[~(links.vdf.isin([1, 6])) & (links['atype'] >= 9), 'roadTypeID'] = 2

#from here, we move away from 'links' dataframe to 'df2' dataframe
df2 = links.copy(deep=True)

# set to actual number of temporal hours, other periods OK
df2.loc[df2['timeperiod'] == 1, 'hours'] = 10

# split into hours
df2['pvt_vehicles_vmt'] = df2['pvt_vehicles_vmt'] / df2.hours
df2['bplate_vmt'] = df2['bplate_vmt'] / df2.hours
df2['ltruck_vmt'] = df2['ltruck_vmt'] / df2.hours
df2['mtruck_vmt'] = df2['mtruck_vmt'] / df2.hours
df2['mtrucklh_vmt'] = df2['mtrucklh_vmt'] / df2.hours
df2['htruck_vmt'] = df2['htruck_vmt'] / df2.hours
df2['htrucklh_vmt'] = df2['htrucklh_vmt'] / df2.hours
df2['bus_vmt'] = df2['bus_vmt'] / df2.hours

dflist = []
for h in range(1, 11):
    piece = df2.loc[df2.timeperiod == 1].copy()
    piece.loc[:, 'hr'] = h + 20
    dflist.append(piece)

piece = df2.loc[df2.timeperiod == 2].copy()
piece.loc[:, 'hr'] = 7
dflist.append(piece)

for h in range(1, 3):
    piece = df2.loc[df2.timeperiod == 3].copy()
    piece.loc[:, 'hr'] = h + 7
    dflist.append(piece)

piece = df2.loc[df2.timeperiod == 4].copy()
piece.loc[:, 'hr'] = 10
dflist.append(piece)

for h in range(1, 5):
    piece = df2.loc[df2.timeperiod == 5].copy()
    piece.loc[:, 'hr'] = h + 10
    dflist.append(piece)

for h in range(1, 3):
    piece = df2.loc[df2.timeperiod == 6].copy()
    piece.loc[:, 'hr'] = h + 14
    dflist.append(piece)

for h in range(1, 3):
    piece = df2.loc[df2.timeperiod == 7].copy()
    piece.loc[:, 'hr'] = h + 16
    dflist.append(piece)

for h in range(1, 3):
    piece = df2.loc[df2.timeperiod == 8].copy()
    piece.loc[:, 'hr'] = h + 18
    dflist.append(piece)

df3 = pd.concat(dflist).reset_index()

df3.loc[df3.hr > 24, 'hr'] = df3.hr - 24

# hourdayID
df3.loc[:,'hourDayID'] = df3.hr * 10 + 5

# new - assign more correct MOVES vehicle classes here before rate multiplication
df3['st11'] = df3['pvt_vehicles_vmt'] * 0.015  # from claire's workbook (internal/external flow)
df3['st21'] = (df3['pvt_vehicles_vmt'] * 0.985) * 0.55  # 2019 SoS rate
# new tbm - bplates just commercial
df3['st31'] = ((df3['pvt_vehicles_vmt'] * 0.985) * 0.45)
df3['st32'] = df3['bplate_vmt']
# light-duty model vehicles are weight plates 10,000lb+, so go in sush
df3['st52'] = df3.ltruck_vmt + df3.mtruck_vmt
df3['st53'] = df3.mtrucklh_vmt
# from the model it's about 96:4 the other direction, but that's single trips only (not daily total)
df3['st61'] = (df3.htruck_vmt + df3.htrucklh_vmt) * 0.06
df3['st62'] = (df3.htruck_vmt + df3.htrucklh_vmt) * 0.94
df3['st42'] = df3.bus_vmt

typelist = []


for v in [11, 21, 31, 32, 42, 52, 53, 61, 62]:
    coltotake = 'st' + str(v)
    x = df3[['roadTypeID', 'timeperiod', 'avgSpeedBinID', 'hr', coltotake]].copy()
    x.rename({'timeperiod': 'period', coltotake: 'vmt'}, axis=1, inplace=True)
    x.loc[:,'sourceTypeID'] = v
    typelist.append(x)

vmtdf = pd.concat(typelist).reset_index(drop=True)

ghgrates.rename({'coalesce(rateperdistance,0)': 'co2e/mi'}, axis=1, inplace=True)
pmrates.rename({'sum(coalesce(rateperdistance,0))': 'pm/mi'}, axis=1, inplace=True)
vocrates.rename({'sum(coalesce(rateperdistance,0))': 'voc/mi'}, axis=1, inplace=True)
noxrates.rename({'sum(coalesce(rateperdistance,0))': 'nox/mi'}, axis=1, inplace=True)

#merge ghg and pm together
ghgpm = ghgrates.merge(
    pmrates, 
    on=['yearID', 'monthID', 'dayID', 'hourID', 'roadTypeID', 'avgSpeedBinID', 'sourceTypeID']
)

#merge above with vmtdf
mdf = vmtdf.merge(
    ghgpm[['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID', 'co2e/mi', 'pm/mi']], 
    left_on=['sourceTypeID', 'avgSpeedBinID', 'hr', 'roadTypeID'],
    right_on=['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID'], 
    how='left'
)

#merge vocrates
mdf = mdf.merge(
    vocrates[['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID', 'voc/mi']], 
    left_on=['sourceTypeID', 'avgSpeedBinID', 'hr', 'roadTypeID'], 
    right_on=['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID'], 
    how='left'
)

#merge noxrates
mdf = mdf.merge(
    noxrates[['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID', 'nox/mi']],
    left_on=['sourceTypeID', 'avgSpeedBinID', 'hr', 'roadTypeID'],
    right_on=['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID'],
    how='left'
)

#drop redundant hourID fields that carried over from merge
drop = [x for x in mdf.columns.tolist() if x.startswith('hourID')]
mdf.drop(columns=drop, inplace=True)

#calculate emissions (in tons -- rates are in grams)
mdf['co2e'] = mdf.vmt * mdf['co2e/mi'] / 10**6
mdf['pm'] = mdf.vmt * mdf['pm/mi'] / 10**6
mdf['voc'] = mdf.vmt * mdf['voc/mi'] / 10**6
mdf['nox'] = mdf.vmt * mdf['nox/mi'] / 10**6


# typical July weekday results
emissions = mdf[['co2e', 'pm', 'voc', 'nox']].sum(axis=0)


#calculate costs
emissions['co2e_cost'] = emissions['co2e'] * parameters['POLL_ghg'] * parameters['pv_deprec_rate']
emissions['pm_cost'] = emissions['pm'] * parameters['POLL_pm25'] * parameters['pv_deprec_rate']
emissions['voc_cost'] = emissions['voc'] * parameters['POLL_voc'] * parameters['pv_deprec_rate']
emissions['nox_cost'] = emissions['nox'] * parameters['POLL_nox'] * parameters['pv_deprec_rate'] 
emissions['total_r_emissions'] = emissions['co2e_cost'] + emissions['pm_cost'] + emissions['voc_cost'] + emissions['nox_cost']
emissions['geog'] = 'region'

## --
## -- NOISE COSTS -- ##
## --

# noise costs depend on urban v rural. the following explains the 'atype' column of the dataset:
# atype_key = {
#     1: 'Chicago CBD',
#     2: 'Remainder of Central Chicago',
#     3: 'Remainder of City of Chicago',
#     4: 'Inner ring suburbs where Chicago street grid generally maintained',
#     5: 'Remainder of Illinois portion of Chicago Urbanized Area',
#     6: 'Indiana portion of Chicago Urbanized Area',
#     7: 'Other Urbanized Areas and Urban Clusters within CMAP Metropolitan Planning Area, plus other Urbanized Areas in northeastern Illinois',
#     8: 'Other Urbanized Areas and Urban Clusters in northwestern Indiana',
#     9: 'Remainder of CMAP Metropolitan Planning Area',
#     10: 'Remainder of Lake County, IN (rural)',
#     11: 'External Area',
#     99: 'Points of Entry - not defined in Capacity Zone system'
# }

print('  -- Calculating noise costs... ')
#pvt_vehicles
links.loc[links['atype']<=8, 'pvt_vehicles_noise_cost'] = links['pvt_vehicles_vmt'] * parameters['N_allveh_urban'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
links.loc[~(links['atype']<=8), 'pvt_vehicles_noise_cost'] = links['pvt_vehicles_vmt'] * parameters['N_allveh_rural'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
#ltruck
links.loc[links['atype']<=8, 'ltruck_noise_cost'] = links['ltruck_vmt'] * parameters['N_ltruck_urban'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
links.loc[~(links['atype']<=8), 'ltruck_noise_cost'] = links['ltruck_vmt'] * parameters['N_ltruck_rural'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
#mtruck
links.loc[links['atype']<=8, 'mtruck_noise_cost'] = links['mtruck_vmt'] * parameters['N_bustruck_urban'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
links.loc[~(links['atype']<=8), 'mtruck_noise_cost'] = links['mtruck_vmt'] * parameters['N_bustruck_rural'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
#htruck
links.loc[links['atype']<=8, 'htruck_noise_cost'] = links['htruck_vmt'] * parameters['N_bustruck_urban'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
links.loc[~(links['atype']<=8), 'htruck_noise_cost'] = links['htruck_vmt'] * parameters['N_bustruck_rural'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
#bus
links.loc[links['atype']<=8, 'bus_noise_cost'] = links['bus_vmt'] * parameters['N_bustruck_urban'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
links.loc[~(links['atype']<=8), 'bus_noise_cost'] = links['bus_vmt'] * parameters['N_bustruck_rural'] * parameters['ann_factor'] * parameters['pv_deprec_rate']
#total
links.eval('''\
    total_r_noise_cost = \
    pvt_vehicles_noise_cost + \
    ltruck_noise_cost + mtruck_noise_cost + \
    htruck_noise_cost + bus_noise_cost
''', inplace=True)


## --
## -- OPERATING COSTS -- ##
## --

print('  -- Calculating operating costs... ')
#by vehicle type
links.eval(f"pvt_vehicles_op_cost = pvt_vehicles_vmt * {parameters['OC_auto']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
links.eval(f"bplate_op_cost = bplate_vmt * {parameters['OC_bplate']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace = True)
links.eval(f"ltruck_op_cost = ltruck_vmt * {parameters['OC_ltruck']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace = True)
links.eval(f"mtruck_op_cost = mtruck_vmt * {parameters['OC_mtruck']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace = True)
links.eval(f"htruck_op_cost = htruck_vmt * {parameters['OC_htruck']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
#total operating cost value
links.eval('''\
    total_r_op_cost = \
    pvt_vehicles_op_cost + \
    bplate_op_cost + ltruck_op_cost + \
    mtruck_op_cost + htruck_op_cost\
''', inplace=True)

## --
## -- TRAVEL TIME COSTS
## --

print('  -- Calculating value-of-time metrics... ')
## NOTE -- right now we don't have pvt_vehicles separated by work and non-work-- need partial demand traffic assignment for that
## for now, i'm just calculating a percentage (based on parquet files)-- work trips are 27% of total trips and VHT, non-work is 73% of total trips and VHT
links.eval(f"pvt_vehicles_work_vot = pvt_vehicles_vht * 0.27 * {parameters['VOT_inv_work']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
links.eval(f"pvt_vehicles_nonwork_vot = pvt_vehicles_vht * 0.73 * {parameters['VOT_inv_nw']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
links.eval(f"bplate_vot = bplate_vht * {parameters['VOT_bplate']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
links.eval(f"ltruck_vot = ltruck_vht * {parameters['VOT_ltruck']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
links.eval(f"mtruck_vot = mtruck_vht * {parameters['VOT_mtruck']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
links.eval(f"htruck_vot = htruck_vht * {parameters['VOT_htruck']} * {parameters['ann_factor']} * {parameters['pv_deprec_rate']}", inplace=True)
#total travel time value
links.eval('''\
    total_r_vot = \
    pvt_vehicles_work_vot + \
    pvt_vehicles_nonwork_vot + \
    bplate_vot + ltruck_vot + \
    mtruck_vot + htruck_vot\
''', inplace=True)


print('  -- Done.')

# print('  -- Exporting rows to file (for QA/QC). This will take a couple minutes... ')
# links.to_csv(punchmoves_out)
# print(f'  -- Exported successfully. File stored at {punchmoves_out}')

print('  -- Creating model output summary table... ')
#calculation involves creating a dictionary of aggregation operations

# -- empty dictionary
agg = {}

#next several lines makes a list 'agg_cols' of all the columns to aggregate
agg_auto = [[a+'_vmt', a+'_vht', a+'_cvmt', a+'_cvht', a+'_pmt', a+'_cpmt', a+'_pht', a+'_cpht'] for a in auto_cols]
agg_auto = auto_cols + [a for list in agg_auto for a in list]
#--
agg_freight = [[a+'_vmt', a+'_vht', a+'_cvmt', a+'_cvht'] for a in freight_cols]
agg_freight = freight_cols + [a for list in agg_freight for a in list]
#--
# agg_slcols = [[a+'_vmt', a+'_vht', a+'_cvmt', a+'_cvht'] for a in sl_cols]
# agg_slcols = sl_cols + [a for list in agg_slcols for a in list]
#--
agg_othercols = ['len', 'lanemi']

agg_cols = agg_auto + agg_freight + agg_othercols

#make dictionary using each element in agg_cols
for col in agg_cols:
    agg[col] = 'sum'

#region links aggregation
totals = links.groupby('timeperiod').agg(agg)
totals['geog'] = 'region'

#project links aggregation
projectlinks = links.loc[links['projlink']==1].copy(deep=True)
projectlinktotals = projectlinks.groupby('timeperiod').agg(agg)
projectlinktotals['geog'] = f'project_{rsp_id}'

#regionwide total for all times of day (1 row)
sums = totals.sum(axis=0)
sums['geog'] = 'region'
totals.reset_index(inplace=True)
sums['timeperiod'] = 'Region Total'
#convert array (column) back to a dataframe, make row
sums = sums.to_frame().T

#projectwide total for all times of day (1 row)
psums = projectlinktotals.sum(axis=0)
psums['geog'] = f'project_{rsp_id}'
projectlinktotals.reset_index(inplace=True)
psums['timeperiod'] = 'Project Total'
#convert array (column) back to a dataframe, make row
psums = psums.to_frame().T

#concatenate everything together
totals = pd.concat([totals, sums, projectlinktotals, psums], axis=0, sort=True, ignore_index=True)


print('  -- Exporting summary to file...')
totals.to_csv(hwysummary_out, index=False)
print(f'  -- Exported successfully. File stored at {hwysummary_out}')
print('  -- Creating BCA table... ')

bca_cols = [
    'total_r_vot', 
    'total_r_op_cost', 
    'total_r_safety_cost', 
    'total_r_noise_cost', 
    'total_r_reliability_cost',
]


# - region links aggregation
bcatotal = links[bca_cols].sum(axis=0)
bcatotal['total_r_emissions'] = emissions['total_r_emissions']
bcatotal['geog'] = 'region'
bcatotal = bcatotal.to_frame().T

#project links aggregation
projectlinks = links.loc[links['projlink']==1].copy(deep=True)
bca_projtotal = projectlinks[bca_cols].sum(axis=0)
bca_projtotal['geog'] = f'project_{rsp_id}'
bca_projtotal = bca_projtotal.to_frame().T

#concatenate region and project metrics together
bcatotal = pd.concat([bcatotal, bca_projtotal], axis=0, sort=True, ignore_index=True)

#merge transit metrics
bca = pd.merge(bcatotal, bcasummary_trnt.to_frame().T, how='left', on='geog')
#get rid of nulls
bca.loc[bca['geog']==f'project_{rsp_id}'] = bca.loc[bca['geog']==f'project_{rsp_id}'].fillna(0)
bca = bca.copy(deep=True)

#add transit and roadway together
bca['Total Travel Time Cost'] = bca['total_r_vot'] + bca['total_trnt_vot']
bca['Total Vehicle Operating Cost (based on vehicle miles)'] = bca['total_r_op_cost'] + bca['total_trnt_op_cost']
bca['Total Emissions Cost'] = bca['total_r_emissions']
bca['Total Safety Cost (vehicular crashes and injuries)'] = bca['total_r_safety_cost'] #no transit safety cost
bca['Total Noise Cost'] = bca['total_r_noise_cost'] + bca['total_trnt_noise_cost']
bca['Total Reliability Cost'] = bca['total_r_reliability_cost'] #no trnt reliability cost
bca = bca[[
    'geog',
    'Total Travel Time Cost',
    'Total Vehicle Operating Cost (based on vehicle miles)',
    'Total Safety Cost (vehicular crashes and injuries)',
    'Total Noise Cost',
    'Total Reliability Cost',
    'Total Emissions Cost'
]]
#sum all columns together (except geog column)
bca['Total Costs'] = bca.iloc[:,1:].sum(axis=1)


print('  -- Exporting summary to file...')
bca.to_csv(bcasummary_out, index=False)
print(f'  -- Exported successfully. File stored at {bcasummary_out}')