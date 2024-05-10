###########
## SETUP ##
###########

#libraries
import os, sys
import pandas as pd
import fnmatch

#filepaths
run = sys.argv[1] #'cmap_trip-based_model' folder
print(f"run folder location: {run}")

rsp_name = os.path.basename(os.path.dirname(run)) #name of run -- above cmap_trip: e.g., 'RSP57'

emp_file = [os.path.join(run, file) for file in os.listdir(run) if file.endswith('.emp')][0]

##########################################
## INITIALIZE EMME AND SETUP EMME TOOLS ##
##########################################

#import emme desktop and initialize emme
import inro.emme.desktop.app as _app
desktop = _app.start_dedicated(
    visible=False,
    user_initials="cmap",
    project=emp_file
)

#import emme modeller
import inro.modeller as _m
modeller = _m.Modeller(desktop=desktop)
emmebank = modeller.emmebank

#define emme tool and scenario
export = modeller.tool('inro.emme.data.network.export_network_as_shapefile')
net_calc = modeller.tool('inro.emme.network_calculation.network_calculator')

#####################################################
## DETERMINE NO-BUILD, ROADWAY, OR TRANSIT ##
#####################################################

# determine RSP type -- no-build, roadway (select_link), or transit (select_line)
slink_file = fnmatch.filter(os.listdir(os.path.join(run,'Database\\Select_Link')), '*.txt') #select link file, if it exists
sline_file = fnmatch.filter(os.listdir(os.path.join(run,'Database\\Select_Line')), '*.txt') #select line file, if it exists
link = len(slink_file) #number of files it flagged (should be 0 or 1)
line = len(sline_file) #number of files it flagged (should be 0 or 1)

# throw an error if there's more than 1 select_link or select_line
if link != 0 and line != 0:
    raise ValueError(f'There are both select_line AND select_link files in {rsp_name} when only one is permitted.')
if link == 1:
    rsptype='link'
elif line == 1:
    rsptype='line'
elif link == 0 and line == 0:
    rsptype='base'
else:
    raise ValueError(f'There is not exactly one Select_Link or one Select_Line file in {rsp_name} when only one (or none) is permitted.')

####################
## EXPORT NETWORK ##
####################

#if the rsp is roadway, export scenario 70029
if rsptype=='link':
    rsp_shp = run + '\\Database\\Select_Link' #export location
    print(f'Exporting highway project links for {rsp_name}')

    rsp_links = slink_file[0] #file that tells us which links are part of the RSP

    #export network as shp

    #rsp links
    export(
        export_path = os.path.join(rsp_shp,'scen_70029\\rsp'),
        view_results_flag = False,
        transit_shapes = 'LINES_AND_SEGMENTS',
        scenario = emmebank.scenario(70029),
        selection = {"link":f"~<Select_Link\{rsp_links}"}
    )

    #entire network
    export(
        export_path = os.path.join(rsp_shp,'scen_70029\\all'),
        view_results_flag = False,
        transit_shapes = 'LINES_AND_SEGMENTS',
        scenario = emmebank.scenario(70029)
    )

# --- 
    
#if run is no-build, will export scenario 70029 and create transitpunch.csv if not already created
if rsptype=='base':

    rsp_shp = run + '\\Database\\Select_Link' #export location of links
    print(f'Exporting no-build highway network {rsp_name}...')
    scen_list = [70029]
    for scen in scen_list:
        print(f'... exporting scenario {scen}')
        export(
            export_path = rsp_shp+f'\\scen_{scen}',
            view_results_flag = False,
            transit_shapes = 'LINES_AND_SEGMENTS',
            scenario = emmebank.scenario(scen)
        )

    #check whether transitpunch.csv has been created. if not, execute the following:
    if not os.path.exists(run+'\\Database\\data\\transitpunch.csv'):
        print('Transit punch data not found. Creating now. This will take a couple minutes...')
        df_list = []

        desired_attributes = '''\
            "length+hdw+voltr+\
            us1+@zone"\
        '''

        #transit time periods (x21, x23, x25, x27, where x=1st digit of scenario year)
        trnt_scen = [721,723,725,727]
        #iterate through each time period
        for tp in trnt_scen:
            print(f'  -- Obtaining transit link data for scenario {tp}...')

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

        #export to csv
        trlinkdata.to_csv(run+'\\Database\\data\\transitpunch.csv')
        print('Created transitpunch.csv in ..\\Database\\data.')
    else: 
        print(f'File transitpunch.csv found in ..\\Database\\data. Proceeding...')


#if the rsp is transit, then export rsp segments and create transitpunch.csv if not already created
if rsptype=='line':

    rsp_shp = run + '\\Database\\Select_Line' #export location of rsp segments

    print(f'shapefile output location: {rsp_shp}')
    print(f'Exporting transit project segments for {rsp_name}')
    scen_list = [721,723,725,727]
    rsp_links = [file for file in os.listdir(run+'\\Database\\Select_Line')][0]
    for scen in scen_list:

        #export rsp transit segment by itself, for i-j pairs
        export(
        export_path = rsp_shp+f'\\scen_{scen}\\rsp',
        view_results_flag = False,
        transit_shapes = 'LINES_AND_SEGMENTS',
        scenario = emmebank.scenario(scen),
        selection = {"transit_line":f"~<Select_Line\{rsp_links}"}
        )


    #check whether transitpunch.csv has been created. if not, execute the following:
    if not os.path.exists(run+'\\Database\\data\\transitpunch.csv'):
        print('Transit punch data not found. Creating now. This will take a couple minutes...')
        df_list = []

        desired_attributes = '''\
            "length+hdw+voltr+\
            us1+@zone"\
        '''

        #transit time periods (x21, x23, x25, x27, where x=1st digit of scenario year)
        trnt_scen = [721,723,725,727]
        #iterate through each time period
        for tp in trnt_scen:
            print(f'  -- Obtaining transit link data for scenario {tp}...')

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

        #export to csv
        trlinkdata.to_csv(run+'\\Database\\data\\transitpunch.csv')
        print('Created transitpunch.csv in ..\\Database\\data.')
    else: 
        print(f'File transitpunch.csv found in ..\\Database\\data. Proceeding...')

print(f'Completed export_geog.py for: {rsp_name}')