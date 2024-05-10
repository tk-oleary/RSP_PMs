import arcpy
import pandas as pd
import sys, os
import fnmatch

arcpy.env.overwriteOutput = True
arcpy.env.workspace = 'in_memory'

run = sys.argv[1] #'cmap_trip-based_model' folder of rsp run
nbrun = sys.argv[2] #'cmap_trip-based_model' folder of no-build run
rsp_name = os.path.basename(os.path.dirname(run)) #name of folder in repository (e.g., 'RSP57')

##check whether RSP is transit, highway, or no-build, by finding .txt file in Select_Link or Select_Line
link = len(fnmatch.filter(os.listdir(run+'\\Database\\Select_Link'), '*.txt'))
line = len(fnmatch.filter(os.listdir(run+'\\Database\\Select_Line'), '*.txt'))

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


#setup values for iterator through geography data
rsp_geo = run + '\\Database\\Select_Link' #rsp folder containing shapefiles
nb_geo = nbrun + '\\Database\\Select_Link' #nobuild folder containing shapefiles


#select nobuild shp by proximity to rsp shp

if rsptype == 'link':
    #highway RSPs need corridor-level info on no-build network and RSP network -- for corridor-level comparison metrics

    out = run + '\\Database\\Select_Link' #desired location for output csv of networks within RSP corridor

    #setup input/output file names for each scenario
    nb_geo_scen = nb_geo + f'\\scen_70029\\emme_links.shp'
    rsp_geo_scen = rsp_geo + f'\\scen_70029\\rsp\\emme_links.shp'
    rsp_all_geo_scen = rsp_geo + f'\\scen_70029\\all\\emme_links.shp'

    out_nb_scen = out + f'\\nb_corridor_70029.csv'
    out_rsp_scen = out + f'\\rsp_corridor_70029.csv'

    print(f'processing scenario 70029 for {rsp_name}...')

    #put data in NAD 27 State Plane Illinois East (system we use in model)
    for fc in [nb_geo_scen, rsp_geo_scen, rsp_all_geo_scen]:
        arcpy.management.DefineProjection(
            in_dataset = fc,
            coor_system = arcpy.SpatialReference(26771)
        )

    #export corridor links from no-build network

    #make feature layer for selection
    arcpy.management.MakeFeatureLayer(
        in_features = nb_geo_scen,
        out_layer = 'nb_geo_scen'
    )
    #select feature layer
    arcpy.management.SelectLayerByLocation(
        in_layer = 'nb_geo_scen',
        overlap_type = 'WITHIN_A_DISTANCE',
        select_features = rsp_geo_scen,
        search_distance = '5 Miles',
        selection_type = 'NEW_SELECTION'
    )
    #export selected links from feature layer
    arcpy.management.CopyRows(
        in_rows = 'nb_geo_scen',
        out_table = out_nb_scen
    )

    #export corridor links from RSP network 
    
    #make feature layer for selection
    arcpy.management.MakeFeatureLayer(
        in_features = rsp_all_geo_scen,
        out_layer = 'rsp_all_geo_scen'
    )
    #select feature layer
    arcpy.management.SelectLayerByLocation(
        in_layer = 'rsp_all_geo_scen',
        overlap_type = 'WITHIN_A_DISTANCE',
        select_features = rsp_geo_scen,
        search_distance = '5 Miles',
        selection_type = 'NEW_SELECTION'
    )
    #export selected links from feature layer
    arcpy.management.CopyRows(
        in_rows = 'rsp_all_geo_scen',
        out_table = out_rsp_scen
    )

    #remove other output files that aren't the csv
    if os.path.exists(out_nb_scen+'.xml'):
        os.remove(out_nb_scen+'.xml')
    if os.path.exists(out_rsp_scen+'.xml'):
        os.remove(out_rsp_scen+'.xml')
    if os.path.exists(out+'schema.ini'):
        os.remove(out+'schema.ini')

if rsptype == 'line':
    #transit RSPs need full networks exported for each scenario -- for regionwide comparison metrics
    # scens = ['721','723','725','727']
    # for scen in scens:
    #     #setup input/output file names for each scenario
    #     rsp_geo_scen = rsp_geo + f'\\scen_{scen}\\rsponly\\emme_tsegs.shp'
    #     all_geo_scen = rsp_geo + f'\\scen_{scen}\\all\\emme_tsegs.shp'
    #     out_rsp_scen = out + f'\\tsegs_{rsp_name}_{scen}.csv'
    #     out_all_scen = out + f'\\tsegs_{rsp_name}_{scen}_all.csv'

    #     print(f'processing scenario {scen} for {rsp_name}...')
    #     #no "corridor" analysis with select_line, so just exporting the transit segments to csv
    #     arcpy.management.CopyRows(
    #         in_rows = rsp_geo_scen,
    #         out_table = out_rsp_scen
    #     )
    #     arcpy.management.CopyRows(
    #         in_rows = all_geo_scen,
    #         out_table = out_all_scen
    #     )
    print('Line RSP does not need corridor geography. Pass.')
    



if rsptype == 'base':
    #the no-build network needs transit networks exported for comparison with transit RSPs

    ## THE FOLLOWING NEEDS TO BE UNHASHED AFTER TRANSIT ASSIGNMENT GETS DONE ON NOBUILD RUN
    # scens = ['721', '723', '725', '727']
    # for scen in scens:
    #     #exporting everything
    #     nb_geo_scen = nb_geo + f'\\scen_{scen}\\emme_tsegs.shp'
    #     out_nb_scen = out + f'\\nb_{scen}.csv'

    #     print(f'processing scenario {scen} for {rsp_name}...')
    #     #export selected links from nobuild shapefile
    #     arcpy.management.CopyRows(
    #         in_rows = nb_geo_scen,
    #         out_table = out_nb_scen
    #     )
    print('No-build RSP not applicable. Pass.')




print('Done!')