echo For testing, parent folder is E:\tko\TPAT\RSP_Evals\Test_repo. Change when finalized.
@REM set /p parent_folder=Copy-paste the folder location which stores the model runs: 
set parent_folder="E:\tko\TPAT\RSP_Evals\Test_repo"

@REM point to where the python scripts and batch file are located
set batch_dir=%~dp0

@REM point to installation of Emme Python environment
set einfile=epath.txt
dir "C:\Program Files\INRO\*python.exe" /s /b >> %einfile% 
set /p empypath=<%einfile%
set paren="
set empypath=%paren%%empypath%%paren%
echo Emme pypath = %empypath%

@REM point to installation of ArcGIS Pro Python environment
set ginfile=gpath.txt
dir "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" /s /b >> %ginfile%
set /p gispypath=<%ginfile%
set paren="
set gispypath=%paren%%gispypath%%paren%
echo GIS pypath = %gispypath%

@REM export emme geography from all rsp runs (including no-build)
setlocal enabledelayedexpansion
for /D %%F in ("%parent_folder%\RSP*") do (
    echo Obtaining geography and link data for %%~nxF
    %empypath% %batch_dir%export_geog.py %%F\cmap_trip-based_model
    echo Writing corridor csvs for %%~nxF
    %gispypath% %batch_dir%select_by_location.py %%F\cmap_trip-based_model
)

del %einfile%
del %ginfile%

echo GetRSPCorridorInfo.bat complete!
