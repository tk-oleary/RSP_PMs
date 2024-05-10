@echo off
set "nobuild=%1"
set "run=%2"

@REM point to where the python scripts and batch file are located
set "batch_dir=%~dp0"
echo python scripts located %batch_dir%

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
echo Doing export_geog.py
echo %run%\cmap_trip_based_model
%empypath% %batch_dir%export_geog.py !run!
echo Doing select_by_location.py 
%gispypath% %batch_dir%select_by_location.py !run! !nobuild!
echo Doing rsp_emissions_2.py
%empypath% %batch_dir%rsp_emissions2.py !run!

del %einfile%
del %ginfile%

echo GetRSPCorridorInfo_SingleProject.bat complete!