#!/usr/bin/env python
# coding: utf-8

# outputs PM2.5 and CO2e running emissions in grams for typical July weekday
# CO2e is for 7-county region, PM2.5 is only for EDA portions of zones in 7-county region. 

import pandas as pd
import numpy as np
import sys

# b-plate breakout set up for new tbm

# # moves.data from the run
# mdlist = []
# for x in ['pd1','pd2','pd3','pd4','pd5','pd6','pd7','pd8']:
#     mdfpiece = pd.read_csv("data/moves_{}.data".format(x), sep='\s+', engine='python')
#     mdlist.append(mdfpiece)
# df = pd.concat(mdlist)

# lhdf = pd.read_csv("data/moves.longhaul.data", sep='\s+', engine='python')
print('  importing data...')
##
punchlink = sys.argv[1]+'\\Database\\data\\punchlink.csv'
output = sys.argv[1]+'\\Database\\rsp_evaluation\\results\\emissions.csv'
df = pd.read_csv(punchlink)

# eda zone share
eda = pd.read_csv(r"M:\rsp_evaluation\ON_TO_2050_Plan_Update\Inputs\excl_pop_share.csv")

# change years if necessary!
ghgrates = pd.read_csv(r"M:\GHG Estimation Package\aa_GHG_VMT\rates\GHG query output\GHG running 2050.csv")
pmrates = pd.read_csv(r"M:\GHG Estimation Package\aa_GHG_VMT\rates\PM query output\PM running 2050.csv")

# prepare files
df.rename(columns={'i_node':'inode','j_node':'jnode'}, inplace=True)


for x in ['inode', 'jnode', 'lan', 'vdf', 'zone', 'tmpl2', 'imarea']:
    df[x] = df[x].astype(int)

for x in ['len', 'emcap', 'timau', 'ftime', 'avauv', 'avh2v', 'avh3v', 'avbqv', 'avlqv',
          'avmqv', 'avhqv', 'atype', 'busveq', 'h200', 'm200', 'result']:
    df[x] = df[x].astype(float)

print('  performing emissions calculations...')

# flag links within 7-county
df.loc[(df.zone <= 2926), 'dist'] = 1
# join with eda share
df = df.merge(eda, left_on='zone', right_on='o_zone', how='left')

# combine autos
df['avauv'] = df['avauv'] + df['avh2v'] + df['avh3v']

# merge together
# df2 = df.merge(lhdf, on=['inode', 'jnode', 'timeperiod'], how='left')
df2 = df.copy()

# set up other vehicle classes
df2['m200'] = df2[['m200', 'avmqv']].min(axis=1)
df2['avmqv'] = np.maximum(df2['avmqv'] - df2['m200'], 0)
df2['h200'] = df2[['h200', 'avhqv']].min(axis=1)
df2['avhqv'] = np.maximum(df2['avhqv'] - df2['h200'], 0)

# convert from veq to vehicles
df2['auveh'] = np.maximum(df2['avauv'], 0)
df2['bpveh'] = np.maximum(df2['avbqv'], 0)
df2['ldveh'] = np.maximum(df2['avlqv'], 0)
df2['mdshveh'] = np.maximum(df2['avmqv'] / 2, 0)
df2['mdlhveh'] = np.maximum(df2['m200'] / 2, 0)
df2['hdshveh'] = np.maximum(df2['avhqv'] / 3, 0)
df2['hdlhveh'] = np.maximum(df2['h200'] / 3, 0)
df2['vubus'] = np.maximum(df2['busveq'] / 3, 0)

# vmt
df2['auvehmi'] = df2['auveh'] * df2.len
df2['bpvehmi'] = df2['bpveh'] * df2.len
df2['ldvehmi'] = df2['ldveh'] * df2.len
df2['mdshvehmi'] = df2['mdshveh'] * df2.len
df2['mdlhvehmi'] = df2['mdlhveh'] * df2.len
df2['hdshvehmi'] = df2['hdshveh'] * df2.len
df2['hdlhvehmi'] = df2['hdlhveh'] * df2.len
df2['vubusmi'] = df2['vubus'] * df2.len

# total vehicles and speed
df2['veh'] = df2[['auveh', 'bpveh', 'ldveh', 'mdshveh', 'mdlhveh', 'hdshveh', 'hdlhveh', 'vubus']].sum(axis=1)

df2.loc[df2['ftime'] == 0, 'fmph'] = 0
df2.loc[df2['ftime'] != 0, 'fmph'] = df2.len / (df2['ftime'] / 60)

df2.loc[df2['timau'] == 0, 'mph'] = 0
df2.loc[df2['timau'] != 0, 'mph'] = df2.len / (df2['timau'] / 60)

# set up hours
df2.loc[df2.timeperiod == 1, 'hours'] = 5
df2.loc[df2.timeperiod.isin([2, 4]), 'hours'] = 1
df2.loc[df2.timeperiod == 5, 'hours'] = 4
df2.loc[df2.hours.isnull(), 'hours'] = 2

df2['v'] = df2[['avauv', 'avbqv', 'avlqv', 'avmqv', 'm200', 'avhqv', 'h200']].sum(axis=1)
df2['c'] = df2['emcap'] * df2.lan * df2.hours
df2.loc[((df2.vdf == 1) & (df2.fmph > 0)), 'mph'] = df2.fmph * (1 / ((np.log(df2.fmph, where=df2.fmph > 0) * 0.249) + 0.153 * (df2.v / (df2.c * 0.75)) ** 3.98))

# VHT
for i in ['auvehhr', 'bpvehhr', 'ldvehhr',
          'mdshvehhr', 'mdlhvehhr', 'hdshvehhr',
          'hdlhvehhr', 'vubushr']:
    df2.loc[df2.mph == 0, i] = 0

for i in ['auvehhr', 'bpvehhr', 'ldvehhr',
          'mdshvehhr', 'mdlhvehhr', 'hdshvehhr',
          'hdlhvehhr', 'vubushr']:
    name = i[:-2]
    df2.loc[df2.mph != 0, i] = df2.len / (df2.mph * df2[name])

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


df2['avgSpeedBinID'] = df2['mph'].apply(speedclassify)

# road types
df2.loc[(df2.vdf.isin([1, 6])) & (df2['atype'] < 9), 'roadTypeID'] = 5
df2.loc[(df2.vdf.isin([1, 6])) & (df2['atype'] >= 9), 'roadTypeID'] = 3

df2.loc[~(df2.vdf.isin([1, 6])) & (df2['atype'] < 9), 'roadTypeID'] = 4
df2.loc[~(df2.vdf.isin([1, 6])) & (df2['atype'] >= 9), 'roadTypeID'] = 2

# set to actual number of temporal hours, other periods OK
df2.loc[df2.timeperiod == 1, 'hours'] = 10

# split into hours
df2['auvehmi'] = df2['auvehmi'] / df2.hours
df2['bpvehmi'] = df2['bpvehmi'] / df2.hours
df2['ldvehmi'] = df2['ldvehmi'] / df2.hours
df2['mdshvehmi'] = df2['mdshvehmi'] / df2.hours
df2['mdlhvehmi'] = df2['mdlhvehmi'] / df2.hours
df2['hdshvehmi'] = df2['hdshvehmi'] / df2.hours
df2['hdlhvehmi'] = df2['hdlhvehmi'] / df2.hours
df2['vubusmi'] = df2['vubusmi'] / df2.hours


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
df3['st11'] = df3['auvehmi'] * 0.015  # from claire's workbook (internal/external flow)
df3['st21'] = (df3['auvehmi'] * 0.985) * 0.55  # 2019 SoS rate
# new tbm - bplates just commercial
df3['st31'] = ((df3['auvehmi'] * 0.985) * 0.45)
df3['st32'] = df3['bpvehmi']
# light-duty model vehicles are weight plates 10,000lb+, so go in sush
df3['st52'] = df3.ldvehmi + df3.mdshvehmi
df3['st53'] = df3.mdlhvehmi
# from the model it's about 96:4 the other direction, but that's single trips only (not daily total)
df3['st61'] = (df3.hdshvehmi + df3.hdlhvehmi) * 0.06
df3['st62'] = (df3.hdlhvehmi + df3.hdshvehmi) * 0.94
df3['st42'] = df3.vubusmi

typelist = []


for v in [11, 21, 31, 32, 42, 52, 53, 61, 62]:
    coltotake = 'st' + str(v)
    x = df3[['dist', 'roadTypeID', 'timeperiod', 'avgSpeedBinID', 'hr', 'EDAshare', coltotake]].copy()
    x.rename({'timeperiod': 'period', coltotake: 'vmt'}, axis=1, inplace=True)
    x.loc[:,'sourceTypeID'] = v
    typelist.append(x)

vmtdf = pd.concat(typelist).reset_index(drop=True)


ghgrates.rename({'coalesce(rateperdistance,0)': 'co2e/mi'}, axis=1, inplace=True)
pmrates.rename({'sum(coalesce(rateperdistance,0))': 'pm/mi'}, axis=1, inplace=True)

a = ghgrates.merge(pmrates, on=['yearID', 'monthID', 'dayID', 'hourID', 'roadTypeID', 'avgSpeedBinID', 'sourceTypeID'])

mdf = vmtdf.merge(a, left_on=['sourceTypeID', 'avgSpeedBinID', 'hr', 'roadTypeID'],
                  right_on=['sourceTypeID', 'avgSpeedBinID', 'hourID', 'roadTypeID'], how='left')

mdf['co2e'] = mdf.vmt * mdf['co2e/mi']
mdf['pm'] = mdf.vmt * mdf['pm/mi'] * mdf.EDAshare

print('  exporting to csv...')

# typical July weekday results
mdf.groupby(['dist']).agg(
    {'vmt': 'sum', 'co2e': 'sum', 'pm': 'sum'}).to_csv(output)

print('  rsp_emissions_2.py completed!')


