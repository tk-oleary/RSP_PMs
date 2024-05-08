# RSP_PMs
author: toleary
date: 2024 May

This repository is a collection of scripts that will eventually be run as part of RSP evaluations for the long-range planning process.

These scripts agglomerate several performance metrics (PM) scripts into one. First, it searches for completed PM script outputs in each model run; if those outputs do not exist, it re-runs the script.

Then, these scripts compile results from all scripts into a single csv, which will then be uploaded to TPAT.

This is in the beginning stages of development. Ideally, the end-result will allow the user to specify which runs to run, and prompt the user whether they want to upload the metrics directly to TPAT in AGOL. These will be represented as "issues" later.
