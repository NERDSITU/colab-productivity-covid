# Code for computing the collaboration measures reported in the article (+ SI)

## This directory includes the following folders:
- `/network` this folder contains code to reproduce the collaboration analysis
on preprints, which is the basis of the figures in
the main panel (Figure 2). Subfolders  `/prepare_data` contains preprocessing
and computation of most metrics (aiming at Figure 2c and Figure 2d), `/prepare_community`
runs community detection for recreating Figure 2a and Figure 2b. The figures
themselves are generated from the scripts in `plot_final`.
- `network_SI` follows the same organization (without the
`/prepare_community`). This folder contains code to reproduce the collaboration
analysis on 10% of total scientific papers which is reported in the
supplementary information (SI).  \\

## Run
All code run on google colab. 
