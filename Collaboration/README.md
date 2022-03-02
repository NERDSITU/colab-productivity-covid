# Code for computing the collaboration measures reported in the article (+ SI)

## This directory includes the following folders:
- `network` this folder contains code to reproduce the collaboration analysis
on preprints, which is the basis of the figures in
the main panel (Figure 2). Subfolders  `prepare_data` contains preprocessing
and computation of most metrics (aiming at Figure 2c and Figure 2d), `prepare_community`
runs community detection for recreating Figure 2a and Figure 2b.
- `network_SI` this folder contains code to reproduce the collaboration
analysis on 10% of total scientific papers. All for SI. Almost all of the code
in `network_SI` is the same code as for the main `network` analysis, just run
on a different data set.  \\

## Run
Early preprocessing requires a function installation of Spark/PySpark,
otherwise the code relies on python.
