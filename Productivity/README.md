# Code for computing the productivity measures reported in the article

This directory includes the following files:
- `preprints_analysis_6m_nonundefined.ipynb` This file computes several quantities computed for the analysis of new authors, authorships, preprints and productivity.
- `panel_repo_nonundefined.ipynb` This file produces the plots used in the productivity panel reported in the main article.
- `measures.py` Auxiliary file containing the algorithm for computing the productivity measures. It is imported by the notebooks listed above.
- `compute_stats_panel.ipynb` Script to compute database statistics reported in the productivity panel in the main text. This scripts was used on google colab environment and was not tested locally.
- `2CSV_makefiles.ipynb` This script reads the MAG database on spark, selects the necessary data for the productivity analysis and save local files for offline processing. It was used on the google colab environment and was not tested locally.

Except for when explicitly mentioned, all code can be run locally in any Python Anaconda installation requiring only the adjustment of the paths pointing to the input files. 
