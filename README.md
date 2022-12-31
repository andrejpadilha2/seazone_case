seazone_case
==============================

Seazone case solution

Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc. Final report is here.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── 


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

**Make sure that raw data files are in "data/raw"**
Data files considered in this case should have the following names:
1. Details_Data.csv
2. Hosts_ids_Itapema.csv
3. Mesh_Ids_Data_Itapema.csv
4. Price_AV_Itapema-001.csv
5. VivaReal_Itapema.csv

Steps to install and execute this project:
1. git clone this repository
2. navigate to the source of this repository
3. run "pip install -r requirements.txt"
4. make sure that all data is in the correct folder
5. run "python -m src.solve_case"
6. figures will be in "reports/figures"
7. the final PDF report is in "reports"

run "python -m src.solve_case"
imgs will be in "x"
report in PDF
