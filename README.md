Seazone case solution
==============================

**Read the report on "reports/Seazone Challenge Report.pdf"**

**Make sure that all raw data files are in a folder in the following path of this project: "data/raw"**

**Data files considered in this case should have the following names:**
1. Details_Data.csv
2. Hosts_ids_Itapema.csv
3. Mesh_Ids_Data_Itapema.csv
4. Price_AV_Itapema-001.csv
5. VivaReal_Itapema.csv

**Steps to install and execute this project:**
1. git clone this repository
2. navigate to the source of this repository
3. run "pip install -r requirements.txt"
4. make sure that all data is in the correct folder
5. run "python -m src.solve_case"
6. figures will be in "reports/figures"
7. the final PDF report is in "reports"

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
    ├── notebooks          <- Jupyter notebooks.
    │
    ├── references         <- Explanatory materials.
    │
    ├── reports            <- Final report.
    │   └── figures        <- Figures used to analyze and create the report.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment.
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- All scripts
    │   │  
    │   │
    │   │
    │   └── visualization  <- Empty
    │    
    │
    └── 


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
