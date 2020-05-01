# covid19-datafetcher
Fetch COVID19 data published by US states.


This project has the scripts and data-source definitions needed to fetch COVID-19 related data (e.g., tests, cases, death) from ArcGIS data sources used by US states.


The biggest value of this repository comes from (1) the list of state data sources and (2) the mapping that maps state-specific property name to a common terminology (e.g., `T_Pos_Count` to `POSITIVE`)


## Coding TODOs
This was originally a Jupyter notebook (this is why `pandas` is used), I exported it and separated it into separate files, but the separation is not great yet. 

- [ ] Use a real logger
- [ ] Different data-source configuration scheme
- [ ] Next: handle less-structured data sources
