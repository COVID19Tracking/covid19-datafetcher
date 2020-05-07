# covid19-datafetcher
Fetch COVID19 data published by US states.


This project has the scripts and data-source definitions needed to fetch COVID-19 related data (e.g., tests, cases, death) from ArcGIS data sources used by US states.


The biggest value of this repository comes from (1) the list of state data sources and (2) the mapping that maps state-specific property name to a common terminology (e.g., `T_Pos_Count` to `POSITIVE`).
<br />The scripts that fetch and process the data are very minimal now.


## TODO
This was originally a Jupyter notebook (this is why `pandas` is used, now it's only creating a csv), I exported it and separated it into separate files, but the separation is not great yet. 

### Project
- [ ] documentation + running instructions
- [ ] requirements & packaging

### Data 
- [x] handle csv data sources
- [ ] zip data sources
- [ ] shit sources (html page)

### Code
- [ ] Use a real logger
- [x] Different data-source configuration scheme: yaml (with some flow for the query-params part).
- [ ] parallel fetching the data
