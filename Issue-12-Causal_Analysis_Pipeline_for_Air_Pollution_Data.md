# Causal Analysis Pipeline for Air Pollution Data

## Issue # 12

## Assigned to:

* Hendrik Makait - @HndrkMkt
* Ricardo Salazar - @ricardo8914

## Mentored by:

* Tilmann Rabl - @tilmannrabl

## Summary

Learning how to recover causal structures from large and heterogeneous observational data is key to the process of understanding causal hypothesis in different challenging problems of human-related activities like climate change, air pollution analysis or genetics. Problems for causal analysis range from the integration of diverse data sources to the recovery of the correct causal structure based on the data's properties. In this project report we demonstrate the design of a causal analysis pipeline at the example of air pollution data. We introduce feature tables as a standardized representation for individual data sources that facilitate the generation of combined datasets. Further, we illustrate a hypothesis-driven approach to causal discovery and analyze the influence of individual modeling choices on the result quality and runtimes. Finally, our causal model for air pollution and weather data shows that air pollution is highly dependent on the current and previous weather situation, but independent of seasonal effects, which contradicts previous findings.

## Deliverables
* Midterm Presentation
* Final Presentation
* Project Report
* Project Implementation


## Project Implementation Structure
* causal_analysis/
* data/
    * intermediate/
        * filtered/
    * processed/
        * statistics/
    * raw/
        * csv_per_month/ (NEED TO ADD .gitkeep)
         weather/
* data_acquisition/
    * luftdaten/
    * weather/
* data_processing/
* docs/
    * javadoc/
    * pythondoc/
* experiments/
    * causal_discovery/
    * performance/
* notebooks/
* README.md
* requirements.txt
* setup.py
    
    