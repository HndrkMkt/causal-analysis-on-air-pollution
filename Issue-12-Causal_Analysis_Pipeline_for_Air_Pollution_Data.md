# Causal Analysis Pipeline for Air Pollution Data

## Issue # 12

## Assignees:

* Hendrik Makait - @HndrkMkt
* Ricardo Salazar - @ricardo8914

## Mentor:

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
    - In this directory lie neccesary scripts and one Python package to process intermediate datasets 
* data/
    * intermediate/
        * filtered/
            - Filetered sensor data is placed here after been processed by the data processing component using Flink
    * processed/
        * statistics/
            - Intermediate data is placed in this folder after been processed by the data processing component using Flink
    * raw/
        * csv_per_month/ (NEED TO ADD .gitkeep)
            - Bash scripts place the sensor raw data in this directory
         weather/
            - Weather csv containing weather data is placed here after running the corresponding Python script
* data_acquisition/
    * luftdaten/
        -In this directory lie the neccesary bash scripts to pull the sensor data from the luftdaten.org project
    * weather/
        -In this directory the weather api Python script can be found
* data_processing/
    - This directory contains the Java project that processess the sensor and weather data to output filtered and intermediate results to be used later in causal discovery
* docs/
    - In this folder can be found all the documentation for the project code 
        * diagrams/
        * javadoc/
        * pythondoc/
    
* experiments/
    - In this directory the scripts to run the performance and causal discovery experiments can be found
        * causal_discovery/
        * performance/
* notebooks/
    - In this folder some notebooks can be found, to illustrate some guided work trough sensor localization and causal discovery
* README.md
* requirements.txt
* setup.py
    
    