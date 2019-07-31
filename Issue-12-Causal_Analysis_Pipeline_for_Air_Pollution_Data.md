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

## Overview of Project Structure
* causal_analysis/: the Python component to process the intermediate data and run causal experiments
* data/: all the data for the causal analysis pipeline will be placed here after downloading/calculating
    * intermediate/: intermediate data used in subsequent jobs and not used for further analysis
        * filtered/: prefiltered sensor data is stored here to avoid unnecessary computations
    * processed/: processed data such as the intermediate data CSV file 
        * statistics/: statistics calculated for individual sensors
    * raw/: all raw data downloaded from the sources
         weather/: weather data from OpenWeatherMap
* data_acquisition/: scripts for downloading and handling the raw data
    * luftdaten/: bash scripts to pull the sensor data from the luftdaten.org and change their compression
    * weather/: Python scripts for downloading weather data and subsetting sensors using the OpenWeatherMap API
* data_processing/: Java/Flink component that processes the sensor and weather data and generates intermediate data file to be used later in causal discovery
* docs/: documentation of Java and Python components
        * diagrams/: class diagrams of Java component
        * javadoc/: documentation of data_processing Java component
        * pythondoc/: documentation of causal_analysis and data_acquisition Python code
    
* experiments/:  scripts to run the performance and causal discovery experiments
        * causal_discovery/: scripts for causal discovery experiments
        * performance/: scripts for performance evaluation experiments
* notebooks/: Jupyter notebooks used for data and causal exploration of the data
* README.md: instructions around installing dependencies, as well as running the pipeline and experiments
