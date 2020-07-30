# Example ETL with Python, Spark and Airflow

### Project scope

This project demonstrates skills in data engineering, specifically in creating an efficient ETL process utilizing AWS EC2, EMR and S3, Python and Spark and orcehstrating the data pipeline with Airflow.  The output of the pipeline is a star schema that can be utilized by Business Intelligence users, Analysts and Data Scientists allowing for efficient analysis of the data, minimizing required storage space and the need for complex joins.  

Specifically the ETL pipeline ingests data related to US immigration from the following sources:

**I94 Immigration Data**: This data comes from the US National Tourism and Trade Office and contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).  The original source for the data is [here](https://travel.trade.gov/research/reports/i94/historical/2016.html)

**U.S. City Demographic Data:**: Demographic data for world cities.  The data can be found [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)  

**Airport Code Table**:  Dataset containing airport codes and information on the airport such as country, municipality, lattitude and longitude.  The data can be found [here](https://datahub.io/core/airport-codes#data)

### Infrastructure set-up



![](Images/Timeline.PNG)
