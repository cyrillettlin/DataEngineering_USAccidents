# DataEngineering_USAccidents

## Group 
- Cyrill Ettlin
- Fabian Müller

## UI-Access
* Kestra: http://localhost:8080
  * User: ```admin@kestra.io```
  * PW: ```Admin1234!```
* pgAdmin: http://localhost:8085
  * User: ```admin@admin.com```
  * PW: ```root```

## Dataset
- https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

### Use case

The goal of this project is to improve road safety in the United States by analyzing historical traffic accident data. By examining previous accidents, the analysis aims to identify patterns and key factors that contribute to severe or fatal crashes. Factors such as weather conditions, time of day, road characteristics, and location will be evaluated to determine under which circumstances accidents are most likely to result in fatalities. The insights gained from this analysis can support data-driven decisions by authorities to implement targeted safety measures, improve infrastructure, and raise public awareness, ultimately helping to reduce accident severity and fatality rates and make the streets safer for all road users.

### Persona

Peter, a senior data analyst at the Department of Motor Vehicles (DMV), is responsible for analyzing traffic accident data to improve road safety in the United States. He aims to create visualizations and analytical dashboards that help reveal patterns in historical accident data. By analyzing factors such as weather conditions, time of day, road characteristics, and location, Peter seeks to identify the key factors that contribute to severe or fatal accidents. The insights generated from these visualizations help policymakers and transportation authorities better understand accident risks and implement measures to reduce fatalities and make streets safer.

# Installation

## 1. Clone Repository
Clone the repository:
```git clone git@github.com:cyrillettlin/DataEngineering_USAccidents.git```

## 2. Download the data
Go to the Docker setup:
```cd DataEngineering_USAccidents/Docker\ Environment/```

Download the Data:
```
curl -L -o data/us-accidents.zip https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents && \
unzip data/us-accidents.zip -d data
```

## 3. Start the containers
Download and start the docker containers:
```docker compose up```