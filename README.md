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
### Sample Data (Raw)

| ID  | Source  | Severity | Start_Time           | End_Time             | Start_Lat | Start_Lng  | Distance(mi) | City         | State | Temperature(F) | Visibility(mi) | Weather_Condition | Traffic_Signal | Sunrise_Sunset |
|-----|--------|----------|----------------------|----------------------|-----------|------------|--------------|--------------|-------|----------------|----------------|-------------------|----------------|----------------|
| A-1 | Source2 | 3        | 2016-02-08 05:46:00 | 2016-02-08 11:00:00 | 39.865147 | -84.058723 | 0.01         | Dayton       | OH    | 36.9           | 10.0           | Light Rain        | False          | Night          |
| A-2 | Source2 | 2        | 2016-02-08 06:07:59 | 2016-02-08 06:37:59 | 39.928059 | -82.831184 | 0.01         | Reynoldsburg | OH    | 37.9           |                |                   |                |                |

### Source
- https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents

## Use case
The goal of this project is to improve road safety in the United States by analyzing historical traffic accident data. By examining previous accidents, the analysis aims to identify patterns and key factors that contribute to severe or fatal crashes. Factors such as weather conditions, time of day, road characteristics, and location will be evaluated to determine under which circumstances accidents are most likely to result in fatalities. The insights gained from this analysis can support data-driven decisions by authorities to implement targeted safety measures, improve infrastructure, and raise public awareness, ultimately helping to reduce accident severity and fatality rates and make the streets safer for all road users.

### Persona
Peter, a senior data analyst at the Department of Motor Vehicles (DMV), is responsible for analyzing traffic accident data to improve road safety in the United States. He aims to create visualizations and analytical dashboards that help reveal patterns in historical accident data. By analyzing factors such as weather conditions, time of day, road characteristics, and location, Peter seeks to identify the key factors that contribute to severe or fatal accidents. The insights generated from these visualizations help policymakers and transportation authorities better understand accident risks and implement measures to reduce fatalities and make streets safer.


## Transformation
### Format standardisation
The transformation standardises measurement units to ensure consistency across the dataset. In the raw data, distance and visibility are stored in miles (`distance_mi`, `visibility_mi`). These values are converted into kilometres (`distance_km`, `visibility_km`) using the factor 1.60934 and rounded to three decimal places.
Additionally, temperature and wind chill values are converted from Fahrenheit (`temperature_f`, `wind_chill_f`) to Celsius (`temperature_c`, `wind_chill_c`) using the standard formula.
This ensures that all physical measurements are aligned with the metric system, improving consistency and making the dataset easier to use in international contexts.

### Column engineering
Several new columns are derived from existing timestamp fields to improve analytical usability. From `start_time`, `end_time`, and `weather_timestamp`, the following components are extracted:
- year
- month
- day
- hour
- minute


  This allows efficient time-based analysis (e.g. accidents per hour or month) without repeatedly applying SQL extraction functions.

### Sample Data (Transformed)

| ID  | Severity | Start_Year | Start_Month | Start_Hour | Distance(km) | Visibility(km) | Temperature(°C) | Wind_Chill(°C) | City         | State | Weather_Condition |
|-----|----------|-----------|-------------|------------|--------------|----------------|-----------------|----------------|--------------|-------|-------------------|
| A-1 | 3        | 2016      | 2           | 5          | 0.016        | 16.093         | 2.72            |                | Dayton       | OH    | Light Rain        |
| A-2 | 2        | 2016      | 2           | 6          | 0.016        |                | 3.28            |                | Reynoldsburg | OH    |                   |

## Installation
### 1. Clone Repository
Clone the repository:
```git clone git@github.com:cyrillettlin/DataEngineering_USAccidents.git```

### 2. Download the data
Go to the Docker setup:
```cd DataEngineering_USAccidents/Docker\ Environment/```

Download the Data:
```
curl -L -o data/us-accidents.zip https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents && \
unzip data/us-accidents.zip -d data
```

### 3. Start the containers
Download and start the docker containers:
```docker compose up```

### 4. How to verify the System works