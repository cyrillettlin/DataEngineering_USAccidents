# DataEngineering_USAccidents

## Group 
- Cyrill Ettlin
- Fabian Müller

## UI-Access
* Airflow: http://localhost:8080
  * User: `admin`
  * PW: `admin`
* pgAdmin: http://localhost:8085
  * User: `admin@admin.com`
  * PW: `root`

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

**Linux / WSL / macOS / Windows PowerShell:** — if you have an SSH key configured with GitHub:
```bash
git clone git@github.com:cyrillettlin/DataEngineering_USAccidents.git
```

If not (or if you get a `Permission denied (publickey)` error), use HTTPS instead:
```bash
git clone https://github.com/cyrillettlin/DataEngineering_USAccidents.git
```

---

### 2. Navigate to the Docker directory

**Linux / WSL / macOS:**
```bash
cd DataEngineering_USAccidents/Docker\ Environment/
```

**Windows PowerShell:**
```powershell
cd "DataEngineering_USAccidents\Docker Environment"
```

---

### 3. Download the dataset

**Linux / WSL / macOS:**
```bash
curl -L -o data/us-accidents.zip \
  https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents
unzip data/us-accidents.zip -d data
```

**Windows PowerShell:**
```powershell
Invoke-WebRequest -Uri "https://www.kaggle.com/api/v1/datasets/download/sobhanmoosavi/us-accidents" `
  -OutFile "data\us-accidents.zip"
Expand-Archive -Path data\us-accidents.zip -DestinationPath data
```

---

### 4. Configure the environment

Run the setup script once. It detects your OS and writes a `.env` file with the correct Docker socket permissions and Postgres hostname for your platform.

**Linux / WSL / macOS / Windows PowerShell:**
```bash
bash setup_env.sh
```

> If you get a `\r: command not found` error on WSL, the file has Windows line endings. Fix with:
> ```bash
> sed -i 's/\r//' setup_env.sh && bash setup_env.sh
> ```

| Platform | DOCKER_GID | PGHOST |
|---|---|---|
| Linux / WSL | GID of `/var/run/docker.sock` | `pgdatabase` |
| Windows / macOS | `0` (not needed) | `host.docker.internal` |

---

### 5. Load data and scripts into the Docker volume

This one-time setup step copies the CSV and pipeline scripts into the shared Docker volume:
```
docker compose --profile setup up -d
```

---

### 6. Start the containers

```
docker compose up -d
```

The first startup takes a few minutes. Airflow initialises its database and creates the admin user automatically before the webserver and scheduler start.

---

### 7. Workflow Orchestration (Airflow)

The pipeline is orchestrated using Apache Airflow. The DAG `us_accidents_pipeline` runs the ingestion and transformation steps sequentially and is scheduled to execute daily at 03:00 UTC.

#### 7.1 Open the Airflow UI
* Airflow: http://localhost:8080
  * User: `admin`
  * PW: `admin`

#### 7.2 Trigger a run

**Option A — Manual run via UI:**
1. Navigate to **DAGs** and find `us_accidents_pipeline`.
2. Enable the DAG using the toggle on the left if it is paused.
3. Click the **Run** button (▶) on the right to trigger a manual execution.
4. Click on the DAG name, then open the **Graph** view to watch the `ingest → transform` tasks execute in sequence.

**Option B — Backfill via CLI:**

**Linux / WSL / macOS / Windows PowerShell:**
```bash
docker compose exec airflow_scheduler \
  airflow dags backfill us_accidents_pipeline \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

#### 7.3 Run with a reduced row limit (for testing)
The full dataset contains ~7 million rows and takes several minutes to ingest. For a quick smoke test, limit the number of rows via an Airflow Variable — no code changes required.

```
docker compose exec airflow_scheduler airflow variables set ingest_limit 1000
```

**Or via the Airflow UI:** Admin → Variables → Add → Key: `ingest_limit`, Value: `1000`

| Value | Rows | Approximate duration |
|-------|------|----------------------|
| `1000` | 1k | ~10 seconds |
| `100000` | 100k | ~1 minute |
| *(not set)* | all ~7M | production mode |

To switch back to the full dataset, delete the variable:
```
docker compose exec airflow_scheduler airflow variables delete ingest_limit
```

---

### 8. Verify the pipeline completed successfully

Check that both tasks show a **dark green** (success) status in the Airflow UI. Then confirm the data is present in pgAdmin.

#### 8.1 Open pgAdmin
* pgAdmin: http://localhost:8085
  * User: `admin@admin.com`
  * PW: `root`

#### 8.2 Add New Server
* **General**
  * Name: `us_accidents`
* **Connection**
  * Host name/address: `pgdatabase`
  * Port: `5432`
  * Maintenance database: `us_accidents`
  * Username: `root`
  * Password: `root`

#### 8.3 Data location
You can now find the data in the **us_accidents** database:
```
Databases -> us_accidents -> Schemas -> public -> Tables -> accidents
```
Right-click on `accidents` and select **View/Edit Data → First 100 Rows**.