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

### 4. Google Cloud Setup — Create a Service Account JSON Key

The DAGs upload data to GCS and BigQuery, so GCP infrastructure must be provisioned **before** starting the containers.

1. Open https://console.cloud.google.com → **IAM & Admin → Service Accounts**
2. Click **Create Service Account**, enter a name, assign roles (`Storage Admin` + `BigQuery Admin`), click **Done**.
3. Open the account → **Keys** tab → **Add Key → Create new key → JSON**.
4. The key file downloads automatically.

> ⚠️ **Never commit the JSON key to Git.** Store it outside the repository.

---

### 5. Install Terraform

Terraform ≥ 1.0 is required. Choose the method for your platform:

**Linux (Ubuntu / Debian / WSL):**
```bash
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common
wget -O- https://apt.releases.hashicorp.com/gpg | \
  gpg --dearmor | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
  https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
  sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform
```

**macOS (Homebrew):**
```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
```

**Windows PowerShell (Chocolatey):**
```powershell
choco install terraform
```
> If you don't have Chocolatey: https://chocolatey.org/install — or download the binary directly from https://developer.hashicorp.com/terraform/install and add it to your `PATH`.

Verify the installation on all platforms:
```bash
terraform -version
```

---

### 6. Provision Infrastructure with Terraform

Terraform creates a **GCS bucket** (data lake) and a **BigQuery dataset** (data warehouse).

#### Provisioned resources

| Resource | Default name | Location |
|----------|-------------|----------|
| GCS Bucket | `us_accidents_data_lake_bucket20260305` | `EU` |
| BigQuery Dataset | `us_accidents_dataset` | `EU` |

#### Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `credentials` | *(path to JSON key)* | Service account key file |
| `project` | *(your project ID)* | GCP project ID |
| `region` | `europe-west6` | Provider region |
| `location` | `EU` | Resource location |
| `bq_dataset_name` | `us_accidents_dataset` | BigQuery dataset name |
| `gcs_bucket_name` | `us_accidents_data_lake_bucket20260305` | GCS bucket name (must be globally unique, must be changed) |
| `gcs_storage_class` | `STANDARD` | Bucket storage class |

#### Steps

**1. Navigate to the Terraform directory:**
```bash
# Linux / WSL / macOS
cd ../Terraform

# Windows PowerShell
cd ..\Terraform
```

**2. Set your credentials and project ID:**
Edit the `credentials` and `project` defaults directly in `variables.tf`.

**3. Initialise provider plugins:**
```bash
terraform init
```

**4. Preview planned changes:**
```bash
terraform plan
```

**5. Apply the configuration:**
```bash
terraform apply
```
Type `yes` when prompted. Terraform will create the GCS bucket and BigQuery dataset.

**6. Dont forget to tear down the project, when you are done! (avoids ongoing costs):**
```bash
terraform destroy
```
Type `yes` when prompted. The bucket will be force-deleted even if it still contains objects.

> **Note:** `gcs_bucket_name` must be globally unique across all GCP projects. Update the default value in `variables.tf` if the name is already taken.

---

### 7. Configure the environment

Now that `variables.tf` is configured, run the setup script once. It detects your OS, writes a `.env` file with the correct Docker socket permissions and Postgres hostname for your platform, and reads the GCP credentials, project, dataset, and bucket values directly from `variables.tf`.

Navigate back to the Docker directory first:
```bash
cd ../Docker\ Environment   # Linux / WSL / macOS
```
```powershell
cd "..\Docker Environment"  # Windows PowerShell
```

Then run:
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

### 8. Load data and scripts into the Docker volume

Run this one-time setup step to copy the CSV and pipeline scripts into the shared Docker volume:
```
docker compose --profile setup up -d
```

---

### 9. Start the containers

```
docker compose up -d
```

The first startup takes a few minutes. Airflow initialises its database and creates the admin user automatically before the webserver and scheduler start.

---

### 10. Workflow Orchestration (Airflow)

The pipeline is orchestrated using Apache Airflow. The DAG `us_accidents_pipeline` runs the ingestion and transformation steps sequentially and is scheduled to execute daily at 03:00 UTC.

#### 10.1 Open the Airflow UI
* Airflow: http://localhost:8080
  * User: `admin`
  * PW: `admin`

#### 10.2 Trigger a run

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

#### 10.3 Run with a reduced row limit (for testing)
The full dataset contains ~7 million rows and takes several minutes to ingest. For a quick smoke test, limit the number of rows via an Airflow Variable — no code changes required.

**Via the Airflow UI:** Admin → Variables → Add → Key: `ingest_limit` and `upload_limit` , Value: `50000`

| Value | Rows | Approximate duration |
|-------|------|----------------------|
| `1000` | 1k | ~10 seconds |
| `100000` | 100k | ~1 minute |
| *(not set)* | all ~7M | production mode |

To switch back to the full dataset, delete the variable `ingest_limit` and `upload_limit` in airflow.

---

### 11. Verify the pipeline completed successfully
 
The project uses two Airflow DAGs that run sequentially:
 
| DAG | Schedule | Description |
|-----|----------|-------------|
| `us_accidents_pipeline` | 03:00 UTC | Ingests CSV → Postgres, exports to GCS |
| `us_accidents_bq_pipeline` | 06:00 UTC | Loads GCS exports → BigQuery |
 
Check that all tasks in both DAGs show a **dark green** (success) status in the Airflow UI. Then confirm the data is present in all three destinations described below.
 
#### 11.1 Open pgAdmin
* pgAdmin: http://localhost:8085
  * User: `admin@admin.com`
  * PW: `root`
#### 11.2 Add New Server
* **General**
  * Name: `us_accidents`
* **Connection**
  * Host name/address: `pgdatabase`
  * Port: `5432`
  * Maintenance database: `us_accidents`
  * Username: `root`
  * Password: `root`
#### 11.3 Data location in pgAdmin
You can now find the data in the **us_accidents** database:
```
Databases -> us_accidents -> Schemas -> public -> Tables -> accidents
```
Right-click on `accidents` and select **View/Edit Data → First 100 Rows**.
 
#### 11.4 Verify data in GCS (Data Lake)
 
The `upload_to_gcs` task exports the `accidents` table as a timestamped CSV into the `exports/` folder of your bucket. The file follows the naming pattern `accidents_<YYYYMMDD_HHMMSS>.csv`.
 
1. Open https://console.cloud.google.com → **Cloud Storage → Buckets**
2. Navigate to your bucket (e.g. `us_accidents_data_lake_bucket20260305`).
3. Open the `exports/` folder and confirm a file named `accidents_<timestamp>.csv` is present with a non-zero file size.
Alternatively, verify via the `gcloud` CLI:
```bash
gcloud storage ls gs://us_accidents_data_lake_bucket20260305/exports/
```
 
#### 11.5 Verify data in BigQuery (Data Warehouse)
 
The `load_to_bigquery` task creates four tables in the `us_accidents_dataset` dataset:
 
| Table | Description |
|-------|-------------|
| `external_accidents_raw` | External table pointing directly to the GCS CSV(s) |
| `accidents_cleaned` | Native table with correct data types |
| `accidents_partitioned` | Partitioned by `DATE(start_time)` |
| `accidents_partitioned_clustered` | Partitioned by `DATE(start_time)`, clustered by `state`, `severity`, `city` |
 
1. Open https://console.cloud.google.com → **BigQuery**
2. In the Explorer panel, expand your project and navigate to the dataset `us_accidents_dataset`.
3. Confirm all four tables listed above are present.
4. Open `accidents_partitioned_clustered` and click **Preview** to confirm rows are present.
Alternatively, run a quick row count query in the BigQuery editor:
```sql
SELECT COUNT(*) AS row_count
FROM `your_project_id.us_accidents_dataset.accidents_partitioned_clustered`;
```
 Replace `your_project_id` with your actual GCP project ID. A successful pipeline run will return a row count greater than zero.
