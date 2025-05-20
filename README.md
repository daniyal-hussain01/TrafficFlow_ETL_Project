# Traffic Flow ETL & Data Warehouse Project

This project focuses on building an ETL pipeline and a star schema data warehouse for analyzing traffic accident data. It was implemented using Python and SQLite, with source data from Excel files and a strong emphasis on data transformation and quality.

## 🚦 Objective
To help traffic authorities identify accident patterns, severity trends, and high-risk areas by analyzing historical accident data across key dimensions such as location, vehicle type, road condition, and date.

## 📁 Project Structure
- `etl_pipeline.py` — Main ETL pipeline that extracts data from Excel, transforms it into dimension and fact tables, and loads it into SQLite.
- `generate_snapshots.py` — Displays the first 10 rows of each table after loading.
- `queries.sql` — Contains SQL queries for analysis such as top accident locations, severity trends, etc.
- `accident_data_warehouse.db` — Final SQLite database containing the star schema.
- `Traffic.xlsx` — Raw data source including accidents, vehicles, and road conditions.
- `TrafficFlow_ETL_Report.pdf` — Detailed project report covering steps, schema, assumptions, and issues.
- `Traffic.FlowERD.pdf` — ERD diagram used for schema planning.
- `SnapshotsOfTables.pdf` & `QueriesSnapshots.pdf` — Visual previews of table content and query results.
- `ETL PSEUDOCODE.pdf` — Step-by-step pseudocode of the ETL logic.
- `DWH.Design.Template.xlsx` — Design planning sheet for dimensions and fact table.
- `requirements.txt` — Required libraries (e.g., pandas, sqlalchemy).

## 🧱 Star Schema Overview
**Fact Table:**
- `Fact_Accidents`: AccidentID, DateID, LocationID, VehicleID, RoadConditionID, VehiclesInvolved, SeverityScore

**Dimension Tables:**
- `Dim_Date`: DateID, Date, Month, Year, Time
- `Dim_Location`: LocationID, LocationName
- `Dim_Vehicle`: VehicleID, VehicleType
- `Dim_RoadCondition`: RoadConditionID, Surface, Visibility

## 🧪 Features & Analysis
- Mapping of severity levels (Minor, Moderate, Severe) into numerical scores
- Data snapshots and visual previews
- SQL queries to analyze:
  - Most accident-prone locations
  - Severity trends by road condition
  - Monthly accident frequency

## 🛠 Tools & Technologies
- **Python** (pandas, sqlalchemy)
- **SQLite**
- **Microsoft Excel**
- **ERD & Data Templates**

## 📊 Final Stats
- 100 rows in `Fact_Accidents`
- 100–200 rows in each dimension
- ETL pipeline handles missing data and unclear mappings with documented assumptions

## 📘 Report Highlights
- Discusses ERD issues, sample data problems, and assumptions made
- Includes SQL-based analysis and screenshot results
- Reflects hands-on experience with end-to-end ETL and warehouse design

---

### 👨‍💻 Author
**Syed Muhammad Meesum Abbas**  
**Syed Daniyal Hussain** 
*MS Data Science, Institute of Business Administration*  
Project completed as part of Data Warehouse coursework.

