import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text  # For wrapping raw SQL statements in SQLAlchemy
import random
import logging
import os

# Initialize logging to both file and console for debugging and auditing
logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO to capture all relevant events
    format='%(asctime)s - %(levelname)s - %(message)s',  # Include timestamp, level, and message
    handlers=[
        logging.FileHandler('etl_pipeline.log'),  # Log to a file for persistent records
        logging.StreamHandler()  # Also log to console for immediate feedback
    ]
)

# --- Data Loading ---

def load_excel_file(file_path, required_sheets):
    """
    Load an Excel file and verify that all required sheets are present.

    Args:
        file_path (str): Path to the Excel file.
        required_sheets (list): List of required sheet names.

    Returns:
        dict: Dictionary mapping sheet names to their respective DataFrames.

    Raises:
        FileNotFoundError: If the Excel file does not exist.
        ValueError: If any required sheets are missing.
    """
    try:
        # Check if the Excel file exists
        if not os.path.exists(file_path):
            logging.error(f"Excel file not found: {file_path}")
            raise FileNotFoundError(f"Excel file not found: {file_path}")
        
        # Load the Excel file and check for required sheets
        excel_data = pd.ExcelFile(file_path)
        missing_sheets = [sheet for sheet in required_sheets if sheet not in excel_data.sheet_names]
        if missing_sheets:
            logging.error(f"Missing required sheets: {missing_sheets}")
            raise ValueError(f"Missing required sheets: {missing_sheets}")
        
        # Load each required sheet into a DataFrame
        data = {sheet: pd.read_excel(file_path, sheet_name=sheet) for sheet in required_sheets}
        logging.info("Excel file loaded successfully with all required sheets.")
        return data
    except Exception as e:
        logging.error(f"Error loading Excel file: {str(e)}")
        raise

# --- Data Transformation Utilities ---

def convert_timestamps(df, column):
    """
    Convert a specified column in a DataFrame to datetime format.

    Args:
        df (pd.DataFrame): DataFrame containing the column to convert.
        column (str): Name of the column to convert to datetime.

    Returns:
        pd.DataFrame: DataFrame with the converted column.

    Raises:
        Exception: If the conversion fails.
    """
    try:
        # Convert the column to datetime, coercing invalid values to NaT
        df[column] = pd.to_datetime(df[column], errors='coerce')
        logging.info(f"Converted {column} to datetime in DataFrame.")
        return df
    except Exception as e:
        logging.error(f"Error converting timestamps in {column}: {str(e)}")
        raise

# --- Database Setup ---

def create_tables(engine):
    """
    Create the database tables for the star schema in the SQLite database.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Raises:
        Exception: If table creation fails.
    """
    try:
        with engine.connect() as conn:
            # Enable foreign key enforcement in SQLite (not enabled by default)
            conn.execute(text("PRAGMA foreign_keys = ON"))

            # Create Dim_Location table with LocationID as primary key and unique LocationName
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS Dim_Location (
                    LocationID INTEGER PRIMARY KEY,
                    LocationName VARCHAR(255) UNIQUE
                )
            '''))

            # Create Dim_Vehicle table with VehicleID as primary key
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS Dim_Vehicle (
                    VehicleID INTEGER PRIMARY KEY,
                    VehicleType VARCHAR(100)
                )
            '''))

            # Create Dim_RoadCondition table with RoadConditionID as primary key
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS Dim_RoadCondition (
                    RoadConditionID INTEGER PRIMARY KEY,
                    Surface VARCHAR(50),
                    Visibility VARCHAR(50)
                )
            '''))

            # Create Dim_Date table with DateID as primary key
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS Dim_Date (
                    DateID INTEGER PRIMARY KEY,
                    Date DATE,
                    Month INTEGER,
                    Year INTEGER,
                    Time TIMESTAMP
                )
            '''))

            # Create Fact_Accidents table with foreign keys to dimension tables
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS Fact_Accidents (
                    AccidentID INTEGER PRIMARY KEY,
                    DateID INTEGER,
                    LocationID INTEGER,
                    VehicleID INTEGER,
                    RoadConditionID INTEGER,
                    VehiclesInvolved INTEGER,
                    SeverityScore INTEGER,
                    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID),
                    FOREIGN KEY (LocationID) REFERENCES Dim_Location(LocationID),
                    FOREIGN KEY (VehicleID) REFERENCES Dim_Vehicle(VehicleID),
                    FOREIGN KEY (RoadConditionID) REFERENCES Dim_RoadCondition(RoadConditionID)
                )
            '''))

        logging.info("Database tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating database tables: {str(e)}")
        raise

# --- Dimension Table Population ---

def populate_dim_location(accidents_df, engine):
    """
    Populate the Dim_Location table with unique locations from the Accidents data.

    Args:
        accidents_df (pd.DataFrame): DataFrame containing accident data.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: Populated Dim_Location DataFrame.

    Raises:
        Exception: If population fails.
    """
    try:
        # Extract unique locations and assign surrogate keys
        dim_location_df = pd.DataFrame({
            'LocationID': range(1, len(accidents_df['Location'].unique()) + 1),
            'LocationName': accidents_df['Location'].unique()
        })
        # Load the DataFrame into the Dim_Location table
        dim_location_df.to_sql('Dim_Location', engine, if_exists='replace', index=False)
        logging.info("Populated Dim_Location with %d rows.", len(dim_location_df))
        return dim_location_df
    except Exception as e:
        logging.error(f"Error populating Dim_Location: {str(e)}")
        raise

def populate_dim_vehicle(vehicles_df, engine):
    """
    Populate the Dim_Vehicle table with vehicle data.

    Args:
        vehicles_df (pd.DataFrame): DataFrame containing vehicle data.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: Populated Dim_Vehicle DataFrame.

    Raises:
        Exception: If population fails.
    """
    try:
        # Select relevant columns and remove rows with missing values
        dim_vehicle_df = vehicles_df[['VehicleID', 'VehicleType']].dropna()
        # Load the DataFrame into the Dim_Vehicle table
        dim_vehicle_df.to_sql('Dim_Vehicle', engine, if_exists='replace', index=False)
        logging.info("Populated Dim_Vehicle with %d rows.", len(dim_vehicle_df))
        return dim_vehicle_df
    except Exception as e:
        logging.error(f"Error populating Dim_Vehicle: {str(e)}")
        raise

def populate_dim_road_condition(road_conditions_df, engine):
    """
    Populate the Dim_RoadCondition table with road condition data.

    Args:
        road_conditions_df (pd.DataFrame): DataFrame containing road condition data.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: Populated Dim_RoadCondition DataFrame.

    Raises:
        Exception: If population fails.
    """
    try:
        # Select relevant columns and remove rows with missing values
        dim_road_condition_df = road_conditions_df[['ConditionID', 'Surface', 'Visibility']].dropna()
        # Load the DataFrame into the Dim_RoadCondition table
        dim_road_condition_df.to_sql('Dim_RoadCondition', engine, if_exists='replace', index=False)
        logging.info("Populated Dim_RoadCondition with %d rows.", len(dim_road_condition_df))
        return dim_road_condition_df
    except Exception as e:
        logging.error(f"Error populating Dim_RoadCondition: {str(e)}")
        raise

def populate_dim_date(accidents_df, engine):
    """
    Populate the Dim_Date table with unique dates from the Accidents data.

    Args:
        accidents_df (pd.DataFrame): DataFrame containing accident data.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: Populated Dim_Date DataFrame.

    Raises:
        Exception: If population fails.
    """
    try:
        # Extract unique dates (not timestamps) and create date components
        # Keep the full datetime, but we'll group by date to get unique dates
        unique_dates = pd.to_datetime(accidents_df['ReportedAt']).dt.date.drop_duplicates()
        # Convert back to datetime64 for proper .dt accessor usage
        unique_dates_dt = pd.to_datetime(unique_dates)
        dim_date_df = pd.DataFrame({
            'DateID': range(1, len(unique_dates) + 1),
            'Date': unique_dates,
            'Month': unique_dates_dt.dt.month,
            'Year': unique_dates_dt.dt.year,
            'Time': unique_dates_dt  # Use the datetime as the Time column
        })
        # Load the DataFrame into the Dim_Date table
        dim_date_df.to_sql('Dim_Date', engine, if_exists='replace', index=False)
        logging.info("Populated Dim_Date with %d rows.", len(dim_date_df))
        return dim_date_df
    except Exception as e:
        logging.error(f"Error populating Dim_Date: {str(e)}")
        raise

# --- Fact Table Population ---

def populate_fact_accidents(accidents_df, dim_location_df, dim_date_df, road_conditions_df, engine):
    """
    Populate the Fact_Accidents table by joining Accidents data with dimension tables.

    Args:
        accidents_df (pd.DataFrame): DataFrame containing accident data.
        dim_location_df (pd.DataFrame): Dim_Location DataFrame.
        dim_date_df (pd.DataFrame): Dim_Date DataFrame.
        road_conditions_df (pd.DataFrame): DataFrame containing road condition data.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Raises:
        Exception: If population fails.
    """
    try:
        # Debug: Check for unmatched locations between Accidents and Conditions
        accident_locations = set(accidents_df['Location'].unique())
        condition_locations = set(road_conditions_df['Location'].unique())
        unmatched_locations = accident_locations - condition_locations
        if unmatched_locations:
            logging.warning(f"Locations in Accidents not found in Conditions: {unmatched_locations}")

        # Precompute DateID by merging with Dim_Date
        accidents_df['ReportedAt_Date'] = accidents_df['ReportedAt'].dt.date
        # Do not convert dim_date_df['Date'] to string; keep it as datetime.date
        accidents_with_date = accidents_df.merge(
            dim_date_df[['DateID', 'Date']],
            left_on='ReportedAt_Date',
            right_on='Date',
            how='left'
        )
        # Log any unmatched dates for debugging
        unmatched_dates = accidents_with_date[accidents_with_date['DateID'].isnull()]['ReportedAt_Date'].unique()
        if len(unmatched_dates) > 0:
            logging.warning(f"Unmatched dates in Fact_Accidents: {unmatched_dates}")

        # Precompute LocationID by merging with Dim_Location
        accidents_with_location = accidents_with_date.merge(
            dim_location_df[['LocationID', 'LocationName']],
            left_on='Location',
            right_on='LocationName',
            how='left'
        )

        # Group by AccidentID to avoid duplicates, taking the first match
        accidents_with_location = accidents_with_location.groupby('AccidentID').first().reset_index()

        # Build Fact_Accidents table row by row
        fact_accidents_data = []
        for _, row in accidents_with_location.iterrows():
            # Skip rows with missing critical data
            if pd.isnull(row['ReportedAt']) or pd.isnull(row['Location']):
                logging.warning(f"Skipping row with AccidentID {row['AccidentID']} due to missing ReportedAt or Location.")
                continue

            # Extract foreign keys
            date_id = row['DateID']
            location_id = row['LocationID']
            vehicle_id = random.randint(1, 200)  # Assign a random VehicleID as per pseudocode

            # Match RoadConditionID by Location and timestamp proximity
            reported_at = row['ReportedAt']
            accident_location = row['Location']
            # Filter road conditions for the same location
            matching_conditions = road_conditions_df[road_conditions_df['Location'] == accident_location]
            if not matching_conditions.empty:
                # Find the road condition with the closest timestamp
                time_diffs = (matching_conditions['RecordedAt'] - reported_at).abs()
                closest_match = matching_conditions.iloc[time_diffs.idxmin()]
                road_condition_id = closest_match['ConditionID']
            else:
                # If no match, assign a random RoadConditionID
                road_condition_id = random.randint(1, 100)
                logging.info(f"No road condition found for date {reported_at.date()} at location {accident_location}, using random RoadConditionID: {road_condition_id}")

            # Map Severity to SeverityScore
            severity_map = {'Minor': 1, 'Moderate': 2, 'Severe': 3}
            severity_score = severity_map.get(row['Severity'], 1)  # Default to 1 if Severity is invalid

            # Append the row to the fact table data
            fact_accidents_data.append({
                'AccidentID': row['AccidentID'],
                'DateID': date_id,
                'LocationID': location_id,
                'VehicleID': vehicle_id,
                'RoadConditionID': road_condition_id,
                'VehiclesInvolved': row['VehiclesInvolved'],
                'SeverityScore': severity_score
            })

        # Convert the list of dictionaries to a DataFrame and load into Fact_Accidents
        fact_accidents_df = pd.DataFrame(fact_accidents_data)
        fact_accidents_df.to_sql('Fact_Accidents', engine, if_exists='replace', index=False)
        logging.info("Populated Fact_Accidents with %d rows.", len(fact_accidents_df))
    except Exception as e:
        logging.error(f"Error populating Fact_Accidents: {str(e)}")
        raise

# --- Validation ---

def validate_database(engine):
    """
    Validate the database by checking row counts and sampling Fact_Accidents.

    Args:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database connection.

    Raises:
        Exception: If validation fails.
    """
    try:
        with engine.connect() as conn:
            # Check the row count for each table
            for table in ['Dim_Location', 'Dim_Vehicle', 'Dim_RoadCondition', 'Dim_Date', 'Fact_Accidents']:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()[0]
                logging.info(f"{table}: {count} rows")
            
            # Sample 10 rows from Fact_Accidents for verification
            sample = pd.read_sql("SELECT * FROM Fact_Accidents LIMIT 10", conn)
            logging.info("\nSample from Fact_Accidents:\n%s", sample.to_string())
    except Exception as e:
        logging.error(f"Error during validation: {str(e)}")
        raise

# --- Main ETL Pipeline ---

def run_etl_pipeline():
    """
    Run the complete ETL pipeline to build the Accident Analysis data warehouse.

    Steps:
        1. Load data from Excel file.
        2. Convert timestamps to datetime.
        3. Create database tables.
        4. Populate dimension tables.
        5. Populate fact table.
        6. Validate the database.
    """
    # Define input file and required sheets
    excel_file = 'Traffic.xlsx'
    required_sheets = ['Vehicles', 'Accidents', 'RoadConditions']

    # Step 1: Load data from Excel
    data = load_excel_file(excel_file, required_sheets)
    vehicles_df = data['Vehicles']
    accidents_df = data['Accidents']
    road_conditions_df = data['RoadConditions']

    # Step 2: Convert timestamps to datetime format
    accidents_df = convert_timestamps(accidents_df, 'ReportedAt')
    road_conditions_df = convert_timestamps(road_conditions_df, 'RecordedAt')

    # Step 3: Create database connection using SQLAlchemy
    engine = create_engine('sqlite:///accident_data_warehouse.db', echo=False)
    logging.info("Connected to SQLite database.")

    # Step 4: Create the star schema tables
    create_tables(engine)

    # Step 5: Populate dimension tables
    dim_location_df = populate_dim_location(accidents_df, engine)
    dim_vehicle_df = populate_dim_vehicle(vehicles_df, engine)
    dim_road_condition_df = populate_dim_road_condition(road_conditions_df, engine)
    dim_date_df = populate_dim_date(accidents_df, engine)

    # Step 6: Populate the fact table
    populate_fact_accidents(accidents_df, dim_location_df, dim_date_df, road_conditions_df, engine)

    # Step 7: Validate the database
    validate_database(engine)

    logging.info("ETL pipeline completed successfully.")

# --- Entry Point ---

if __name__ == "__main__":
    """
    Main entry point for the ETL pipeline script.
    Wraps the pipeline in a try-except block to catch and log any unhandled errors.
    """
    try:
        run_etl_pipeline()
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        raise