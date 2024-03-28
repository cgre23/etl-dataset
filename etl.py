import pandas as pd

# Step 1: Read the Data
def read_data(filepath):
    """
    Reads data from a CSV or TXT file based on the file extension. Assuming a comma delimiter in both cases
    Args:
        filepath (str): Path to the data file.

    Returns:
        pd.DataFrame: The loaded data as a DataFrame.
    """
    if filepath.endswith(".csv") or filepath.endswith(".txt"):
        try:
            data = pd.read_csv(filepath)
        except:
            raise Exception("File not found.")
    else:
        raise Exception("Unsupported file format")
    return data

# Step 2: Data Validation
def validate_data(data):
    """
    Validates the data by checking missing values, outliers

    Args:
        data (pd.DataFrame): The data to be cleaned.

    Returns:
        pd.DataFrame: The cleaned data.
    """
    if "Airline" in data.columns:  # Assuming a column identifies the dataset
        return validate_airline_flights(data)
    elif "stock_symbol" in data.columns:
        return validate_stock_prices(data)
    else:
        raise Exception("Unsupported dataset")

def validate_airline_flights(data):
    """
    Validates the airline data to ensure it meets specific requirements.

    Args:
        data (pd.DataFrame): The data to be validated.

    Returns:
        bool: True if data is valid, False otherwise.
    """
    # Check for missing values
    airline_missing = data.isnull().sum()
    # Count and remove NaN data
    row_nan_count = data.isnull().any(axis=1).sum()
    print('Total row count: ', len(data))
    print('Rows with missing data', row_nan_count)
    print('Missing values in airline data:\n', airline_missing)
    if row_nan_count > (0.9*len(data)):
        return False
    return True
    
def validate_stock_prices(data):
    """
    Validates the stock data to ensure it meets specific requirements.

    Args:
        data (pd.DataFrame): The data to be validated.

    Returns:
        bool: True if data is valid, False otherwise.
    """
    # Check for missing values
    stock_missing = data.isnull().sum()
    row_nan_count = data.isnull().any(axis=1).sum()
    print('Total row count: ', len(data))
    print('Rows with missing data', row_nan_count)
    print('Missing values in stock data:\n', stock_missing)
    if row_nan_count > (0.9*len(data)):
        return False
    return True


#Step 3: Data Cleaning
def clean_data(data):
    """
    Cleans the data by handling missing values, outliers, and data type conversions.

    Args:
        data (pd.DataFrame): The data to be cleaned.

    Returns:
        pd.DataFrame: The cleaned data.
    """
    if "Airline" in data.columns:  # Assuming a column identifies the dataset
        return clean_airline_flights(data)
    elif "stock_symbol" in data.columns:
        return clean_stock_prices(data)
    else:
        raise Exception("Unsupported dataset")


def clean_airline_flights(data):
    """
    Cleans the airline flights dataset.

    Args:
        data (pd.DataFrame): The airline flights data to be cleaned.

    Returns:
        pd.DataFrame: The cleaned airline flights data.
    """
    clean_data = data.copy()
    clean_data = clean_data.dropna()
    
    # Convert data types
    try:
        clean_data['Date'] = pd.to_datetime(clean_data['Date'])
        clean_data[['Revenue ($)', 'Distance (km)']] = clean_data[['Revenue ($)', 'Distance (km)']].astype(float)
        clean_data[['Flight Number','Passengers (First Class)', 'Passengers (Business Class)', 'Passengers (Economy Class)']] = clean_data[['Flight Number','Passengers (First Class)', 'Passengers (Business Class)', 'Passengers (Economy Class)']].astype(int, errors='ignore')
        clean_data['Origin'] = clean_data['Origin'].str.upper()  # Assuming origin is a string column
        clean_data['Destination'] = clean_data['Destination'].str.upper()  # Assuming destination is a string column
        clean_data['Departure Gate'] = clean_data['Departure Gate'].str.upper()  # Assuming aircraft type is a string column
        clean_data['Arrival Gate'] = clean_data['Arrival Gate'].str.upper()  # Assuming aircraft type is a string column
        clean_data['Aircraft Type'] = clean_data['Aircraft Type'].astype(str)
        clean_data['Pod'] = clean_data['Pod'].str.upper() 
    except:
        raise Exception("One or more columns may have incorrectly formatted data.")
    return clean_data

def clean_stock_prices(data):
    """ Cleans the big tech stock prices dataset.

    Args:
        data (pd.DataFrame): The stock prices data to be cleaned.

    Returns:
        pd.DataFrame: The cleaned stock prices data.
    """
    clean_data = data.copy()
    clean_data = clean_data.dropna()
    # Handling Missing Values
    clean_data.fillna(method='ffill', inplace=True)  # Forward-fill to propagate last valid observation

    # Convert data types
    clean_data['date'] = pd.to_datetime(clean_data['date'])
    clean_data[['open', 'high', 'low', 'close', 'adj_close']] = clean_data[['open', 'high', 'low', 'close', 'adj_close']].astype(float)
    clean_data['volume'] = clean_data['volume'].astype(int) 
    return clean_data


# Step 4: Save data
def save_data(data, filepath):
    """
    Saves the data to a CSV format.

    Args:
      data (pd.DataFrame): The data to be saved.
      filepath (str): Path to the output file.
    """
    try:
        data.to_csv(filepath, index=False)
    except:
        raise Exception("Cannot save data to CSV")
        
        
# Define the data processing pipeline steps
def data_processing_pipeline(filepath):
    """
    Performs the data processing pipeline steps: read, validate, clean and save.

    Args:
        filepath (str): Path to the data file.
            
    """
    data = read_data(filepath)
    validated_data = validate_data(data)
    if validated_data:
        cleaned_data = clean_data(data)
        save_data(cleaned_data, "cleaned_" + filepath)
        print(f"Saved data at cleaned_{filepath}")
    else:
        print(f"Data validation failed for {filepath}")
         
            
#Execute the ETL for the two datasets

data_processing_pipeline("big_tech_stock_prices.txt")
data_processing_pipeline("airline_flights.csv")