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
    if filepath.endswith(".csv") or filepath.endswith(".txt"): # Read dataset if the end of the filepath is .csv or .txt
        try:
            data = pd.read_csv(filepath)
            print('--> Opening', filepath)
        except:
            raise Exception("File not found.")  # Exception is raised when the file is not found
    else:
        raise Exception("Unsupported file format") # Exception is raised when the file format is not one of the two supported.
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
        print('--> Validating')
        return validate_airline_flights(data)
    elif "stock_symbol" in data.columns:
        print('--> Validating')
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
    # Check numerical fields and convert where possible
    data[['Revenue ($)', 'Distance (km)', 'Flight Number', 'Passengers (First Class)', 'Passengers (Business Class)', 'Passengers (Economy Class)']] = data[['Revenue ($)', 'Distance (km)', 'Flight Number', 'Passengers (First Class)', 'Passengers (Business Class)', 'Passengers (Economy Class)']].apply(pd.to_numeric, errors='coerce')
    airline_missing = data.isnull().sum()
    
    # Count NaN data
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
    # Validate numerical fields, converting other data types in those fields to NaN
    data[['open', 'high', 'low', 'close', 'adj_close', 'volume']] = data[['open', 'high', 'low', 'close', 'adj_close', 'volume']].apply(pd.to_numeric, errors='coerce')

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
        print('--> Cleaning')
        return clean_airline_flights(data)
    elif "stock_symbol" in data.columns:
        print('--> Cleaning')
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
    # Remove NaN data
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
        data.to_csv(filepath, index=False) # Save data to CSV
        print('--> Saving data')
    except:
        raise Exception("Cannot save data to CSV")
        
        
# Define the data processing pipeline steps
def data_processing_pipeline(filepath):
    """
    Performs the data processing pipeline steps: read, validate, clean and save.

    Args:
        filepath (str): Path to the data file.
            
    """
    data = read_data(filepath)    # Step 1: Read data
    validated_data = validate_data(data) # Step 2: Validate data
    if validated_data: 
        cleaned_data = clean_data(data) # Step 3: Clean data
        save_data(cleaned_data, "cleaned_" + filepath) # Step 4: Save data
        print(f"--> Saved data at cleaned_{filepath}")
    else:
        print(f"--> Data validation failed for {filepath}")
         
            
#Execute the ETL for the two datasets
data_processing_pipeline("big_tech_stock_prices.txt")
data_processing_pipeline("airline_flights.csv")