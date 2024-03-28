
## Data Processing Pipeline: ETL Exercise 
Christian Grech

This repository contains the Python script and datasets used for a coding exercise evaluating my data processing and ETL development skills.

**Key Features**

* **Reusable Functions:** The script is structured with distinct functions for reading, cleaning, validating, and saving data, promoting modularity and potential reuse.
* **Data Cleaning:**  Implements data type conversions, handling of missing values (forward-filling for stock prices) for two sample datasets:
    * **airline_flights.csv**
    * **big_tech_stock_prices.txt**

**Datasets** 

* **airline_flights.csv:** A sample dataset containing information about airline flights.
* **big_tech_stock_prices.txt:** A sample dataset containing historical stock prices of major tech companies.

**How to Run**

1. **Prerequisites:**  
    * Python 3.x 
    * pandas library (install with `pip install pandas`) 
2. **Download:** Download or clone this repository.
3. **Place Datasets:** Place the `airline_flights.csv` and `big_tech_stock_prices.txt` files in the same directory as the Python script.  
4. **Execute:** Run the script from the command line:
   ```bash
   python etl.py  
    ```
The result is two files called `cleaned_airline_flights.csv` and `cleaned_big_tech_stock_prices.txt`.

**Notes**

* This coding exercise was completed under a time constraint. With more time, I would focus on adding more robust validation checks and additional cleaning steps informed by further data exploration.

**Contact**

If you have questions or feedback, feel free to reach out to me at chrisgre23@gmail.com
 

 
