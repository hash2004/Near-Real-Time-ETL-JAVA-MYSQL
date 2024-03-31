# Near-Real-Time-ETL-Java-MySQL
This project encompasses the development and execution of an ETL (Extract, Transform, Load) process, using Java, threads, and MySQL.

## Project Overview
This project encompasses the development and execution of an ETL (Extract, Transform, Load) process, alongside the setup and utilization of a data warehouse environment. It involves extracting data from transactional and master data sources, transforming this data through a hybrid join process, and loading the integrated data into a data warehouse for OLAP (Online Analytical Processing) queries.

## Getting Started

### Prerequisites
- MySQL Database
- Java JDK
- IntelliJ IDEA or similar Java IDE
- JDBC Connector

### Setup Instructions
1. **Database Preparation:**
   - Download the `transactions.csv` and `master_data.csv` files.
   - Create a MySQL database named `Transactions`.
   - Import the CSV files into the database to create the transactional and master data tables.

2. **Data Warehouse Creation:**
   - Execute the SQL script provided in `electronica_dwh.sql` to create the data warehouse schema.

3. **ETL Process:**
   - Open a new project in IntelliJ IDEA.
   - Add the JDBC connector JAR files to the project's library.
   - Run the `ETL_v1.java` or `ETL_v2.java` file to start the ETL process.

4. **OLAP Queries:**
   - Execute the OLAP queries provided in `OLAP.sql` to analyze the data within the data warehouse.

### Files Description
- `ETL_v1.java` and `ETL_v2.java`: Java programs implementing the ETL logic.
- `electronica_dwh.sql`: SQL script for creating the data warehouse schema.
- `loading.sql`: SQL script for loading data into the data warehouse.
- `OLAP.sql`: Contains OLAP queries for data analysis.

## System Architecture

### ETL Process
- **Extraction:** Data is extracted from the `cleaned_transactions` and `master_data_cleaned` tables.
- **Transformation:** A hybrid join operation is performed on the extracted data.
- **Loading:** The transformed data is loaded into the `OrderFacts` table and various dimension tables in the data warehouse.

### Components
- `TransactionStream`: Manages the continuous extraction of transactional data.
- `MasterDataStream`: Handles the extraction of master data.
- `HybridJoin`: Performs the transformation by joining transactional and master data and loads them into the data warehouse.

## OLAP Analysis
- Executes complex analytical queries, including drill-down, roll-up, and slicing operations on the data warehouse to derive business insights.

## Challenges and Learnings
- Handling real-time data extraction and transformation with concurrent Java threads.
- Implementing a robust and efficient join mechanism in the ETL process.
- Designing and querying a multi-dimensional data warehouse to support OLAP.

## License
This project is open-source and available under the MIT License.

