# Dmart-analysis-using-pyspark

# PySpark Data Pipeline Project for Dmart

## Project Overview
This project focuses on building a scalable data pipeline using PySpark to process and analyze Dmart sales data. The pipeline integrates data from multiple CSV sources (products, sales, customers), performs data cleaning and transformations, and generates meaningful insights.

## Project Structure
```
Sixth_project/
│-- data/
│   ├── products.csv
│   ├── sales.csv
│   ├── customers.csv
│-- integrated_dataset.csv/
│-- venv/
|--pyspark_pipeline.py or newcode.py
│-- newqueries.py
│-- README.md
```

## Dependencies
The project requires the following dependencies:

### Python Packages
Ensure you have Python 3.8 or later installed.

Install dependencies using:
```bash
pip install -r requirements.txt
```

**`requirements.txt` includes:**
```
pyspark==3.5.0
pandas==1.5.3
numpy==1.23.5
```

### Additional Requirements
- **Java Development Kit (JDK):** Ensure that Java 8 or later is installed.
- **Apache Spark:** Download and set up Spark (https://spark.apache.org/downloads.html).
- **Hadoop:** Ensure Hadoop is installed for handling distributed storage (https://hadoop.apache.org/releases.html).

## Installation and Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/dmart-pyspark-pipeline.git
   cd dmart-pyspark-pipeline
   ```
2. Create a virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the pipeline script:
   ```bash
   python new.py
   ```

## Execution
To run the project, use the following command:
```bash
python newcode.py
```

## Key Features
- **Data Ingestion:** Reads CSV files into PySpark DataFrames.
- **Data Cleaning:** Handles missing values, duplicates, and incorrect formats.
- **Data Transformation:** Aggregation, filtering, and joining datasets.
- **Analysis & Insights:** Generates reports and visualizations.
- **Scalability:** Designed to process large datasets efficiently.

## Output
The final integrated dataset is saved in the `integrated_dataset.csv/` directory.

## Error Handling
- Ensure proper file paths are specified in the script.
- Use `mode("overwrite")` when writing output files to prevent existing path errors.
- Check Spark session and memory configurations if facing performance issues.

## Evaluation Criteria
The project will be evaluated based on the following factors:
- **Modular Code Design**
- **Code Maintainability and Portability**
- **PEP 8 Compliance**
- **GitHub Repository Structure and Documentation**

## Contribution
If you'd like to contribute:
1. Fork the repository.
2. Create a new branch (`feature-branch`).
3. Commit your changes and push them.
4. Submit a pull request.

## Contact
For any queries or support, please reach out to:
- **Your Name:** Akash Rathod
---

_This project was developed as part of the Dmart sales analysis challenge._

