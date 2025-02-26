# 🚀 Cryptocurrency Data Pipeline with Snowflake & Snowpark

### **📄 Project Summary**  
Quickstart: 

Youtube: 

## **📌 Overview**
This project builds an **incremental data pipeline** to ingest, transform, and analyze **cryptocurrency price data** (BTC, ETH, DOGE) using **Snowflake, Snowpark Python, and GitHub Actions**.

The system provides:
- Fetch **daily crypto prices** from **Yahoo Finance API**.
- Store raw data in **Snowflake staging tables**.
- Compute **daily & weekly returns** in **analytics tables**.
- Automate **data ingestion & processing** using **Snowflake Tasks**.
- Deploy & schedule workflows using **GitHub Actions & Snowflake Notebooks**.

---

## **🔑 Features**


---

## **✔ Technology Stack**

| **Technology** | **Purpose** |
|--------------|------------|
| **Snowflake** | Cloud Data Warehouse for storing & processing data |
| **Snowpark Python** | Enables Python-based data transformations within Snowflake |
| **Yahoo Finance API** | Fetches cryptocurrency market data |
| **CoinGecko API** | Alternative data source for real-time prices |
| **Snowflake Streams & Tasks** | Automates incremental data ingestion & transformation |
| **Stored Procedures (SQL & Python)** | Manages updates & analytics computations |
| **GitHub Actions** | CI/CD automation for Snowpark Python deployment |
| **Snowflake Notebooks** | Data exploration & transformations |
| **Jinja Templates** | Environment management for DEV & PROD |
| **Pytest** | Unit testing framework for data validation |
| **Pandas & NumPy** | Data transformation & analytics in Snowpark |
| **Matplotlib & Seaborn** | Data visualization for insights |

---

## **🛠️ Diagrams**

![image](./docs/data_extraction_architecture.png)


---

## **🛠️ User Guide**
1. Users can choose different ways to extract data. 
2. When user inputs a PDF or URL, it will be temporarily stored in S3 first
3. PDF data will be processed through the API call function to obtain the table image and text
4. Text will be marked down in two ways (docling markitdown)
5. Finally a **download link** will be returned, which contains all the output files.

---

## **📂 Project Structure**
```plaintext
crypto-data-pipeline/
│── .github/
│   └── workflows/              # GitHub Actions for CI/CD
│       └── deploy.yml
│
│── docs/                       # Documentation
│   ├── architecture_diagram.png
│   ├── README.md                # Project overview
│   ├── AIUseDisclosure.md        # AI usage disclosure
│   ├── setup_instructions.md     # Setup guide
│
│── notebooks/                   # Jupyter or Snowflake Notebooks
│   ├── data_exploration.ipynb    # Exploratory analysis
│   ├── snowflake_pipeline.ipynb  # Snowflake Notebook for processing
│
│── sql/                         # SQL scripts for Snowflake
│   ├── create_schemas.sql        # Schema creation
│   ├── create_tables.sql         # Table creation
│   ├── stored_procedures.sql      # Stored procedures
│   ├── tasks.sql                  # Snowflake tasks automation
│
│── src/                         # Source code for data pipeline
│   ├── load_data.py              # Fetches and loads crypto data
│   ├── transform_data.py         # Cleans & processes data in Snowpark
│   ├── analytics.py              # Computes performance metrics
│   ├── config.py                 # Configurations (API keys, Snowflake creds)
│
│── tests/                       # Unit tests for pipeline
│   ├── test_udfs.py              # Tests SQL & Python UDFs
│   ├── test_pipeline.py          # Validates data ingestion
│
│── requirements.txt             # Python dependencies
│── Dockerfile                   # Containerized deployment (optional)
│── .gitignore                    # Ignore unnecessary files
│── LICENSE                       # Open-source license
│── setup.py                      # Setup script (if using as a package)
│── CONTRIBUTING.md               # Guidelines for collaboration
│── CODE_OF_CONDUCT.md            # Community standards


```

---

## **🚀 Installation & Setup**
1️⃣ Prerequisites
Ensure you have:

- **Python 3.8+**
- **Snowflake Account**
- **Git & GitHub**
- **Yahoo Finance API Access**

2️⃣ Clone the Repository
```
git clone https://github.com/Neneanuj/Snowflake_DataPipeline.git
cd Snowflake_DataPipeline
```

3️⃣ Create a Virtual Environment
```
python -m venv venv
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate     # Windows

pip install -r requirements.txt
```

4️⃣ Configure Snowflake Credentials
Edit the config.py file:
```
SNOWFLAKE_ACCOUNT = "your_snowflake_account"
SNOWFLAKE_USER = "your_username"
SNOWFLAKE_PASSWORD = "your_password"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "CRYPTO_DB"
SNOWFLAKE_SCHEMA = "RAW_CRYPTO"
```
---

## **🛠️ Usage**

---


## **📌 Expected Outcomes**

✅ **Fully functional, automated cryptocurrency data pipeline** built on **Snowflake & Snowpark**.  
✅ **Daily ingestion of cryptocurrency prices (BTC, ETH, DOGE)** using **Yahoo Finance API**.  
✅ **Incremental updates** using **Snowflake Streams, Tasks, and Stored Procedures**.  
✅ **Precomputed analytics tables** with **daily & weekly return metrics**.  
✅ **Snowflake Notebooks** for **data exploration & analytics processing**.  
✅ **Continuous Integration & Deployment (CI/CD)** with **GitHub Actions**.  
✅ **Unit tests** to validate **UDFs, transformations, and pipeline correctness**.  
✅ **Environment management with Jinja-based templates** for **DEV & PROD**.  

---

## **📌 AI Use Disclosure**

📄 See AiUseDisclosure.md for details.

---

## **👨‍💻 Authors**
* Sicheng Bao (@Jellysillyfish13)
* Yung Rou Ko (@KoYungRou)
* Anuj Rajendraprasad Nene (@Neneanuj)

---

## **📞 Contact**
For questions, reach out via Big Data Course or open an issue on GitHub.
