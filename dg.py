from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.saas.analytics import Snowflake
from diagrams.programming.language import Python
from diagrams.onprem.ci import GithubActions
from diagrams.onprem.workflow import Airflow

with Diagram("Crypto Data Pipeline Architecture", show=False):
    
    # Data Extraction
    yfinance = Python("YFinance API")
    
    # Storage
    s3 = S3("S3 Storage")

    with Cluster("Snowflake Environment"):
        
        # Data Staging
        raw_stage = Snowflake("RAW_STAGE")

        with Cluster("Data Processing & Transformation"):
            crypto_raw = Snowflake("CRYPTO_RAW Table")
            snowpark = Python("Snowpark Transformations")
            crypto_harmonized = Snowflake("CRYPTO_HARMONIZED Table")

        with Cluster("User-Defined Functions (UDFs)"):
            sql_udf = Snowflake("SQL UDF\n(Currency Normalization)")
            python_udf = Python("Python UDF\n(Volatility Calculation)")

        with Cluster("Analytics & Aggregation"):
            crypto_analytics = Snowflake("CRYPTO_ANALYTICS")
            stored_proc = Snowflake("UPDATE_CRYPTO Procedure")

        with Cluster("Task Orchestration"):
            tasks = Snowflake("Snowflake Tasks")
            notebook = Snowflake("Snowflake Notebook")

    # CI/CD & Automation
    github_actions = GithubActions("GitHub Actions")
    airflow = Airflow("Airflow Orchestration")

    # Environment Management
    jinja = Python("Jinja Templates\n(DEV/PROD)")
    
    # Testing
    tests = Python("Unit Tests")

    # Flow connections
    yfinance >> s3 >> raw_stage >> crypto_raw
    crypto_raw >> snowpark >> crypto_harmonized
    crypto_harmonized >> [sql_udf, python_udf] >> crypto_analytics
    crypto_analytics >> stored_proc >> tasks >> notebook

    # CI/CD Integration
    github_actions >> Edge(style="dashed") >> [snowpark, python_udf]
    github_actions >> Edge(style="dashed") >> tasks

    # Environment Management (Jinja templates)
    jinja >> Edge(style="dashed") >> [raw_stage, crypto_raw, crypto_harmonized, crypto_analytics]

    # Testing
    tests >> Edge(style="dashed") >> [sql_udf, python_udf, stored_proc]

    # Airflow orchestrates tasks
    airflow >> Edge(style="dashed") >> tasks
