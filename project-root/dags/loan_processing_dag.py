
import os
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from faker import Faker

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

# --- KONFIGURASI ---
POSTGRES_CONN_STRING = "postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow"
DB_CONN_ID = "postgres_default"
DATA_PATH = "/opt/airflow/data/loan_train.csv"


@dag(
    dag_id="loan_processing_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath="/opt/airflow/sql",
    doc_md="""
    ## Pipeline ELT untuk Data Persetujuan Pinjaman
    Pipeline ini melakukan proses ELT (Extract, Load, Transform) untuk data aplikasi pinjaman.
    1. **Generate**: Membuat dataset CSV lokal dengan konteks Indonesia.
    2. **Load (Staging)**: Memuat data mentah ke dalam tabel staging di PostgreSQL.
    3. **Transform & Load (DWH)**: Membersihkan, mentransformasi, dan memuat data ke dalam
       Data Warehouse dengan model Star Schema. **Logika DWH diperbaiki.**
    """,
    tags=["data-engineer", "loan-project", "final-fix"],
)
def loan_processing_pipeline():
    """
    ### Pipeline ELT Data Pinjaman
    DAG ini mengorkestrasi seluruh proses dari pembuatan data hingga memuatnya ke DWH.
    """

    @task
    def generate_local_dataset():
        """
        Membuat dataset CSV lokal dengan konteks Indonesia.
        """
        data_dir = os.path.dirname(DATA_PATH)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        fake = Faker("id_ID")
        num_records = 614  # Sesuai jumlah data asli

        data = []
        for i in range(num_records):
            gender = np.random.choice(["Male", "Female"], p=[0.81, 0.19])
            married = np.random.choice(["Yes", "No"], p=[0.65, 0.35])
            dependents = np.random.choice(["0", "1", "2", "3+"], p=[0.57, 0.17, 0.17, 0.09])
            education = np.random.choice(["Graduate", "Not Graduate"], p=[0.78, 0.22])
            self_employed = np.random.choice(["No", "Yes"], p=[0.86, 0.14])
            applicant_income = int(np.random.gamma(2.5, 2000) * 1000)
            coapplicant_income = int(np.random.gamma(1.5, 1500) * 1000) if np.random.rand() > 0.4 else 0
            loan_amount = int(np.random.gamma(3, 60_000) * 1000) # Diubah agar variasi puluhan-ratusan juta
            loan_amount_term = np.random.choice(
                [12.0, 24.0, 36.0, 60.0, 120.0, 180.0, 360.0] # Diubah menjadi lebih beragam (dalam bulan)
            )
            credit_history = np.random.choice([1.0, 0.0], p=[0.84, 0.16])
            property_area = np.random.choice(["Semiurban", "Urban", "Rural"], p=[0.38, 0.33, 0.29])
            loan_status = np.random.choice(["Y", "N"], p=[0.69, 0.31])

            # Simulasi data nulls
            row = {
                "Loan_ID": f"LP00{1000+i}",
                "Gender": np.random.choice([gender, None], p=[0.98, 0.02]),
                "Married": np.random.choice([married, None], p=[0.99, 0.01]),
                "Dependents": np.random.choice([dependents, None], p=[0.97, 0.03]),
                "Self_Employed": np.random.choice([self_employed, None], p=[0.95, 0.05]),
                "LoanAmount": np.random.choice([loan_amount, None], p=[0.96, 0.04]),
                "Loan_Amount_Term": np.random.choice([loan_amount_term, None], p=[0.98, 0.02]),
                "Credit_History": np.random.choice([credit_history, None], p=[0.92, 0.08]),
                "Education": education,
                "ApplicantIncome": applicant_income,
                "CoapplicantIncome": coapplicant_income,
                "Property_Area": property_area,
                "Loan_Status": loan_status,
            }
            data.append(row)

        df = pd.DataFrame(data)
        df.to_csv(DATA_PATH, index=False)
        print(f"Successfully generated and saved {len(df)} records to {DATA_PATH}")

    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        postgres_conn_id=DB_CONN_ID,
        sql="create_staging_table.sql",
    )

    @task
    def load_to_staging():
        """
        Memuat file CSV ke tabel staging.
        """
        df = pd.read_csv(DATA_PATH)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        engine = create_engine(POSTGRES_CONN_STRING)
        df.to_sql("stg_loan_applications", engine, if_exists="replace", index=False, chunksize=1000)
        print(f"Successfully loaded {len(df)} rows to staging table.")

    create_dwh_tables = PostgresOperator(
        task_id="create_dwh_tables",
        postgres_conn_id=DB_CONN_ID,
        sql="create_dwh_tables.sql",
    )

    @task
    def transform_and_load_dwh():
        """
        Membaca dari staging, melakukan transformasi Star Schema, dan memuat ke DWH.
        PERBAIKAN: Menambahkan TRUNCATE untuk memastikan idempotensi.
        """
        engine = create_engine(POSTGRES_CONN_STRING)

        # --- KUNCI PERBAIKAN: Kosongkan tabel DWH sebelum memuat data baru ---
        with engine.connect() as conn:
            conn.execute(
                text(
                    "TRUNCATE TABLE dim_customers, dim_location, dim_loan_details, fact_loans "
                    "RESTART IDENTITY CASCADE"
                )
            )
        print("Successfully truncated DWH tables.")
        # ------------------------------------------------------------------

        df_staging = pd.read_sql("SELECT * FROM stg_loan_applications", engine)

        # 1. Cleaning & Persiapan
        df_staging["loan_status_approved"] = df_staging["loan_status"] == "Y"
        df_staging["dependents"] = df_staging["dependents"].fillna("0")
        df_staging["self_employed"] = df_staging["self_employed"].fillna("No")
        df_staging["married"] = df_staging["married"].fillna("Yes")
        df_staging["gender"] = df_staging["gender"].fillna("Male")
        for col in ["loanamount", "loan_amount_term", "credit_history"]:
            df_staging[col].fillna(df_staging[col].median(), inplace=True)

        # 2. Dimensi
        dim_customers = df_staging[
            ["gender", "married", "dependents", "education", "self_employed", "credit_history"]
        ].drop_duplicates().reset_index(drop=True)
        dim_customers["customer_id"] = dim_customers.index
        dim_customers.to_sql("dim_customers", engine, if_exists="append", index=False)

        dim_location = df_staging[["property_area"]].drop_duplicates().reset_index(drop=True)
        dim_location["location_id"] = dim_location.index
        dim_location.to_sql("dim_location", engine, if_exists="append", index=False)

        dim_loan_details = df_staging[["loan_id"]].drop_duplicates().reset_index(drop=True)
        dim_loan_details["loan_detail_id"] = dim_loan_details.index
        dim_loan_details.to_sql("dim_loan_details", engine, if_exists="append", index=False)

        print(
            f"Loaded {len(dim_customers)} customers, {len(dim_location)} locations, "
            f"{len(dim_loan_details)} loan details."
        )

        # 3. Merge ke tabel fakta
        df_merged = df_staging.merge(
            dim_customers,
            on=["gender", "married", "dependents", "education", "self_employed", "credit_history"],
        )
        df_merged = df_merged.merge(dim_location, on="property_area")
        df_merged = df_merged.merge(dim_loan_details, on="loan_id")

        fact_loans = df_merged[
            [
                "customer_id",
                "location_id",
                "loan_detail_id",
                "applicantincome",
                "coapplicantincome",
                "loanamount",
                "loan_amount_term",
                "loan_status_approved",
            ]
        ].rename(
            columns={
                "applicantincome": "applicant_income",
                "coapplicantincome": "coapplicant_income",
                "loanamount": "loan_amount",
            }
        )

        # 4. Load fakta
        fact_loans.to_sql("fact_loans", engine, if_exists="append", index=False)
        print(f"Successfully loaded {len(fact_loans)} records into fact_loans table.")

    # Alur tugas
    generate_dataset_task = generate_local_dataset()
    load_staging_task = load_to_staging()
    transform_dwh_task = transform_and_load_dwh()

    generate_dataset_task >> create_staging_table >> load_staging_task
    load_staging_task >> create_dwh_tables >> transform_dwh_task


loan_processing_dag = loan_processing_pipeline()
