
# loan_processing_dag_realistic.py
# Single-file Airflow DAG to generate realistic synthetic loan data, load to Postgres staging,
# and transform into a Star Schema DWH. Includes PTI (annuity), LTV cap, audit columns,
# reproducibility, and simple calibration knobs.

import os
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

# ===========================
# CONFIG
# ===========================
POSTGRES_CONN_STRING = os.getenv(
    "POSTGRES_CONN_STRING",
    "postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow",
)
DB_CONN_ID = os.getenv("DB_CONN_ID", "postgres_default")
DATA_PATH = os.getenv("DATA_PATH", "/opt/airflow/data/loan_train.csv")

# Number of rows to generate (can override via env)
NUM_RECORDS = int(os.getenv("NUM_RECORDS", "100000"))

# ===========================
# CALIBRATION & CONSTANTS
# ===========================
MAX_LTV = float(os.getenv("MAX_LTV", "0.80"))            # Max loan as % of property value
APPROVAL_INTERCEPT_SHIFT = float(os.getenv("APPROVAL_INTERCEPT_SHIFT", "0"))
CH_ZERO_APPROVAL_P = float(os.getenv("CH_ZERO_APPROVAL_P", "0.02"))
GLOBAL_SEED = int(os.getenv("GLOBAL_SEED", "42"))

np.random.seed(GLOBAL_SEED)

# ===========================
# FINANCIAL HELPERS (REALISTIC)
# ===========================

def _annuity_payment(principal: float, annual_rate: float, n_months: int) -> float:
    if n_months <= 0:
        return principal
    r = max(annual_rate / 12.0, 1e-6)
    return principal * r / (1 - (1 + r) ** -n_months)


def _principal_from_payment(monthly_payment: float, annual_rate: float, n_months: int) -> float:
    if n_months <= 0:
        return 0.0
    r = max(annual_rate / 12.0, 1e-6)
    return monthly_payment * (1 - (1 + r) ** -n_months) / r


def _sample_credit_history(education: str, self_employed: str, property_area: str) -> float:
    p = 0.90 if education == "Graduate" else 0.78
    if self_employed == "Yes":
        p -= 0.06
    if property_area == "Rural":
        p -= 0.05
    p = float(np.clip(p, 0.10, 0.95))
    return 1.0 if np.random.rand() < p else 0.0


def _sample_applicant_income(education: str, married: str, self_employed: str, property_area: str) -> int:
    # Monthly income in IDR
    if education == "Graduate":
        shape, scale = 3.0, 3000   # mean ≈ 9M
    else:
        shape, scale = 2.2, 1800   # mean ≈ 4M
    area_mul = {"Urban": 1.20, "Semiurban": 1.05, "Rural": 0.90}.get(property_area, 1.0)
    married_mul = 1.06 if married == "Yes" else 1.0
    se_mul = 0.95 if self_employed == "Yes" else 1.0
    noise = np.random.normal(1.0, 0.05)
    inc = np.random.gamma(shape, scale) * 1000 * area_mul * married_mul * se_mul * noise
    return int(np.clip(inc, 2_000_000, 50_000_000))


def _sample_coapplicant_income(married: str, applicant_edu: str, property_area: str) -> int:
    if married != "Yes" or np.random.rand() < 0.30:
        return 0
    if applicant_edu == "Graduate":
        shape, scale = 2.2, 2200  # mean ~ 4.8M
    else:
        shape, scale = 2.0, 1600  # mean ~ 3.2M
    area_mul = {"Urban": 1.15, "Semiurban": 1.03, "Rural": 0.90}.get(property_area, 1.0)
    inc = np.random.gamma(shape, scale) * 1000 * area_mul
    return int(np.clip(inc, 1_000_000, 30_000_000))


def _sample_property_value(monthly_income_total: float, property_area: str) -> int:
    # Property value ≈ (3.5–6.0) x annual household income, adjusted by area
    annual_income = max(monthly_income_total * 12, 1)
    base_mult = np.random.uniform(3.5, 6.0)
    area_mul = {"Urban": 1.15, "Semiurban": 1.00, "Rural": 0.85}.get(property_area, 1.0)
    value = annual_income * base_mult * area_mul
    return int(np.clip(value, 100_000_000, 10_000_000_000))


def _sample_term_and_loan(monthly_income_total: float, credit_history: float, property_area: str):
    # Annual interest rate (effective-equivalent), 8%–20%
    annual_rate = float(np.clip(np.random.normal(0.14, 0.03), 0.08, 0.20))
    # Target PTI distribution (cicilan/bulan / income/bulan)
    pti = float(np.clip(np.random.normal(0.32, 0.07), 0.15, 0.50))
    if credit_history == 0.0:
        pti = min(pti, 0.28)
    if property_area == "Urban":
        pti = min(pti + 0.02, 0.50)

    # Initial term choice
    if monthly_income_total < 6_000_000:
        choices, probs = [12.0, 24.0, 36.0, 60.0], [0.20, 0.30, 0.35, 0.15]
    elif monthly_income_total < 12_000_000:
        choices, probs = [24.0, 36.0, 60.0, 120.0], [0.10, 0.35, 0.35, 0.20]
    else:
        choices, probs = [36.0, 60.0, 120.0, 180.0, 240.0, 360.0], [0.05, 0.20, 0.35, 0.20, 0.10, 0.10]
    term = float(np.random.choice(choices, p=probs))

    # PTI-driven principal from target monthly payment (annuity)
    monthly_pay_target = pti * monthly_income_total
    principal_pti = _principal_from_payment(monthly_pay_target, annual_rate, int(term))

    # LTV cap based on sampled property value
    prop_value = _sample_property_value(monthly_income_total, property_area)
    principal_ltv_cap = MAX_LTV * prop_value

    # Choose min of PTI and LTV, add mild noise
    principal = min(principal_pti, principal_ltv_cap) * np.random.uniform(0.9, 1.1)

    # Avoid very long term for very small principal
    if principal < 75_000_000 and term > 180:
        term = float(np.random.choice([60.0, 120.0, 180.0], p=[0.3, 0.4, 0.3]))

    loan_amount = int(np.clip(principal, 5_000_000, 2_000_000_000))
    return loan_amount, term, annual_rate, pti, prop_value


def determine_loan_status_realistic(
    education: str,
    married: str,
    dependents: str,
    self_employed: str,
    property_area: str,
    credit_history: float,
    applicant_income: int,
    coapplicant_income: int,
    loan_amount: int,
    loan_amount_term: float,
    annual_rate: float,
) -> str:
    # If credit history is bad, approve rarely
    if credit_history == 0.0:
        return 'Y' if np.random.rand() < CH_ZERO_APPROVAL_P else 'N'

    total_inc_mo = applicant_income + coapplicant_income
    if total_inc_mo <= 0 or not loan_amount_term:
        return 'N'

    pay_mo = _annuity_payment(loan_amount, annual_rate, int(loan_amount_term))
    pti = pay_mo / max(total_inc_mo, 1)

    # Logistic-style score
    z = -0.15 + APPROVAL_INTERCEPT_SHIFT  # global calibration knob
    z += 1.6  # credit_history == 1.0
    z += 0.30 if education == "Graduate" else 0.0
    z += 0.15 if married == "Yes" else 0.0
    z -= 0.25 if self_employed == "Yes" else 0.0
    z += 0.12 if property_area == "Urban" else (0.04 if property_area == "Semiurban" else 0.0)

    dep = str(dependents)
    z -= (0.25 if dep in {"2", "3+"} else (0.10 if dep == "1" else 0.0))

    if pti <= 0.35:
        z += 0.8
    elif pti <= 0.45:
        z += 0.2
    elif pti <= 0.55:
        z -= 0.4
    else:
        z -= 0.9

    # mild noise for realism
    z += np.random.normal(0, 0.25)

    p = 1 / (1 + np.exp(-z))
    return 'Y' if np.random.rand() < p else 'N'


# ===========================
# AIRFLOW DAG
# ===========================
@dag(
    dag_id="loan_processing_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # manual trigger by default
    catchup=False,
    template_searchpath="/opt/airflow/sql",
    doc_md='''
    ## Pipeline ELT untuk Data Persetujuan Pinjaman (Logika Realistis)
    Pipeline ini menggunakan logika yang disempurnakan untuk menghasilkan data yang lebih realistis.
    1. **Generate**: Membuat dataset CSV dengan korelasi fitur yang kaya (default 100.000 baris, ubah via env NUM_RECORDS).
    2. **Load (Staging)**: Memuat data mentah ke dalam tabel staging di PostgreSQL.
    3. **Transform & Load (DWH)**: Membersihkan dan memuat data ke dalam Data Warehouse (Star Schema).
    ''',
    tags=["data-engineer", "loan-project", "realistic-synth"],
)
def loan_processing_pipeline():
    @task
    def generate_local_dataset():
        # Ensure data directory
        data_dir = os.path.dirname(DATA_PATH)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        data = []
        for i in range(NUM_RECORDS):
            education = np.random.choice(["Graduate", "Not Graduate"], p=[0.70, 0.30])
            married = np.random.choice(["Yes", "No"], p=[0.65, 0.35])
            gender = np.random.choice(["Male", "Female"], p=[0.81, 0.19])
            self_employed = np.random.choice(["No", "Yes"], p=[0.86, 0.14])
            property_area = np.random.choice(["Semiurban", "Urban", "Rural"], p=[0.38, 0.33, 0.29])
            dependents = (
                np.random.choice(["0", "1", "2", "3+"], p=[0.35, 0.30, 0.22, 0.13])
                if married == "Yes" else "0"
            )

            applicant_income = _sample_applicant_income(education, married, self_employed, property_area)
            coapplicant_income = _sample_coapplicant_income(married, education, property_area)
            credit_history = _sample_credit_history(education, self_employed, property_area)

            total_income_mo = applicant_income + coapplicant_income
            loan_amount, loan_amount_term, annual_rate, pti_target, property_value = _sample_term_and_loan(
                total_income_mo, credit_history, property_area
            )

            loan_status = determine_loan_status_realistic(
                education, married, dependents, self_employed, property_area, credit_history,
                applicant_income, coapplicant_income, loan_amount, loan_amount_term, annual_rate
            )

            monthly_payment = _annuity_payment(loan_amount, annual_rate, int(loan_amount_term))
            pti_actual = monthly_payment / max(total_income_mo, 1)
            ltv = loan_amount / max(property_value, 1)

            row = {
                "Loan_ID": f"LP00{1000+i}",
                "Gender": np.random.choice([gender, None], p=[0.98, 0.02]),
                "Married": np.random.choice([married, None], p=[0.99, 0.01]),
                "Dependents": np.random.choice([dependents, None], p=[0.97, 0.03]),
                "Education": education,
                "Self_Employed": np.random.choice([self_employed, None], p=[0.95, 0.05]),
                "ApplicantIncome": applicant_income,  # monthly
                "CoapplicantIncome": coapplicant_income,  # monthly
                "LoanAmount": np.random.choice([loan_amount, None], p=[0.97, 0.03]),
                "Loan_Amount_Term": np.random.choice([loan_amount_term, None], p=[0.99, 0.01]),
                "Credit_History": np.random.choice([credit_history, None], p=[0.95, 0.05]),
                "Property_Area": property_area,
                "Loan_Status": loan_status,
                # Audit columns (stay in CSV/staging; not required in DWH)
                "Annual_Rate": round(annual_rate, 4),
                "MonthlyPayment": int(monthly_payment),
                "PTI": round(pti_actual, 4),
                "Property_Value": int(property_value),
                "LTV": round(ltv, 4),
            }
            data.append(row)

        df = pd.DataFrame(data)
        # Sanity checks
        ok = df.dropna(subset=["Loan_Status", "ApplicantIncome", "CoapplicantIncome", "LoanAmount", "Loan_Amount_Term", "Annual_Rate"])  # noqa
        approval_rate = (ok["Loan_Status"] == "Y").mean() if len(ok) else 0
        print(f"Approval rate (rough): {approval_rate:.2%}")
        if len(ok):
            print(
                "Median PTI:", round(float(ok["PTI"].median()), 3),
                "| Median LTV:", round(float(ok["LTV"].median()), 3)
            )

        # Save
        data_dir = os.path.dirname(DATA_PATH)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        df.to_csv(DATA_PATH, index=False)
        print(f"Successfully generated and saved {len(df)} records to {DATA_PATH}")

    create_staging_table = PostgresOperator(
        task_id="create_staging_table",
        postgres_conn_id=DB_CONN_ID,
        sql="create_staging_table.sql",
    )

    @task
    def load_to_staging():
        df = pd.read_csv(DATA_PATH)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        engine = create_engine(POSTGRES_CONN_STRING)
        # Faster writes for large datasets
        df.to_sql(
            "stg_loan_applications",
            engine,
            if_exists="replace",
            index=False,
            chunksize=5000,
            method='multi'
        )
        print(f"Successfully loaded {len(df)} rows to staging table.")

    create_dwh_tables = PostgresOperator(
        task_id="create_dwh_tables",
        postgres_conn_id=DB_CONN_ID,
        sql="create_dwh_tables.sql",
    )

    @task
    def transform_and_load_dwh():
        engine = create_engine(POSTGRES_CONN_STRING)
        with engine.connect() as conn:
            conn.execute(
                text(
                    "TRUNCATE TABLE dim_customers, dim_location, dim_loan_details, fact_loans RESTART IDENTITY CASCADE"
                )
            )
        print("Successfully truncated DWH tables.")

        df_staging = pd.read_sql("SELECT * FROM stg_loan_applications", engine)

        df_staging["loan_status_approved"] = df_staging["loan_status"] == "Y"
        
        # --- BEST OF BOTH WORLDS: Robust Missing Value Handling ---
        # 1. Drop rows where the most critical data (credit_history) is missing.
        df_staging.dropna(subset=["credit_history"], inplace=True)
        print(f"Records after dropping missing credit_history: {len(df_staging)}")

        # 2. Fill less critical data with reasonable defaults.
        df_staging["dependents"].fillna("0", inplace=True)
        df_staging["self_employed"].fillna("No", inplace=True)
        df_staging["married"].fillna("Yes", inplace=True)
        df_staging["gender"].fillna("Male", inplace=True)
        # For loan amount/term, median is a reasonable strategy for missing values.
        for col in ["loanamount", "loan_amount_term"]:
            if col in df_staging.columns:
                df_staging[col].fillna(df_staging[col].median(), inplace=True)
        
        # Drop any remaining rows with nulls just in case, for DWH integrity
        df_staging.dropna(inplace=True)
        print(f"Records after final dropna: {len(df_staging)}")


        # Dimensions
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
            f"Loaded {len(dim_customers)} customers, {len(dim_location)} locations, {len(dim_loan_details)} loan details."
        )

        # Fact
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

        fact_loans.to_sql("fact_loans", engine, if_exists="append", index=False, chunksize=5000, method='multi')
        print(f"Successfully loaded {len(fact_loans)} records into fact_loans table.")

    # Flow
    generate_dataset_task = generate_local_dataset()
    load_staging_task = load_to_staging()
    transform_dwh_task = transform_and_load_dwh()

    generate_dataset_task >> create_staging_table >> load_staging_task
    load_staging_task >> create_dwh_tables >> transform_dwh_task


# DAG object
loan_processing_dag = loan_processing_pipeline()
