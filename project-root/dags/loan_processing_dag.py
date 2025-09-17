
import os
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

# --- KONFIGURASI ---
POSTGRES_CONN_STRING = "postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow"
DB_CONN_ID = "postgres_default"
DATA_PATH = "/opt/airflow/data/loan_train.csv"


# --- Helper finansial realistis (dari ChatGPT) ---
def _annuity_payment(principal, annual_rate, n_months):
    if n_months <= 0: return principal
    r = max(annual_rate / 12.0, 1e-6)
    return principal * r / (1 - (1 + r) ** -n_months)

def _principal_from_payment(monthly_payment, annual_rate, n_months):
    if n_months <= 0: return 0
    r = max(annual_rate / 12.0, 1e-6)
    return monthly_payment * (1 - (1 + r) ** -n_months) / r

def _sample_credit_history(education, self_employed, property_area):
    p = 0.90 if education == "Graduate" else 0.78
    if self_employed == "Yes": p -= 0.06
    if property_area == "Rural": p -= 0.05
    p = float(np.clip(p, 0.10, 0.95))
    return 1.0 if np.random.rand() < p else 0.0

def _sample_applicant_income(education, married, self_employed, property_area):
    # income **BULANAN** (IDR)
    if education == "Graduate":
        shape, scale = 3.0, 3000   # mean ≈ 9 jt
    else:
        shape, scale = 2.2, 1800   # mean ≈ 4 jt
    area_mul = {"Urban": 1.20, "Semiurban": 1.05, "Rural": 0.90}.get(property_area, 1.0)
    married_mul = 1.06 if married == "Yes" else 1.0
    se_mul = 0.95 if self_employed == "Yes" else 1.0
    noise = np.random.normal(1.0, 0.05)
    inc = np.random.gamma(shape, scale) * 1000 * area_mul * married_mul * se_mul * noise
    return int(np.clip(inc, 2_000_000, 50_000_000))

def _sample_coapplicant_income(married, applicant_edu, property_area):
    if married != "Yes" or np.random.rand() < 0.30:
        return 0
    if applicant_edu == "Graduate":
        shape, scale = 2.2, 2200  # mean ~ 4.8 jt
    else:
        shape, scale = 2.0, 1600  # mean ~ 3.2 jt
    area_mul = {"Urban": 1.15, "Semiurban": 1.03, "Rural": 0.90}.get(property_area, 1.0)
    inc = np.random.gamma(shape, scale) * 1000 * area_mul
    return int(np.clip(inc, 1_000_000, 30_000_000))

def _sample_term_and_loan(monthly_income_total, credit_history, property_area):
    # bunga tahunan realistis
    annual_rate = float(np.clip(np.random.normal(0.14, 0.03), 0.08, 0.20))
    # target PTI (cicilan/bulan / income/bulan)
    pti = float(np.clip(np.random.normal(0.32, 0.07), 0.15, 0.50))
    if credit_history == 0.0: pti = min(pti, 0.28)
    if property_area == "Urban": pti = min(pti + 0.02, 0.50)

    # tenor dipilih dulu, lalu plafon dihitung dari cicilan target via anuitas
    if monthly_income_total < 6_000_000:
        choices, probs = [12.0, 24.0, 36.0, 60.0], [0.20, 0.30, 0.35, 0.15]
    elif monthly_income_total < 12_000_000:
        choices, probs = [24.0, 36.0, 60.0, 120.0], [0.10, 0.35, 0.35, 0.20]
    else:
        choices, probs = [36.0, 60.0, 120.0, 180.0, 240.0, 360.0], [0.05,0.20,0.35,0.20,0.10,0.10]
    term = float(np.random.choice(choices, p=probs))

    monthly_pay_target = pti * monthly_income_total
    principal = _principal_from_payment(monthly_pay_target, annual_rate, int(term))
    loan_amount = int(np.clip(principal * np.random.uniform(0.9, 1.1), 5_000_000, 2_000_000_000))
    return loan_amount, term, annual_rate, pti

def determine_loan_status_realistic(education, married, dependents, self_employed,
                                    property_area, credit_history,
                                    applicant_income, coapplicant_income,
                                    loan_amount, loan_amount_term, annual_rate):
    # Keputusan probabilistik berbasis PTI + faktor risiko.
    total_inc_mo = applicant_income + coapplicant_income
    if total_inc_mo <= 0: 
        return 'N'
    
    # handle potential zero term
    if loan_amount_term is None or loan_amount_term == 0:
        return 'N'

    pay_mo = _annuity_payment(loan_amount, annual_rate, int(loan_amount_term))
    pti = pay_mo / total_inc_mo

    # skor logit sederhana
    z = -0.15
    z += 1.6 if credit_history == 1.0 else -1.0
    z += 0.30 if education == "Graduate" else 0.0
    z += 0.15 if married == "Yes" else 0.0
    z -= 0.25 if self_employed == "Yes" else 0.0
    z += 0.12 if property_area == "Urban" else (0.04 if property_area == "Semiurban" else 0.0)

    # penalti dependents
    dep = str(dependents)
    z -= (0.25 if dep in {"2","3+"} else (0.10 if dep=="1" else 0.0))

    # penalti/bonus dari PTI
    if pti <= 0.35: z += 0.8
    elif pti <= 0.45: z += 0.2
    elif pti <= 0.55: z -= 0.4
    else: z -= 0.9

    # noise kecil
    z += np.random.normal(0, 0.25)

    p = 1/(1+np.exp(-z))
    return 'Y' if np.random.rand() < p else 'N'

@dag(
    dag_id="loan_processing_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    template_searchpath="/opt/airflow/sql",
    doc_md='''
    ## Pipeline ELT untuk Data Persetujuan Pinjaman (Logika Realistis)
    Pipeline ini menggunakan logika yang disempurnakan untuk menghasilkan data yang lebih realistis.
    1. **Generate**: Membuat 100.000 baris data CSV dengan korelasi fitur yang kaya.
    2. **Load (Staging)**: Memuat data mentah ke dalam tabel staging di PostgreSQL.
    3. **Transform & Load (DWH)**: Membersihkan dan memuat data ke dalam Data Warehouse.
    ''',
    tags=["data-engineer", "loan-project"],
)
def loan_processing_pipeline():
    @task
    def generate_local_dataset():
        data_dir = os.path.dirname(DATA_PATH)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        num_records = 100000
        data = []

        for i in range(num_records):
            education = np.random.choice(["Graduate", "Not Graduate"], p=[0.70, 0.30])
            married = np.random.choice(["Yes", "No"], p=[0.65, 0.35])
            gender = np.random.choice(["Male", "Female"], p=[0.81, 0.19])
            self_employed = np.random.choice(["No", "Yes"], p=[0.86, 0.14])
            property_area = np.random.choice(["Semiurban", "Urban", "Rural"], p=[0.38, 0.33, 0.29])

            dependents = (np.random.choice(["0","1","2","3+"], p=[0.35,0.30,0.22,0.13])
                          if married=="Yes" else "0")

            applicant_income = _sample_applicant_income(education, married, self_employed, property_area)
            coapplicant_income = _sample_coapplicant_income(married, education, property_area)

            credit_history = _sample_credit_history(education, self_employed, property_area)

            total_income_mo = applicant_income + coapplicant_income
            loan_amount, loan_amount_term, annual_rate, pti_target = _sample_term_and_loan(
                total_income_mo, credit_history, property_area
            )

            loan_status = determine_loan_status_realistic(
                education, married, dependents, self_employed, property_area, credit_history,
                applicant_income, coapplicant_income, loan_amount, loan_amount_term, annual_rate
            )

            row = {
                "Loan_ID": f"LP00{1000+i}",
                "Gender": np.random.choice([gender, None], p=[0.98, 0.02]),
                "Married": np.random.choice([married, None], p=[0.99, 0.01]),
                "Dependents": np.random.choice([dependents, None], p=[0.97, 0.03]),
                "Education": education,
                "Self_Employed": np.random.choice([self_employed, None], p=[0.95, 0.05]),
                "ApplicantIncome": applicant_income, # BULANAN
                "CoapplicantIncome": coapplicant_income, # BULANAN
                "LoanAmount": np.random.choice([loan_amount, None], p=[0.97, 0.03]),
                "Loan_Amount_Term": np.random.choice([loan_amount_term, None], p=[0.99, 0.01]),
                "Credit_History": np.random.choice([credit_history, None], p=[0.95, 0.05]),
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
        engine = create_engine(POSTGRES_CONN_STRING)
        with engine.connect() as conn:
            conn.execute(
                text("TRUNCATE TABLE dim_customers, dim_location, dim_loan_details, fact_loans RESTART IDENTITY CASCADE")
            )
        print("Successfully truncated DWH tables.")

        df_staging = pd.read_sql("SELECT * FROM stg_loan_applications", engine)

        df_staging["loan_status_approved"] = df_staging["loan_status"] == "Y"
        df_staging["dependents"] = df_staging["dependents"].fillna("0")
        df_staging["self_employed"] = df_staging["self_employed"].fillna("No")
        df_staging["married"] = df_staging["married"].fillna("Yes")
        df_staging["gender"] = df_staging["gender"].fillna("Male")
        for col in ["loanamount", "loan_amount_term", "credit_history"]:
            df_staging[col].fillna(df_staging[col].median(), inplace=True)

        dim_customers = df_staging[["gender", "married", "dependents", "education", "self_employed", "credit_history"]].drop_duplicates().reset_index(drop=True)
        dim_customers["customer_id"] = dim_customers.index
        dim_customers.to_sql("dim_customers", engine, if_exists="append", index=False)

        dim_location = df_staging[['property_area']].drop_duplicates().reset_index(drop=True)
        dim_location["location_id"] = dim_location.index
        dim_location.to_sql("dim_location", engine, if_exists="append", index=False)

        dim_loan_details = df_staging[['loan_id']].drop_duplicates().reset_index(drop=True)
        dim_loan_details["loan_detail_id"] = dim_loan_details.index
        dim_loan_details.to_sql("dim_loan_details", engine, if_exists="append", index=False)

        print(f"Loaded {len(dim_customers)} customers, {len(dim_location)} locations, {len(dim_loan_details)} loan details.")

        df_merged = df_staging.merge(dim_customers, on=["gender", "married", "dependents", "education", "self_employed", "credit_history"])
        df_merged = df_merged.merge(dim_location, on="property_area")
        df_merged = df_merged.merge(dim_loan_details, on="loan_id")

        fact_loans = df_merged[['customer_id', 'location_id', 'loan_detail_id', 'applicantincome', 'coapplicantincome', 'loanamount', 'loan_amount_term', 'loan_status_approved']].rename(columns={"applicantincome": "applicant_income", "coapplicantincome": "coapplicant_income", "loanamount": "loan_amount"})
        fact_loans.to_sql("fact_loans", engine, if_exists="append", index=False)
        print(f"Successfully loaded {len(fact_loans)} records into fact_loans table.")

    generate_dataset_task = generate_local_dataset()
    load_staging_task = load_to_staging()
    transform_dwh_task = transform_and_load_dwh()

    generate_dataset_task >> create_staging_table >> load_staging_task
    load_staging_task >> create_dwh_tables >> transform_dwh_task

loan_processing_dag = loan_processing_pipeline()
