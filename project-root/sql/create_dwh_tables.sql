-- Skrip ini juga idempoten.
-- Membuat tabel dimensi dan fakta untuk Data Warehouse.

-- Hapus tabel dalam urutan yang benar atau gunakan CASCADE
-- Menggunakan CASCADE lebih aman jika ada dependensi yang tidak terduga.
DROP TABLE IF EXISTS fact_loans CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_loan_details CASCADE;

-- Tabel Dimensi untuk informasi Peminjam (Customer)
CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY,
    gender VARCHAR(10),
    married VARCHAR(5),
    dependents VARCHAR(10),
    education VARCHAR(20),
    self_employed VARCHAR(5),
    credit_history REAL
);

-- Tabel Dimensi untuk area properti
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    property_area VARCHAR(20)
);

-- Tabel Dimensi untuk detail unik pinjaman
CREATE TABLE dim_loan_details (
    loan_detail_id INT PRIMARY KEY,
    loan_id VARCHAR(50)
);

-- Tabel Fakta yang menghubungkan semua dimensi dan berisi ukuran (measures)
CREATE TABLE fact_loans (
    loan_fact_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(customer_id),
    location_id INT REFERENCES dim_location(location_id),
    loan_detail_id INT REFERENCES dim_loan_details(loan_detail_id),
    applicant_income INT,
    coapplicant_income REAL,
    loan_amount REAL,
    loan_amount_term REAL,
    loan_status_approved BOOLEAN
);
