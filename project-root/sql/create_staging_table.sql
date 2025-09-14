-- Skrip ini bersifat idempoten, aman untuk dijalankan berulang kali.
DROP TABLE IF EXISTS stg_loan_applications;

CREATE TABLE stg_loan_applications (
    Loan_ID VARCHAR(50),
    Gender VARCHAR(10),
    Married VARCHAR(5),
    Dependents VARCHAR(10),
    Education VARCHAR(20),
    Self_Employed VARCHAR(5),
    ApplicantIncome INT,
    CoapplicantIncome REAL,
    LoanAmount REAL,
    Loan_Amount_Term REAL,
    Credit_History REAL,
    Property_Area VARCHAR(20),
    Loan_Status CHAR(1)
);
