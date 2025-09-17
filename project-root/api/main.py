
import os
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Callable, Optional

# --- Pydantic Model for Input Validation ---
class LoanApplication(BaseModel):
    Gender: str
    Married: str
    Dependents: str
    Education: str
    Self_Employed: str
    ApplicantIncome: int
    CoapplicantIncome: float
    LoanAmount: float
    Loan_Amount_Term: float
    Credit_History: float
    Property_Area: str

# --- AI Guardrails: Indonesian Banking Rules Framework ---

# --- Pre-Model Policy Rules (Fast Rejection) ---

def rule_check_credit_history(app: LoanApplication) -> Optional[str]:
    """Hard Rule inspired by SLIK/BI Checking. A bad credit history is an immediate rejection."""
    if app.Credit_History == 0:
        return "Rejected due to poor credit history (equivalent to SLIK/BI Checking score 3-5)."
    return None

def rule_check_min_income(app: LoanApplication, min_income_threshold: int = 4000000) -> Optional[str]:
    """Policy Rule: Ensure applicant has a minimum viable income for loan consideration."""
    total_income = app.ApplicantIncome + app.CoapplicantIncome
    if total_income < min_income_threshold:
        return f"Rejected due to insufficient total income. Minimum required is Rp {min_income_threshold:,.0f}."
    return None

def rule_check_lti_ratio(app: LoanApplication, max_lti_ratio: int = 7) -> Optional[str]:
    """Loan-to-Income (LTI) ratio check. Prevents approving loans that are disproportionately large compared to annual income."""
    total_income = app.ApplicantIncome + app.CoapplicantIncome
    if total_income > 0:
        annual_income = total_income * 12
        lti_ratio = app.LoanAmount / annual_income
        if lti_ratio > max_lti_ratio:
            return f"Rejected by LTI rule. Total loan is {lti_ratio:.1f}x the annual income, exceeding the {max_lti_ratio}x limit."
    elif total_income <= 0:
        # This case is primarily handled by the DTI rule's income check, but included for robustness
        return "Rejected due to zero or negative total income, making LTI calculation impossible."
    return None


# --- Post-Model Affordability & Sanity Rules ---

def rule_check_loan_term(app: LoanApplication) -> Optional[str]:
    """Hard Rule for logical validation. A loan must have a valid duration."""
    if app.Loan_Amount_Term <= 0:
        return "Rejected due to invalid loan term. Term must be greater than zero."
    return None

def rule_check_dti_ratio(app: LoanApplication) -> Optional[str]:
    """Debt-to-Income (DTI) ratio check. Prevents approving loans that are not affordable."""
    total_income = app.ApplicantIncome + app.CoapplicantIncome
    if total_income > 0:
        # Ensure loan term is valid before division to prevent ZeroDivisionError
        if app.Loan_Amount_Term <= 0:
            return None # The loan term rule will catch this, so we skip DTI calculation.
        
        monthly_payment = app.LoanAmount / app.Loan_Amount_Term
        dti_ratio = monthly_payment / total_income
        if dti_ratio > 0.5:
            return f"Rejected by DTI rule. Monthly payment is {dti_ratio:.0%} of income, exceeding the 50% limit."
    elif total_income <= 0:
        return "Rejected due to zero or negative total income."
    return None

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Loan Prediction API",
    description=".",
    version="7.4.0"
)

# --- Global variables ---
model = None
model_version = "not_loaded"

@app.on_event("startup")
def load_model():
    global model, model_version
    try:
        mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        registry_model_name = "loan_approval_predictor"
        model_stage = "Production"
        model = mlflow.pyfunc.load_model(model_uri=f"models:/{registry_model_name}/{model_stage}")
        client = MlflowClient(tracking_uri=mlflow_tracking_uri)
        latest_version_info = client.get_latest_versions(name=registry_model_name, stages=[model_stage])[0]
        model_version = latest_version_info.version
        print(f"--- Model '{registry_model_name}' version '{model_version}' loaded successfully. ---")
    except Exception as e:
        print(f"FATAL: Could not load model from MLflow. API will not serve predictions. Error: {e}")

@app.post("/predict", tags=["Prediction"])
def predict_loan_status(application: LoanApplication):
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not available or failed to load. Check API server logs.")
    
    # --- 1. Pre-Model Guardrails (Policy & Knock-Out Rules) ---
    # These rules are fast and based on firm business policies.
    pre_model_rules = [
        rule_check_credit_history,
        rule_check_min_income,
        rule_check_lti_ratio
    ]
    for rule in pre_model_rules:
        rejection_reason = rule(application)
        if rejection_reason:
            return {"prediction": "Loan Rejected", "is_approved": False, "model_version": model_version, "reason": rejection_reason}

    try:
        # --- 2. ML Model Prediction ---
        # If the application passes basic policy checks, let the ML model evaluate the complex patterns.
        input_df = pd.DataFrame([application.dict()])
        rename_map = {
            'Gender': 'gender', 'Married': 'married', 'Dependents': 'dependents', 
            'Education': 'education', 'Self_Employed': 'self_employed', 
            'ApplicantIncome': 'applicant_income', 'CoapplicantIncome': 'coapplicant_income', 
            'LoanAmount': 'loan_amount', 'Loan_Amount_Term': 'loan_amount_term', 
            'Credit_History': 'credit_history', 'Property_Area': 'property_area'
        }
        input_df_renamed = input_df.rename(columns=rename_map)
        
        prediction_result = model.predict(input_df_renamed)
        is_approved_by_model = bool(prediction_result[0])
        
        if not is_approved_by_model:
            return {"prediction": "Loan Rejected", "is_approved": False, "model_version": model_version, "reason": f"Rejected by ML Model (Version: {model_version})"}

        # --- 3. Post-Model Guardrails (Affordability & Sanity Checks) ---
        # Run these affordability checks only if the model approves.
        post_model_rules = [
            rule_check_loan_term,
            rule_check_dti_ratio
        ]
        for rule in post_model_rules:
            rejection_reason = rule(application)
            if rejection_reason:
                return {"prediction": "Loan Rejected", "is_approved": False, "model_version": model_version, "reason": rejection_reason}

        # --- 4. Final Approval ---
        return {"prediction": "Loan Approved", "is_approved": True, "model_version": model_version, "reason": "Approved by ML Model and passed all business rule checks."}

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"An error occurred during prediction: {str(e)}")
