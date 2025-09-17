
import os
import pandas as pd
import mlflow
from mlflow.tracking import MlflowClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

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

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Loan Prediction API V4",
    description="API to serve a loan prediction model from the MLflow Registry. The currently loaded model will be shown here upon startup.",
    version="4.0.0"
)

# --- Global variables for model details ---
model = None
mlflow_tracking_uri = "not set"
model_name = "not_loaded"
model_version = "not_loaded"

@app.on_event("startup")
def load_model():
    """
    Load the model from the MLflow Model Registry on application startup.
    Dynamically updates the API's description with the loaded model's version.
    """
    global model, mlflow_tracking_uri, model_name, model_version, app

    try:
        mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        
        registry_model_name = "loan_approval_predictor"
        model_stage = "Production"

        # --- NEW ROBUST LOGIC: Use MlflowClient to get model info first ---
        print(f"--- Initializing MLflow Client to query registry at {mlflow_tracking_uri} ---")
        client = MlflowClient()
        
        # Get the latest version of the model in the specified stage
        latest_versions = client.get_latest_versions(name=registry_model_name, stages=[model_stage])
        
        if not latest_versions:
            raise RuntimeError(f"No model named '{registry_model_name}' found in stage '{model_stage}'.")

        # Extract details from the model version object
        prod_model_version = latest_versions[0]
        model_name = prod_model_version.name
        model_version = prod_model_version.version
        model_uri = prod_model_version.source # This is the direct URI to the model artifacts

        print(f"--- Found model '{model_name}' version '{model_version}' in stage '{model_stage}'. ---")
        print(f"--- Loading model from URI: {model_uri} ---")
        
        # Load the model using the specific URI
        model = mlflow.pyfunc.load_model(model_uri=model_uri)

        # Dynamically update the app's description
        app.description = (
            f"API serving the **{model_name}** model (Version: **{model_version}**, Stage: **{model_stage}**) "
            "from the MLflow Registry. Use the /predict endpoint to get a loan approval prediction."
        )
        
        print(f"--- Model '{model_name}' version '{model_version}' loaded successfully. ---")
        print(f"--- API description updated. Swagger UI at /docs will show model details. ---")

    except Exception as e:
        print(f"FATAL: Could not load model from MLflow. API will not be able to serve predictions. Error: {e}")
        app.description = f"API startup failed: Could not load a '{model_stage}' model from MLflow. Please check the logs."


# --- API Endpoints ---

@app.get("/", tags=["Health Check"])
def read_root():
    return {"status": "API is running", "model_status": "loaded" if model is not None else "failed_to_load"}

@app.get("/info", tags=["Model Info"])
def get_model_info():
    """Returns detailed information about the currently loaded model."""
    if model is None:
         raise HTTPException(status_code=503, detail="Model is not available. Check API server logs for loading errors.")
    return {
        "model_name": model_name,
        "model_version": model_version,
        "model_stage": "Production",
        "mlflow_tracking_uri": mlflow_tracking_uri
    }

@app.post("/predict", tags=["Prediction"])
def predict_loan_status(application: LoanApplication):
    """
    Receives application data, makes a prediction using the loaded pipeline,
    and returns the result.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not available. Check API server logs.")
    try:
        input_df = pd.DataFrame([application.dict()])
        rename_map = {
            'Gender': 'gender', 'Married': 'married', 'Dependents': 'dependents', 
            'Education': 'education', 'Self_Employed': 'self_employed', 
            'ApplicantIncome': 'applicant_income', 'CoapplicantIncome': 'coapplicant_income', 
            'LoanAmount': 'loan_amount', 'Loan_Amount_Term': 'loan_amount_term', 
            'Credit_History': 'credit_history', 'Property_Area': 'property_area'
        }
        input_df.rename(columns=rename_map, inplace=True)
        
        prediction_result = model.predict(input_df)
        is_approved = bool(prediction_result[0])
        prediction_text = "Loan Approved" if is_approved else "Loan Rejected"
        
        return {
            "prediction": prediction_text,
            "is_approved": is_approved,
            "model_version": model_version
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred during prediction: {str(e)}")
