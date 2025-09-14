
import os
import pandas as pd
import mlflow
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
    title="Loan Prediction API V2",
    description="API yang menggunakan ML pipeline (preprocessor + model) dari MLflow Registry.",
    version="2.0.2"
)

# --- Model Loading on Startup ---
model = None
mlflow_tracking_uri = "not set"

@app.on_event("startup")
def load_model():
    """Memuat pipeline model dari MLflow Model Registry saat aplikasi dimulai."""
    global model, mlflow_tracking_uri
    
    try:
        mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        
        model_name = "loan_approval_predictor"
        model_stage = "Production"
        
        # URI untuk memuat model 'Production'
        model_uri = f"models:/{model_name}/{model_stage}"
        
        # Memuat model (yang sekarang adalah sebuah pipeline lengkap)
        model = mlflow.pyfunc.load_model(model_uri=model_uri)
        
        print(f"--- Model '{model_name}' (Stage: {model_stage}) loaded successfully. ---")
        if model:
            # Menampilkan metadata, sangat berguna untuk debugging
            print(f"--- MODEL METADATA: {model.metadata} ---")

    except Exception as e:
        # Jika gagal, log error. Prediksi tidak akan bisa dilakukan.
        print(f"FATAL: Could not load model from MLflow. API will not be able to serve predictions. Error: {e}")

# --- API Endpoints ---

@app.get("/", tags=["Health Check"])
def read_root():
    """Endpoint untuk memeriksa status API dan model."""
    return {
        "status": "API is running",
        "model_status": "loaded" if model is not None else "failed_to_load",
        "mlflow_tracking_uri": mlflow_tracking_uri
    }

@app.post("/predict", tags=["Prediction"])
def predict_loan_status(application: LoanApplication):
    """
    Menerima data aplikasi, melakukan prediksi menggunakan pipeline yang sudah dimuat,
    dan mengembalikan hasilnya.
    """
    if model is None:
        raise HTTPException(
            status_code=503,
            detail="Model is not available. Check API server logs for loading errors."
        )
    try:
        # 1. Konversi input Pydantic ke Pandas DataFrame.
        input_df = pd.DataFrame([application.dict()])

        # 2. FIX: Ubah nama kolom agar sesuai dengan yang diharapkan model (snake_case)
        rename_map = {
            'Gender': 'gender', 'Married': 'married', 'Dependents': 'dependents', 
            'Education': 'education', 'Self_Employed': 'self_employed', 
            'ApplicantIncome': 'applicant_income', 'CoapplicantIncome': 'coapplicant_income', 
            'LoanAmount': 'loan_amount', 'Loan_Amount_Term': 'loan_amount_term', 
            'Credit_History': 'credit_history', 'Property_Area': 'property_area'
        }
        input_df.rename(columns=rename_map, inplace=True)

        # 3. Lakukan prediksi.
        prediction_result = model.predict(input_df)
        
        # 4. Format output yang lebih informatif
        is_approved = bool(prediction_result[0])
        prediction_text = "Loan Approved" if is_approved else "Loan Rejected"
        
        return {
            "prediction": prediction_text,
            "is_approved": is_approved
        }

    except Exception as e:
        # Menangkap error yang mungkin terjadi selama proses prediksi.
        raise HTTPException(status_code=500, detail=f"An error occurred during prediction: {str(e)}")
