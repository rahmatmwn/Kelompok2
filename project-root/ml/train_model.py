
import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import mlflow
import mlflow.sklearn

def train_loan_prediction_model():
    """
    Membangun pipeline preprocessing dan model, melatihnya,
    dan mencatat pipeline lengkap ke MLflow.
    """
    # --- 1. Setup MLflow ---
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("loan_approval_prediction")

    # --- 2. Mengambil Data ---
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow')
    query = """
    SELECT 
        gender, married, dependents, education, self_employed, credit_history,
        property_area, applicant_income, coapplicant_income, loan_amount, loan_amount_term,
        loan_status_approved
    FROM fact_loans f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    JOIN dim_location loc ON f.location_id = loc.location_id;
    """
    df = pd.read_sql(query, engine)
    df.dropna(subset=['loan_status_approved'], inplace=True)

    X = df.drop('loan_status_approved', axis=1)
    y = df['loan_status_approved']

    # Mengidentifikasi tipe kolom
    categorical_features = X.select_dtypes(include=['object']).columns
    numerical_features = X.select_dtypes(include=['number']).columns

    # --- 3. Membangun Preprocessing Pipeline ---
    # Pipeline untuk data numerik: imputasi (mengisi nilai kosong) lalu scaling
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler())
    ])

    # Pipeline untuk data kategorikal: imputasi lalu one-hot encoding
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])

    # Menggabungkan kedua pipeline di atas dengan ColumnTransformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numerical_features),
            ('cat', categorical_transformer, categorical_features)
        ])

    # --- 4. Membangun Model Pipeline Lengkap ---
    # Menggabungkan preprocessor dengan model classifier
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', RandomForestClassifier(random_state=42))
    ])

    # --- 5. Split Data ---
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # --- 6. Melatih Pipeline dan Tracking dengan MLflow ---
    with mlflow.start_run() as run:
        # Definisikan parameter untuk Grid Search atau tuning (opsional)
        params = {
            'classifier__n_estimators': 100,
            'classifier__max_depth': 10
        }
        
        # Set dan log parameter
        model_pipeline.set_params(**params)
        mlflow.log_params(params)

        # Melatih pipeline lengkap
        model_pipeline.fit(X_train, y_train)
        
        # Evaluasi
        y_pred = model_pipeline.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred, output_dict=True)
        
        # Log Metrik
        mlflow.log_metric("accuracy", accuracy)
        if 'True' in report: # Handle case where a class might not be in the test set
            mlflow.log_metric("precision_true", report['True']['precision'])
            mlflow.log_metric("recall_true", report['True']['recall'])
            mlflow.log_metric("f1_score_true", report['True']['f1-score'])
        
        print(f"Run ID: {run.info.run_id}")
        print(f"Accuracy: {accuracy}")
        print(classification_report(y_test, y_pred))

        # --- 7. Log Pipeline Lengkap sebagai Model ---
        # Ini adalah perubahan KUNCI. Kita menyimpan seluruh pipeline.
        mlflow.sklearn.log_model(
            sk_model=model_pipeline,
            artifact_path="model_pipeline", # Beri nama yang jelas
            registered_model_name="loan_approval_predictor"
        )
        print("\nPipeline lengkap (preprocessor + model) berhasil dilatih dan disimpan di MLflow.")


if __name__ == '__main__':
    train_loan_prediction_model()
