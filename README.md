# Proyek ELT & Data Preparation for Non-Performing Loan (NPL) Analysis

Proyek ini mendemonstrasikan alur kerja ELT serta terdapat tambahan menggunakan MLOps (Machine Learning Operations) lengkap, mulai dari pemrosesan data, pelatihan model, hingga penyajian model sebagai API untuk prediksi real-time.



Arsitektur ini menggunakan Docker untuk mengorkestrasi beberapa Service:
- **Airflow**: Sebagai orkestrator untuk pipeline data (ELT).
- **PostgreSQL**: Berfungsi ganda sebagai database backend untuk Airflow dan sebagai Data Warehouse.
- **MLflow**: Untuk melacak eksperimen, mengelola artefak model, dan sebagai Model Registry.
- **FastAPI**: Sebagai API server yang melayani model "Production" dari MLflow untuk membuat prediksi.

---

## 📋 Daftar Isi

- [Arsitektur](#-arsitektur)
- [Fitur Utama](#-fitur-utama)
- [Prasyarat](#-prasyarat)
- [Pemecahan Masalah (Troubleshooting)](#%EF%B8%8F-pemecahan-masalah-troubleshooting)
- [🚀 Panduan Setup & Menjalankan Proyek](#-panduan-setup--menjalankan-proyek)
  - [Langkah 1: Masuk ke Direktori Proyek](#langkah-1-masuk-ke-direktori-proyek)
  - [Langkah 2: Bangun dan Jalankan Semua Layanan](#langkah-2-bangun-dan-jalankan-semua-layanan)
  - [Langkah 3: Jalankan Pipeline Data (ELT) via Airflow](#langkah-3-jalankan-pipeline-data-elt-via-airflow)
  - [Langkah 4: Latih Model Machine Learning](#langkah-4-latih-model-machine-learning)
  - [Langkah 5: Promosikan Model ke "Production" di MLflow](#langkah-5-promosikan-model-ke-production-di-mlflow)
  - [Langkah 6: Bangun Ulang API untuk Memuat Model Produksi](#langkah-6-bangun-ulang-api-untuk-memuat-model-produksi)
  - [Langkah 7: Uji API Prediksi](#langkah-7-uji-api-prediksi)
- [🌐 Akses Portal Layanan](#-akses-portal-layanan)
- [📂 Struktur Proyek](#-struktur-proyek)

---

## 🏗️ Arsitektur

```
1. Airflow (DAG)
   |
   +--> Menjalankan ELT:
   |    - Generate data CSV
   |    - Load ke Staging Table (PostgreSQL)
   |    - Transform ke DWH (PostgreSQL)
   |
   V
2. ML Training (Script Python)
   |
   +--> Mengambil data dari DWH (PostgreSQL)
   |
   V
3. MLflow
   |
   +--> Menerima & menyimpan pipeline model
   |    (Model Registry: Version -> Staging -> Production)
   |
   V
4. API (FastAPI)
   |
   +--> Memuat model "Production" dari MLflow
   |
   V
5. User / Client
   |
   +--> Mengirim request ke API
   |
   <--+ Menerima hasil prediksi
```

---

## ✨ Fitur Utama

- **Pipeline Data ELT**: Proses Extract, Load, Transform yang andal diorkestrasi oleh Airflow.
- **Manajemen Model**: Pelacakan eksperimen dan pengelolaan siklus hidup model (staging, production, archived) menggunakan MLflow.
- **Model sebagai Pipeline**: Model yang disimpan bukan hanya *classifier*, tetapi sebuah *pipeline* scikit-learn lengkap yang mencakup preprocessing.
- **Penyajian Model (Serving)**: API berbasis FastAPI yang secara otomatis memuat model versi "Production".
- **Reproducibility**: Seluruh lingkungan terisolasi dan dapat direproduksi berkat Docker dan `docker-compose`.

---

## 🔧 Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/)
- [Docker Compose](https://docs.docker.com/compose/install/)

Pastikan Docker Engine sedang berjalan di sistem Anda.

---

## ⚠️ Pemecahan Masalah (Troubleshooting)

### Error Izin Docker (`Permission Denied`)

Jika Anda menjalankan perintah `docker` atau `docker-compose` di Linux/macOS dan mendapatkan error yang berkaitan dengan izin (seperti `permission denied while trying to connect to the Docker daemon socket`), itu berarti pengguna Anda tidak memiliki hak untuk mengakses Docker daemon.

**Solusi Cepat (Linux/macOS):** Tambahkan pengguna Anda ke grup `docker`.

```bash
sudo usermod -aG docker ${USER}
```
> **Penting:** Setelah menjalankan perintah ini, Anda harus **logout dan login kembali**, atau restart sistem Anda sepenuhnya agar perubahan grup ini diterapkan.

---

## 🚀 Panduan Setup & Menjalankan Proyek

Ikuti langkah-langkah ini secara berurutan untuk menjalankan keseluruhan alur kerja.

### Langkah 1: Masuk ke Direktori Proyek

*Catatan: Semua perintah selanjutnya harus dijalankan dari dalam direktori `project-root`.*

```bash
cd project-root
```

### Langkah 2: Bangun dan Jalankan Semua Layanan

Perintah ini akan membuat *image* untuk setiap layanan dan menjalankannya di *background*.

```bash
docker-compose up -d 
```
> **Tunggu beberapa menit** agar semua layanan (khususnya Airflow dan PostgreSQL) sepenuhnya siap. Anda dapat memeriksa status dengan `docker-compose ps`.

### Langkah 3: Jalankan Pipeline Data (ELT) via Airflow

1.  Buka **Airflow UI** di browser: `http://localhost:8080` (login: `airflow`/`airflow`).
2.  Aktifkan DAG `loan_processing_dag` dengan meng-klik tombol *toggle*.
3.  Klik nama DAG, lalu klik tombol **Play (▶️)** di kanan atas untuk memicu eksekusi.
4.  Tunggu hingga semua proses di DAG selesai (berwarna hijau tua).

### Langkah 4: Latih Model Machine Learning

Jalankan skrip pelatihan untuk mengambil data dari DWH, melatih, dan menyimpan model ke MLflow.

```bash
docker exec project-root-airflow-scheduler-1 python /opt/airflow/ml/train_model.py
```
> Outputnya akan menunjukkan bahwa model telah terdaftar di MLflow sebagai **Version 1**.

### Langkah 5: Promosikan Model ke "Production" di MLflow

1.  Buka **MLflow UI** di browser: `http://localhost:5000`
2.  Klik tab **Models** > `loan_approval_predictor` > **Version 1**.
3.  Klik tombol **Stage** dan pilih **Transition To ▸ Production**.
4.  Tambahkan komentar dan klik OK.

### Langkah 6: Bangun Ulang API untuk Memuat Model Produksi

Layanan API memuat model "Production" saat container dibangun. Karena kita baru saja mempromosikan model baru, kita perlu **membangun ulang (rebuild)** container API agar ia memuat model tersebut. Perintah `restart` saja tidak cukup.

```bash
docker-compose up -d --build api
```

#Kalau model tidak terupdate jalankan

```bash
docker-compose up -d --force-recreate --build api
```
> Perintah ini secara spesifik akan membangun ulang image untuk layanan `api` dan memastikannya menggunakan kode dan model terbaru.

### Langkah 7: Uji API Prediksi

Kirim contoh aplikasi pinjaman ke API Anda dan lihat hasilnya.

```bash
curl -X POST "http://localhost:8000/predict" \
-H "Content-Type: application/json" \
-d \'{
    "Gender": "Male",
    "Married": "Yes",
    "Dependents": "0",
    "Education": "Graduate",
    "Self_Employed": "No",
    "ApplicantIncome": 5400,
    "CoapplicantIncome": 2100,
    "LoanAmount": 165000,
    "Loan_Amount_Term": 360.0,
    "Credit_History": 1.0,
    "Property_Area": "Urban"
}\'
```

Anda akan menerima respons JSON yang berisi prediksi:
```json
{"prediction":"Loan Rejected","is_approved":false}
```

**Selamat! Anda telah berhasil menjalankan seluruh pipeline MLOps.**

---

## 🌐 Akses Portal Layanan

-   **Airflow UI**: `http://localhost:8080` (user: `airflow`, pass: `airflow`)
-   **MLflow UI**: `http://localhost:5000`
-   **API Documentation (Swagger UI)**: `http://localhost:8000/docs`

---

## 📂 Struktur Proyek

Struktur ini mencerminkan file-file yang ada di dalam repositori.

```
.
├── project-root/
│   ├── api/
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── dags/
│   │   └── loan_processing_dag.py
│   ├── ml/
│   │   └── train_model.py
│   ├── sql/
│   │   ├── create_dwh_tables.sql
│   │   └── create_staging_table.sql
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow
│   ├── Dockerfile.api
│   └── Dockerfile.mlflow
└── README.md
```
