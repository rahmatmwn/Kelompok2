# Penjelasan Struktur File dan Folder

Dokumen ini menjelaskan fungsi dari setiap file dan folder dalam proyek ini, serta tujuan dari pemisahan folder-folder tersebut.

## `project-root/`

Ini adalah folder utama proyek yang berisi semua komponen inti.

- **`docker-compose.yml`**: File ini adalah pusat dari infrastruktur proyek. Ini mendefinisikan dan mengonfigurasi semua layanan yang dibutuhkan untuk menjalankan aplikasi, seperti scheduler dan webserver Airflow, server tracking MLflow, FastAPI, dan database PostgreSQL. File ini juga mengatur jaringan dan volume yang diperlukan agar layanan-layanan ini dapat berkomunikasi satu sama lain.

- **`Dockerfile.airflow`**: Dockerfile ini digunakan untuk membangun image Docker khusus untuk Airflow. Ini menginstal paket-paket Python yang diperlukan dan mengatur lingkungan Airflow.

- **`Dockerfile.api`**: Dockerfile ini membangun image untuk aplikasi FastAPI. Ini menginstal dependensi Python dan menyalin kode aplikasi ke dalam image.

- **`Dockerfile.mlflow`**: Dockerfile ini digunakan untuk membangun image MLflow.

- **`requirements.txt`**: File ini mendaftar semua dependensi Python untuk keseluruhan proyek.

### `api/`

Folder ini berisi kode sumber untuk aplikasi FastAPI yang menyajikan model machine learning.

- **`main.py`**: Ini adalah titik masuk utama untuk API. Skrip ini memuat model yang sudah siap produksi dari MLflow dan menyediakan endpoint untuk melakukan prediksi.

- **`requirements.txt`**: File ini berisi daftar dependensi Python yang khusus dibutuhkan oleh API, seperti FastAPI dan Uvicorn.

### `dags/`

Folder ini berisi *Directed Acyclic Graphs* (DAGs) untuk Airflow.

- **`loan_processing_dag.py`**: DAG ini mendefinisikan seluruh alur kerja ELT (Extract, Load, Transform). Ini mencakup tugas-tugas untuk menghasilkan data dummy, memuatnya ke tabel staging, lalu mentransformasi dan memuatnya ke dalam data warehouse.

### `ml/`

Folder ini berisi skrip untuk melatih model machine learning.

- **`train_model.py`**: Skrip ini bertanggung jawab untuk melatih model prediksi persetujuan pinjaman. Ini menggunakan MLflow untuk mencatat eksperimen, melacak performa model, dan mendaftarkan model yang telah dilatih.

### `sql/`

Folder ini berisi semua skrip SQL yang digunakan dalam proyek.

- **`create_staging_table.sql`**: Skrip ini membuat tabel staging di database PostgreSQL, yang digunakan untuk menyimpan data mentah sementara sebelum ditransformasi dan dimuat ke dalam data warehouse.

- **`create_dwh_tables.sql`**: Skrip ini membuat tabel-tabel akhir untuk data warehouse.

## `.idx/`

Folder ini berisi file-file konfigurasi yang spesifik untuk lingkungan pengembangan.

- **`dev.nix`**: File ini digunakan oleh Nix untuk menciptakan lingkungan pengembangan yang reprodusibel. Ini menentukan semua paket dan dependensi yang diperlukan untuk proyek.

- **`airules.md`**: File ini berisi aturan dan panduan untuk asisten AI.

## Direktori Utama

- **`README.md`**: Dokumentasi utama untuk proyek, memberikan gambaran umum dan instruksi tentang cara mengatur dan menjalankan proyek.

- **`dockerd.log`**: File ini berisi log dari daemon Docker.

- **`STRUKTUR_FILE.md`**: File ini, yang sedang Anda baca, menjelaskan struktur dari proyek ini.
