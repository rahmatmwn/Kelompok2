{ pkgs, ... }: {
  channel = "stable-24.05";

  # Mengaktifkan layanan Docker menggunakan sintaks yang benar untuk
  # lingkungan Firebase Studio.
  services.docker.enable = true;

  packages = [
    pkgs.python311
    pkgs.docker
    pkgs.docker-compose
    pkgs.wget
  ];

  idx = {
    extensions = [
      "ms-python.python"
      "ms-python.vscode-pylance"
      "charliermarsh.ruff"
    ];

    previews = {
      enable = true;
      previews = {
        airflow = {
          command = ["sh" "-c" "echo \"Airflow akan tersedia di port 8080 setelah Anda menjalankan 'docker-compose up'. Buka tab 'Ports' untuk menemukan URL-nya.\" && sleep infinity"];
          manager = "web";
        };
        api = {
          command = ["sh" "-c" "echo \"API akan tersedia di port 8000 setelah Anda menjalankan 'docker-compose up'. Buka tab 'Ports' untuk menemukan URL-nya.\" && sleep infinity"];
          manager = "web";
        };
        mlflow = {
          command = ["sh" "-c" "echo \"MLflow akan tersedia di port 5000 setelah Anda menjalankan 'docker-compose up'. Buka tab 'Ports' untuk menemukan URL-nya.\" && sleep infinity"];
          manager = "web";
        };
        metabase = {
          command = ["sh" "-c" "echo \"Metabase akan tersedia di port 3000 setelah Anda menjalankan 'docker-compose up'. Buka tab 'Ports' untuk menemukan URL-nya.\" && sleep infinity"];
          manager = "web";
        };
      };
    };
  };
}
