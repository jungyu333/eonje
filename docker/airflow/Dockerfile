FROM apache/airflow:3.3.2-python3.11

# docker: DockerOperator로 태스크 이미지를 실행
# git:    운영의 GitDagBundle (CD PR에서 사용)
# constraints 파일이 provider 버전을 이 Airflow 버전에 맞게 고정한다
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-docker \
      apache-airflow-providers-git \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
