# 이 파일은 compose configs로 실행 권한 없이 마운트되어 엔트리포인트가 source한다.
# 그래서 set -e/-u 같은 셸 옵션을 바꾸지 않는다 (엔트리포인트 셸에 남는다).
# 실패는 ON_ERROR_STOP=1이 잡고, 비밀번호 변수 누락은 compose의 ${VAR:?}가 막는다.

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname postgres \
  -v eonje_pw="$EONJE_DB_PASSWORD" \
  -v airflow_pw="$AIRFLOW_DB_PASSWORD" << 'SQL'
CREATE ROLE eonje LOGIN PASSWORD :'eonje_pw';
CREATE DATABASE eonje OWNER eonje;

CREATE ROLE airflow LOGIN PASSWORD :'airflow_pw';
CREATE DATABASE airflow OWNER airflow;
SQL
