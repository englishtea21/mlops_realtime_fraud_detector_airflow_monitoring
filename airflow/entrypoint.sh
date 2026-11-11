#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  /opt/airflow/models \
  /opt/airflow/training_data \
  /opt/airflow/dq_reports \
  /opt/airflow/airflow_state \
  /opt/airflow/logs \
  /opt/airflow/dags

# Каталоги общие с data_loader и fraud_detector, которые работают от root.
chmod 777 \
  /opt/airflow/models \
  /opt/airflow/training_data \
  /opt/airflow/dq_reports \
  /opt/airflow/airflow_state \
  /opt/airflow/logs

export PYTHONPATH="/opt/airflow${PYTHONPATH:+:$PYTHONPATH}"
export AIRFLOW__CORE__EXECUTOR="${AIRFLOW__CORE__EXECUTOR:-SequentialExecutor}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:////opt/airflow/airflow_state/airflow.db}"
export AIRFLOW__CORE__LOAD_EXAMPLES="${AIRFLOW__CORE__LOAD_EXAMPLES:-false}"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="${AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION:-false}"

AIRFLOW_USER_NAME="${_AIRFLOW_WWW_USER_USERNAME:-admin}"
AIRFLOW_USER_PASSWORD="${_AIRFLOW_WWW_USER_PASSWORD:-admin}"

# В образе нет gosu: официальный entrypoint не понижает права, контейнер стартует от root.
run_as_airflow() {
  /usr/sbin/runuser --preserve-environment --user airflow -- "$@"
}

# Официальный entrypoint создаёт пользователя при каждом старте и падает, если он уже есть.
unset _AIRFLOW_DB_MIGRATE
unset _AIRFLOW_WWW_USER_CREATE

run_as_airflow airflow db migrate
run_as_airflow airflow users create \
  --username "${AIRFLOW_USER_NAME}" \
  --password "${AIRFLOW_USER_PASSWORD}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  || true

# Если модели нет, ждём регистрации DAG и успешного запуска обучения.
(
  for i in $(seq 1 60); do
    if [ -s /opt/airflow/models/latest.txt ]; then
      bootstrap_model_version="$(cat /opt/airflow/models/latest.txt)"
      if [ -s "/opt/airflow/models/my_catboost_${bootstrap_model_version}.cbm" ]; then
        exit 0
      fi
    fi

    # Проверяем конкретный DAG: успешный `dags list` ещё не означает,
    # что scheduler зарегистрировал retrain_catboost в metadata DB.
    if run_as_airflow airflow dags unpause retrain_catboost >/dev/null 2>&1 &&
       run_as_airflow airflow dags trigger retrain_catboost; then
      echo "Активной модели нет: обучение retrain_catboost запущено."
      exit 0
    fi

    sleep 2
  done
  echo "Не удалось запустить первое обучение. Проверьте регистрацию DAG retrain_catboost и логи Airflow." >&2
) &

exec /usr/sbin/runuser --preserve-environment --user airflow -- /entrypoint standalone
