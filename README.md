# HW5: мониторинг потокового антифрода

Проект обрабатывает транзакции из Kafka моделью CatBoost, сохраняет скоры в PostgreSQL и показывает технические и бизнес-показатели в Grafana. Prometheus собирает метрики каждую секунду. Airflow из прошлого задания нужен для публикации новых моделей и демонстрации смены версии без перезапуска скоринга.

Поток данных: `train.csv → transaction_simulator → Kafka (transactions) → fraud_detector → Kafka (scoring) → scoring_writer → PostgreSQL (scores)`.

## Что реализовано по условию ДЗ

| Требование | Статус и реализация |
|---|---|
| Запуск скоринга и мониторинга через Docker Compose | Реализовано в [docker-compose.yaml](docker-compose.yaml): сервисы потока, PostgreSQL, Prometheus, Grafana и Node Exporter находятся в сети `ml-scorer` |
| Работающий скоринг Kafka и растущие метрики | `fraud_detector` публикует скор и флаг фрода; оба обработчика отдают `/metrics`, счётчики обновляются при обработке потока |
| Метрики скоринга, записи и системы | Число транзакций, время обработки/записи, фрод, гистограммы скоров; CPU, память, сеть и диск через Node Exporter |
| Три дашборда с автоматическим подключением источников | JSON и provisioning лежат в `grafana/`; источники Prometheus и PostgreSQL подключаются при запуске |
| Отметка смены модели | Счётчик `model_version_updates_total{version="..."}` и аннотация **Смена модели**, в том числе поверх heatmap |
| Собственная метрика скоринга для уровня 6–7 | `scoring_near_threshold_total`: доля решений около порога, панель **Scorings near threshold**; назначение описано в конце README |
| Два алерта для уровня 8–10 | Правила **Scoring flow stopped** и **Fraud above threshold** сохранены в provisioning; пороги, проверка и оговорка для чистого запуска — ниже |
| Срез PostgreSQL по версиям модели | Таблица **Scores and fraud by model version (PostgreSQL)**: количество скоров и доля фрода по версии |
| Плотность скоров и TPS с фильтрами `us_state` и `merch` | **Не реализовано**: фильтров по штатам и мерчантам нет |
| Barplot доли фрода по `cat_id` в последних 1000 транзакциях | Панель **Fraud rate among categories, last 1000 transactions only** |
| Собственный график для понимания бизнеса | Две денежные панели: **Доля фродовых денег** и **Сумма денег, помеченных как фрод** |

## Запуск

Нужны Docker Engine с Compose v2 или Docker Desktop и файл `train.csv` из [датасета задания](https://www.kaggle.com/competitions/teta-ml-1-2025/data). Все команды выполняются из корня клонированного репозитория. Порты `2181`, `5432`, `9095`, `8080`, `8081`, `8000`, `8001`, `9090`, `3000` должны быть свободны.

### 1. Настроить окружение

```bash
cp .env.example .env
```

В `.env` замените `TRAIN_DATA_CSV` на абсолютный путь к существующему CSV. Остальные настройки можно оставить из шаблона:

```dotenv
TRAIN_DATA_CSV="/absolute/path/to/train.csv"
FRAUD_DETECTOR_THRESHOLD=0.9
FRAUD_AROUND_THRESHOLD_RADIUS=0.05
FRAUD_DETECTOR_PROM_PORT=8000
SCORE_WRITER_PROM_PORT=8001
```

Порты метрик должны совпадать с Compose и `prometheus/prometheus.yml`. Порог фрода применяется как `score > 0.9`; радиус `0.05` используется для подсчёта пограничных решений. Доступ PostgreSQL задаётся переменными `POSTGRES_*` из шаблона.

### 2. Поднять проект

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps -a
```

Первый запуск занимает несколько минут: загружаются образы и CSV, Airflow обучает первую модель. `kafka-setup` и `data_loader` — одноразовые задачи, их нормальное состояние — завершение с кодом `0`. Остальные сервисы должны работать.

Скоринг ждёт появления модели в общем томе `model_registry`; до этого его `/metrics` недоступен. Airflow автоматически запускает обучение, если `latest.txt` ещё нет. Логи запуска:

```bash
docker compose logs --tail=50 airflow fraud_detector scoring_writer transaction_simulator
```

### 3. Открыть интерфейсы

| Сервис | Адрес | Доступ по умолчанию |
|---|---|---|
| Grafana | [localhost:3000](http://localhost:3000) | `admin` / `admin` при первом запуске |
| Prometheus | [localhost:9090](http://localhost:9090) | Без авторизации |
| Метрики скоринга | [localhost:8000/metrics](http://localhost:8000/metrics) | Без авторизации |
| Метрики записи | [localhost:8001/metrics](http://localhost:8001/metrics) | Без авторизации |
| Airflow | [localhost:8081](http://localhost:8081) | `admin` / `admin` |
| Kafka UI | [localhost:8080](http://localhost:8080) | Топики `transactions` и `scoring` |
| PostgreSQL | `localhost:5432` | База `fraud_db`, пользователь `fraud_user`, пароль `fraud_pass` |

Если пароль Grafana уже менялся, он сохраняется в томе `grafana_data`.

## Дашборды и метрики

В **Grafana → Dashboards** автоматически появляются три дашборда. Для демонстрации выберите последние 15 минут и обновление `1s`.

| Дашборд | Что смотреть |
|---|---|
| [Fraud statistics](http://localhost:3000/d/ad29kxz) | Скорость и время скоринга, долю фрода, распределение скоров, отметки смены модели, пограничные решения и бизнес-панели PostgreSQL |
| [Scoring writer statistics](http://localhost:3000/d/adtkbkk) | Скорость записи, количество фрода, распределения времени записи и скоров, среднее время и p95 |
| [Node Metrics](http://localhost:3000/d/node-metrics) | **CPU Utilization**, **Memory Utilization**, **Network Traffic**, **Disk I/O** |

### Метрики скоринга

Endpoint `fraud_detector:8000/metrics`, job `fraud_detector`.

| Метрика | Тип | Панель / назначение |
|---|---|---|
| `scoring_transactions_processed_total` | Counter | **Transactions per second** — скорость успешной обработки |
| `transaction_processing_seconds` | Summary | **Seconds per transaction scoring** — среднее время вызова обработки, включая неудачные попытки |
| `fraud_score` | Histogram | **Fraud scores distribution** — heatmap распределения скоров |
| `fraud_ratio` | Gauge | **Fraud rate** — доля фрода с запуска процесса |
| `model_version_updates_total{version="..."}` | Counter | Аннотация **Смена модели** |
| `scoring_fraud_transactions_total` | Counter | **Fraud per minute** — скорость появления транзакций с флагом фрода; оговорка о единицах ниже |
| `scoring_near_threshold_total` | Counter | **Scorings near threshold** — процент транзакций со скором около порога |

### Метрики записи

Endpoint `scoring_writer:8001/metrics`, job `scoring_writer`. Метрики записи обновляются после успешного `commit` в PostgreSQL.

| Метрика | Тип | Панель / назначение |
|---|---|---|
| `scoring_writer_scores_written_total` | Counter | **Writes per second** — скорость записи |
| `scoring_writer_write_duration_seconds` | Histogram | **Write duration distribution in seconds** и **Write duration: mean and p95** — время INSERT и commit |
| `scoring_writer_fraud_written_total` | Counter | **Fraud detected** — число записанных транзакций с флагом фрода с запуска writer |
| `scoring_writer_score` | Histogram | **Written scores distribution** — распределение сохранённых скоров |

У Histogram доступны серии `_bucket`, `_sum`, `_count`, у Summary — `_sum`, `_count`. Гистограммы скоров используют границы от `0` до `1` с шагом `0.01`. Цвет нормализованных heatmap показывает долю наблюдений в интервале. Скорости вычисляются через `rate`, среднее время — как отношение скоростей прироста `_sum` и `_count`.

Node Exporter (`node-exporter:9100/metrics`, job `node`) отдаёт `node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`, `node_memory_MemTotal_bytes`, `node_network_receive_bytes_total`, `node_network_transmit_bytes_total`, `node_disk_read_bytes_total`, `node_disk_written_bytes_total`. В Docker Desktop системные показатели относятся к Linux-окружению Docker; сеть при текущем bridge-подключении — к сетевому пространству Node Exporter.

### Бизнес-панели PostgreSQL

Все четыре панели находятся на **Fraud statistics** и читают `public.scores` через источник `PostgreSQL`:

| Панель | Расчёт |
|---|---|
| **Scores and fraud by model version (PostgreSQL)** | Число записей и доля `fraud_flag = 1` по `model_version` за выбранный период |
| **Fraud rate among categories, last 1000 transactions only** | Сначала берутся последние 1000 записей всей таблицы по `created_at`, затем считается доля фрода внутри каждой категории `cat_id`. Фильтр времени дашборда не применяется |
| **Доля фродовых денег** | `SUM(amount * fraud_flag) / NULLIF(SUM(amount), 0)` за выбранный период |
| **Сумма денег, помеченных как фрод** | `SUM(amount * fraud_flag)` за выбранный период, в единицах исходного `amount` |

Writer сохраняет `transaction_id`, `score`, `fraud_flag`, `model_version`, `cat_id`, `amount` и время записи `created_at`. Сумма сохраняется для всех транзакций, чтобы вычислять долю фродовых денег. В старых строках `amount` может быть `NULL`: они не участвуют в денежных расчётах. При отсутствии сумм панели показывают «Нет данных».

Под фродом здесь понимается решение модели, а денежные показатели оценивают потенциально предотвращённые потери. Проект не подтверждает факт мошенничества и не блокирует платежи.

## Проверка требований после запуска

### Поток и заполненные панели

1. Откройте [Prometheus → Targets](http://localhost:9090/targets): `fraud_detector`, `scoring_writer` и `node` должны быть `UP`.
2. Выполните в Prometheus по очереди `scoring_transactions_processed_total` и `scoring_writer_scores_written_total`. Повторите через несколько секунд: значения должны вырасти. То же видно на страницах `/metrics`.
3. Откройте все три дашборда. Симулятор отправляет примерно 10 транзакций в секунду; после начала скоринга скорости должны быть положительными, а heatmap — заполненными. Для накопления данных подождите около минуты.
4. На **Fraud statistics** проверьте таблицу версий, категории и денежные панели. Нулевой фрод допустим; пустая SQL-панель требует проверки потока и выбранного периода.

### Публикация новой модели и аннотация

1. При работающем потоке запомните текущую версию:

   ```bash
   docker compose exec airflow cat /opt/airflow/models/latest.txt
   ```

2. В Airflow откройте `retrain_catboost`, нажмите **Trigger DAG** и дождитесь задачи `publish_model`. DAG также запускается каждую минуту. Повторите команду: версия в `latest.txt` должна измениться.
3. На **Fraud statistics** включите переключатель **Смена модели**. На следующей обработанной транзакции скоринг подхватит новую модель без перезапуска контейнера. Аннотация подписана `Модель <version>` и `Скоринг перешёл на <version>`.
4. Отметка охватывает примерно 30 секунд и остаётся в истории графика. Счётчик `model_version_updates_total` получает серию с новой версией, а таблица PostgreSQL — новую строку после записи результата.

Первая загрузка модели не считается сменой. Для этой проверки не перезапускайте `fraud_detector`. Успешный DAG не всегда меняет активную модель: если качество хуже предыдущей, `latest.txt` остаётся прежним; если следующего месяца данных нет, обучение пропускается. Смотрите лог `publish_model` и проверяйте именно изменение версии. Подробности обучения из прошлого задания находятся в `dags/` и `pipeline/retrain/`.

### Алерты

Правила находятся в [grafana/provisioning/alerting/Alerts.yaml](grafana/provisioning/alerting/Alerts.yaml). В Grafana их нужно смотреть в **Alerting → Alert rules**, папка **Alerts**, группа **Fraud detector health**. Оценка выполняется раз в минуту; условие должно сохраняться ещё минуту (`for: 1m`) перед переходом в `Firing` / `Alerting`.

| Правило | Условие |
|---|---|
| **Scoring flow stopped** | `rate(scoring_transactions_processed_total{job="fraud_detector"}[1m]) < 0.1`: менее 0.1 транзакции/с |
| **Fraud above threshold** | `fraud_ratio > 0.01`: доля фрода выше 1% **с запуска процесса**, а не за последнюю минуту |

**Оговорка для чистой установки:** в обоих правилах указан получатель `empty`, но его определение не сохранено в provisioning. Правила находятся в репозитории, однако перенос настройки Alerting на пустой том Grafana требует устранения этой зависимости: добавить provisioning контактной точки либо убрать привязку к ней из правил. Наличие `empty` в старой БД Grafana не гарантирует воспроизводимость на новой установке. Доставка в Telegram или почту для ДЗ не требуется.

Проверка остановки потока:

```bash
docker compose stop transaction_simulator
```

Дождитесь обработки очереди Kafka и ещё примерно 2–3 минут на окно запроса и оценку правила. **Scoring flow stopped** должен перейти в состояние срабатывания. Возобновить поток:

```bash
docker compose start transaction_simulator
```

Проверка высокой доли фрода: временно поставьте в `.env` `FRAUD_DETECTOR_THRESHOLD=-1` и пересоздайте только скоринг:

```bash
docker compose up -d --no-deps --force-recreate fraud_detector
```

При таком пороге все новые скоры помечаются как фрод; через оценки правила **Fraud above threshold** должен сработать. Верните прежний порог (в шаблоне `0.9`) и повторите команду. Эти демонстрационные транзакции сохраняются в PostgreSQL с тестовыми флагами. При недоступности метрик правила настроены на `NoData`, при ошибке запроса — на `Error`.

## Где лежат настройки и как применить изменения

| Файл / каталог | Назначение |
|---|---|
| [prometheus/prometheus.yml](prometheus/prometheus.yml) | Три цели сбора, интервал `1s` |
| [grafana/provisioning/datasources/](grafana/provisioning/datasources/) | Prometheus (`fg3x097p7l3i8c`) и PostgreSQL (`postgres-scores`); доступ к БД берётся из окружения Grafana |
| [grafana/provisioning/dashboards/dashboards.yaml](grafana/provisioning/dashboards/dashboards.yaml) | Загрузка JSON из `/var/lib/grafana/dashboards`, опрос изменений каждые 10 секунд |
| [grafana/dashboards/](grafana/dashboards/) | Три JSON дашборда с панелями, запросами и аннотациями |
| [grafana/provisioning/alerting/Alerts.yaml](grafana/provisioning/alerting/Alerts.yaml) | Два правила алертов |

Изменения через UI Grafana сохраняются во внутреннюю БД, а не в репозиторий. Для переноса экспортируйте **Classic JSON** и замените соответствующий файл, сохранив UID источников. После изменения YAML provisioning перезапустите Grafana:

```bash
docker compose restart grafana
```

После обновления кода скоринга или writer пересоберите их:

```bash
docker compose up -d --build --no-deps fraud_detector scoring_writer
```

Writer сам создаёт таблицу `scores` и добавляет недостающие колонки `model_version`, `cat_id`, `amount`. Для применения новых переменных окружения Grafana используйте `docker compose up -d --no-deps --force-recreate grafana`.

Если панели пустые, сначала проверьте Targets и логи обработчиков. Если скоринг ждёт модель, проверьте Airflow. Если метрики уже есть, расширьте диапазон Grafana и дождитесь накопления данных. Для SQL-панелей проверьте подключение **Connections → Data sources → PostgreSQL** и запуск `scoring_writer`.

Остановка с сохранением данных: `docker compose down`. Команда `docker compose down -v` дополнительно удаляет БД, модели и историю метрик; следующий запуск потребует загрузки CSV и первой модели заново.

## Дополнения к обязательному набору

Конкретные метрики и панели, выбранные для проекта:

| Добавление | Зачем нужно и где смотреть |
|---|---|
| `scoring_near_threshold_total` | Считает решения с `abs(score - threshold) < radius` (`0.05` по умолчанию). Процент таких решений на **Fraud statistics → Scorings near threshold** показывает чувствительность решений к изменению порога. Это собственная метрика для требования уровня 6–7 |
| `scoring_fraud_transactions_total` | Позволяет следить за скоростью появления фрода, а не только за его накопленной долей. Панель **Fraud statistics → Fraud per minute** помогает заметить рост потока подозрительных операций |
| Среднее время и p95 записи | **Scoring writer statistics → Write duration: mean and p95** дополняет гистограмму и помогает увидеть замедление наиболее долгих записей |
| Доля и сумма фродовых денег | **Fraud statistics → Доля фродовых денег / Сумма денег, помеченных как фрод** показывают денежный масштаб решений модели. Это реализация свободного пункта ДЗ о бизнес-графике; конкретные денежные показатели условием не заданы |

**Единицы панели Fraud per minute:** текущий запрос `rate(scoring_fraud_transactions_total[1m])` возвращает события в секунду, хотя панель подписана «в минуту». Для значения в минуту требуется `60 * rate(scoring_fraud_transactions_total[1m])`; одна настройка единиц `opm` этот пересчёт не выполняет.

Дополнительно сохранён DAG **data_quality** из проекта обучения: раз в 5 минут он проверяет текущий датасет и создаёт отчёт `/opt/airflow/dq_reports/latest.html`. Это не входит в перечисленные требования мониторинга данного ДЗ.
