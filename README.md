# Databricks Practice: Delta Lake 

Репозиторий для выполнения практических заданий по работе с Databricks и технологией Delta Lake.

## Task 1: Основы Delta Lake и DML
В рамках этого задания реализованы следующие этапы:

1. **Ingestion**: Загрузка данных из CSV (Volumes) в управляемую Delta-таблицу.
2. **DML Operations**: Имитация реальной работы с данными (Update, Delete, Insert).
3. **Time Travel**: Использование `VERSION AS OF` для восстановления данных после случайного удаления.
4. **Schema Evolution**: Демонстрация ошибки при несовпадении схем и её решение через `mergeSchema`.
5. **Upsert (Merge)**: Слияние новых данных с существующей таблицей (обработка обновлений и новых записей).
6. **Optimization**: Применение Liquid Clustering (`CLUSTER BY`) и выполнение команды `OPTIMIZE` для повышения производительности.



## Task 2. Анализ данных и интерактивные отчеты NYC TAXI.
Фокус на обработке данных (ETL/EDA) и создании инструментов для бизнес-анализа:
1. **Интерактивность**: Реализация фильтрации данных через виджеты Databricks (даты, районы, категории).
2. **Data Cleaning**: Очистка датасета от аномалий (нулевые дистанции, некорректные временные метки).
3. **Feature Engineering**: Создание производных метрик (длительность поездки, час пик, выручка на милю — RPM).
4. **Business Insights**: 
    * Анализ влияния погодных условий на спрос (кейс со снежной бурей 23-24 января).
    * Сегментация рынка по дистанции поездок.
    * Определение наиболее прибыльных часов для водителей (анализ операционной эффективности).
5. **Визуализация**: Подготовка агрегированных данных для встроенных дашбордов Databricks.


## Task 3. Weather Data Pipeline

Автоматизированный конвейер обработки погодных данных на Databricks Serverless по принципам Medallion Architecture.

**Источник:** `samples.accuweather` — почасовые прогнозы, ежедневные прогнозы и исторические данные по 50 городам.

---

## Структура каталога

```
weather_project/
├── bronze/       # сырые данные из источника
├── silver/       # очищенные и трансформированные данные
├── gold/         # аналитические витрины и ML фичи
└── monitoring/   # результаты DQ-проверок и отчёты о запусках
```

---

## Слои

### Bronze
Копирование трёх таблиц из источника без изменений + колонка `ingestion_ts`.

| Таблица | Строк |
|---|---|
| bronze.hourly | 6 850 |
| bronze.daily_forecast | 750 |
| bronze.daily_historical | 800 |

### Silver
**hourly_clean** — фильтрация null-строк, нормализация типов, добавление `event_date` и `is_rain_event`.

**daily_clean** — конвертация boolean-like строк → boolean/double, удаление corrupted records.

### Gold
**daily_city_metrics** — avg temperature, max wind speed, avg precipitation probability по городу и дате.

**weather_trends** — 7-дневное скользящее среднее температуры (Window функция), rain frequency.

**ml_features** — lag temperature (1 день), rolling avg temperature (7 дней), rain last 3 days indicator.

### Monitoring
**quality_checks** — 4 DQ-проверки (hourly rows > 0, daily rows > 0, temperature range, date nulls).

**pipeline_reports** — processed rows, cities count, last processing date.

---

## Workflow DAG

```
01_ingest_bronze
       ↓
02_clean_hourly ──┐
                  ├──→ 04_quality_checks → 05_gold_tables → 06_features → 07_reporting
03_clean_daily ───┘
```

`02_clean_hourly` и `03_clean_daily` выполняются параллельно. Настройки: retries = 2, timeout = 10 мин.

Значения между tasks передаются через `dbutils.jobs.taskValues`. Общее время выполнения: **~53 секунды**.
