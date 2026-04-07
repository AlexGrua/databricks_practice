
## Task 4. Medallion Architecture Pipeline

```
source_volume → landing_volume → Bronze → Silver → Gold
```
---
 
## Структура
 
```
pipeline_1/
├── bronze/
│   ├── raw_events          # Bronze Delta таблица
│   ├── landing_volume      # Входящие JSON файлы
│   ├── source_volume       # Исходные 500 файлов
│   └── checkpoints_volume  # Checkpoints всех слоёв
├── silver/
│   └── silver_events       # Silver Delta таблица
└── gold/
    ├── metrics_avg         # Средние значения
    ├── metrics_min         # Минимальные значения
    ├── metrics_max         # Максимальные значения
    └── metrics_stddev      # Стандартное отклонение
```
 
---
 
## Ноутбуки
 
| Ноутбук | Описание |
|---------|----------|
| `01_bronze` | Auto Loader читает JSON из `landing_volume`, добавляет `ingestion_time`, пишет в Bronze |
| `02_copy_script` | Копирует файлы по одному из `source_volume` в `landing_volume` с задержкой 1-5 сек |
| `02_silver` | Читает Bronze, делает explode + pivot по метрикам, дедупликацию, пишет в Silver |
| `03_gold` | Читает Silver, агрегирует по device_id и окнам 1 минута, пишет в 4 Gold таблицы через MERGE |
 
---
 
## Jobs
 
### `copy_script`
Запускается руками перед демо. Копирует файлы из `source_volume` в `landing_volume`.
 
### `bronze_pipeline`
Расписание: каждые 2 минуты.
```
bronze_ingestion → silver_ingestion
```
 
### `gold_pipeline`
Триггер: обновление таблицы `pipeline_1.silver.silver_events`.
```
gold_aggregation
```

 



