
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

## Ключевые решения
 
### Инкрементальное чтение
`readStream` + checkpoint. Каждый слой читает только новые транзакции Delta таблицы предыдущего слоя. Checkpoint хранит версию Delta на которой остановился.
 
### foreachBatch
Используется в Silver и Gold вместо чистого стриминга. Позволяет делать сложные трансформации (`groupBy`, `pivot`, `MERGE INTO`) внутри стримингового фреймворка.
 
### Дедупликация в Silver
`dropDuplicates(["device_id", "event_time"])` внутри `foreachBatch` — одно устройство в один момент времени = одна строка.
 
### Late-arriving data в Gold
MERGE INTO вместо append. Если новые данные пришли за уже обработанное временное окно — агрегат пересчитывается, дубликаты не создаются.
 
### availableNow trigger
Вместо бесконечного стриминга на Serverless. Джоба обрабатывает все доступные данные и останавливается, экономя ресурсы.
 
---
 
## Запуск
 
1. Запустить джобу `copy_script`
2. Запустить джобу `bronze_pipeline`
3. Запустить джобу `gold_pipeline`
 



