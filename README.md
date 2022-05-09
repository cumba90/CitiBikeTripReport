# Структура функциональных каталогов и файлов:
<pre>
.
│   docker-compose-LocalExecutor.yml
│   LICENSE
│   README.md
│   __init__.py
│
├───config
│   │   airflow.cfg
│   │   dag_config.py
│   │   __init__.py
│
├───dags
│   │   CitiBikeTripReport.py
│   │   __init__.py
│   │
│   ├───metadata
│   │       processing_files.txt
│   │
│   ├───programs
│   │   │   Loader.py
│   │   │   yaS3Saver.py
│   │   │   yaS3Sensor.py
│   │   │   __init__.py
│
├───database
│   └───scripts
│       │   create_database.sql
│       │
│       └───db
│           ├───dm
│           │       dm_tripdata_avg.sql
│           │       dm_tripdata_by_gender.sql
│           │       dm_tripdata_count.sql
│           │
│           └───stg
│                   stg_citibyke_trips.sql
</pre>

1. config
    - airflow.cfg - настройки airflow
    - dag_config.py - содержит в себе параметры подключения к S3, БД приемнику, директории выгрузки отчетов и формирования метаданных.
2. dags
    - CitiBikeTripReport - Реализация оркестрации задач загрузки и формирования файлов отчетов
    - metadata - директория для сохранения файла "processing_files.txt" со списком не обработанных файлов источников + дата создания
    - programs - реализация функций чтения источника, ожидания новых файлов, загрузки данных в БД и выгрузки в s3
3. database
    - scripts/create_database.sql - скрипт создания БД
    - scripts/db/stg - скрипты создания промежуточных таблиц
    - scripts/db/dm - скрипты создания витрин данных
4. docker-compose-LocalExecutor.yml
    - Файл разворачивания окружения airflow, clickhouse в docker

# Логика загрузки данных и функциональные компоненты

## 1. Проверка наличия файлов источников во входном s3 bucket
<p>
Вызывается python функция check_new_files() созданная в файле /dags/programs/yaS3sensor.py.
Функция подключается к бакету-источнику. Определяет дату последнего загруженного файла(для первого запуска это 1900-01-01 00:00:00.000000+00:00).
Далее список файлов в бакете фильтруется по дате последнего загруженного файла. Если итоговый список пуст каждые 10 сек. проверяем бакет-источник на наличие новых файлов.
Из полученного списка файлов формируем файл метаданных processing_files.txt, который содержит имя файла и дату создания и сохраняем в ./dags/metadata.
</p>
<p>
Имя таска: check_new_files_task
</p>

## 2. Загрузка полученных данных в слой промежуточного хранения и слой витрин данных
<p>
Загрука в слой промежуточного хранения и слой витрин данных реализована функциями python в файле /dags/programs/Loader.py.
</p>
<p>
Функция stg_load_tripdata() скачивает архив с данными из бакета-источника s3 и разархивирует файлы с расширением csv.
С помощью Pandas DataFrame считывается файл с данными csv и сохраняет их в промежуточной таблице с учетом различий в модели данных(старая и новая)
</p>
<p>
Имя таска: load_stg_tripdata
</p>
<p>
Функции dm_load_tripdata_cout(), dm_load_tripdata_avg(), dm_load_tripdata_by_gender() осуществляют загрузку в витрину с предварительным удалением уже имеющегося отчета на дату.
Выполняются параллельно.
</p>
<p>
Имя таска: dm_load_tripdata_cout, dm_load_tripdata_avg, dm_load_tripdata_by_gender
</p>

## 3. Выгрузка отчетов в s3 бакет-приемник
<p>
Функции выгрузки реализованы в файле /dags/programs/yaS3Saver.py.
В функциях export_*() формируется запрос к БД на выгрузку данных и список выгружаемых полей.
Далее запускается функция чтения данных, выгрузки в файл и его загрузки в бакет-приемник.
Функция start_export(sql_str, full_file_path, file_name, columns: list)
Выполняются параллельно.
</p>
<p>
Имя таска: export_tripdata_count, export_tripdata_avg, export_dm_tripdata_by_gender
</p>

## 6. Обновление параметра "дата последнего обработанного файла"
<p>
Функция set_last_load_date() реализованная в файле ./dags/CitiBikeTripReport отбирает максимальную дату создания файла из файла метаданных processing_file.txt
и сохраняет ее в Airflow Variables
</p>
<p>
Имя таска: set_last_load_date
</p>

## 5. Реализация непрерывного выполнения(цикл)
<p>
При успешном выполнении предыдущих шагов граф вызывает сам себя.
</p>
<p>
Имя таска: rerun_dag
</p>