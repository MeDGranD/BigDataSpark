1. Запускаем контейнеры:
```
docker-compose up -d
```
2. Загружаем данные в постгрю:
```
docker exec -it bigdataspark-postgres-1 psql -U spark_user -d spark_db -f /import_data/sql/mock_data.sql
```
3. Создаем структуру звезды:
```
docker exec -it bigdataspark-postgres-1 psql -U spark_user -d spark_db -f /import_data/sql/ddl.sql 
```
4. Запускаем spark скрипт для заполнения звезды:
```
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark/jars/postgresql-42.6.0.jar /opt/spark-apps/ETL.py
```
5. Запускаем spark скрипт анализа фактов для создания отчетов в clickhouse:
```
docker exec -it spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark/jars/postgresql-42.6.0.jar --driver-class-path /opt/spark/
jars/postgresql-42.6.0.jar:/opt/spark/jars/clickhouse-jdbc-0.4.6.jar /opt/spark-apps/clickhouse.py
```
6. Проверим существование созданных отчетов:
```
docker exec -it bigdataspark-clickhouse-1 clickhouse-client --query "SHOW TABLES;"
```
