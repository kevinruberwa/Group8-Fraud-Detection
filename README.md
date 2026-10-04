# Catching Fraud in Real Time

**Big Data Essentials: Course Project, Group 8**
MSc Big Data Analytics, Adventist University of Central Africa (AUCA). Lecturer: Dr. Kundan Kumar.

Carlos HARERIMANA (101200)
Kevin RUBERWA (101190)
Ernest NDACYAYISENGA (101198)
Felicien GAKUBA (101285)
Florent Isheja MUNYAKAZI (101485)

An end-to-end real-time card fraud detection pipeline:

```
HDFS  ->  Kafka  ->  Spark Structured Streaming + MLlib  ->  MySQL  ->  Django dashboard
```

13.5 million transactions (1.25 GB) are stored in HDFS, streamed through Kafka, enriched and scored
by a Random Forest model (AUC-ROC 0.904, 82% of frauds caught on the test set), written to MySQL and
shown on a live dashboard that refreshes every 5 seconds. The full explanation, results and findings
are in the project report.

\## Demo Video

\[▶ Watch the live dashboard demo](video/Group8\_Dashboard\_Live\_Demo.mp4)



\## Project Report

\[📄 Full project report (PDF)](report/Group8\_Fraud\_Detection\_Report.pdf)

## Project structure

|Path|What it is|
|-|-|
|`docker-compose.yml`|The 7 Big Data services: HDFS NameNode + DataNode, ZooKeeper, Kafka, Spark master + worker, MySQL|
|`hadoop.env`|HDFS configuration|
|`spark/Dockerfile`|Custom Spark 3.5.3 image with numpy and pandas (needed by MLlib)|
|`scripts/generate\_fraud\_dataset.py`|Generates the 1.25 GB synthetic transaction dataset (fixed seed, reproducible)|
|`scripts/create\_tables.sql`|MySQL table for the predictions|
|`scripts/download\_jars.bat`|Downloads the 5 JAR files Spark needs|
|`producer/kafka\_producer.py`|Kafka producer: reads the dataset **from HDFS** (WebHDFS) and streams it to Kafka|
|`consumer/spark\_consumer\_stage1.py`|Stage 1: reads and parses the Kafka stream (verification)|
|`consumer/spark\_consumer\_stage2.py`|Stage 2: adds cleaning and feature engineering (verification)|
|`consumer/spark\_consumer\_stage3.py`|Stage 3: scores each transaction with the model and writes to MySQL (production)|
|`models/train\_fraud\_model.py`|MLlib training on HDFS data: Logistic Regression vs Random Forest, saves the best to HDFS|
|`models/training\_metrics.json`|Results of our training run (shown on the dashboard)|
|`django\_dashboard/`|Django web dashboard (app `monitor`)|
|`data/`, `jars/`|Empty here; filled by the generator and the download script (see below)|

## Requirements

* Windows 10/11 with WSL2 and Docker Desktop (WSL2 engine)
* Python 3.10+
* 8 GB RAM (give Docker 3 GB) and about 15 GB of free disk space

## How to run

All commands are for the Windows command prompt, from the project folder.

**1. Give Docker 3 GB of memory.** Put this in `%USERPROFILE%\\.wslconfig`, run `wsl --shutdown`, restart Docker Desktop:

```
\[wsl2]
memory=3GB
processors=2
swap=4GB
```

**2. Start the cluster**

```
docker-compose up -d --build
```

**3. Generate the dataset and load it into HDFS**

```
pip install -r requirements.txt
cd scripts
python generate\_fraud\_dataset.py --target-gb 1.2 --out ../data/transactions.csv
cd ..
docker cp data/transactions.csv namenode:/tmp/transactions.csv
docker exec -it namenode hdfs dfs -mkdir -p /fraud\_project/data
docker exec -it namenode hdfs dfs -put /tmp/transactions.csv /fraud\_project/data/
docker exec -it namenode rm /tmp/transactions.csv
```

**4. Create the Kafka topic and the MySQL table, download the JARs**

```
docker exec -it kafka kafka-topics --create --topic transactions\_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -i mysql mysql -ubde\_user -pbde\_pw fraud\_db < scripts\\create\_tables.sql
scripts\\download\_jars.bat
```

**5. Train the model** (pausing services training does not need)

```
docker stop kafka zookeeper mysql
docker exec -it spark-master /opt/spark/bin/spark-submit --driver-memory 512m --executor-memory 768m /tmp/models/train\_fraud\_model.py --sample-fraction 0.05
docker start zookeeper kafka mysql
```

**6. Start real-time scoring** (terminal 1, keep open; wait for "Stage 3 consumer started")

```
docker exec -it spark-master /opt/spark/bin/spark-submit --driver-memory 512m --executor-memory 768m --jars /tmp/jars/spark-sql-kafka-0-10\_2.12-3.5.3.jar,/tmp/jars/spark-token-provider-kafka-0-10\_2.12-3.5.3.jar,/tmp/jars/kafka-clients-3.4.1.jar,/tmp/jars/commons-pool2-2.11.1.jar,/tmp/jars/mysql-connector-j-8.0.33.jar /tmp/consumer/spark\_consumer\_stage3.py
```

**7. Start the dashboard** (terminal 2, keep open), then open http://127.0.0.1:8000

```
cd django\_dashboard
pip install -r requirements.txt
python manage.py runserver
```

**8. Stream transactions** (terminal 3) and watch the dashboard update live

```
cd producer
python kafka\_producer.py --max-rows 10000 --delay 0.01
```

For another run with fresh transactions, add `--start-row 10000`.

## Versions

Hadoop 3.2.1, Kafka (Confluent Platform 7.5.0), Spark / PySpark / MLlib 3.5.3 (Scala 2.12, Java 11),
MySQL 8.0, Django 4.2, Python 3.8 (Spark containers) and 3.12 (Windows host).
The full version and dependency lists are in chapters 4 and 5 of the report.

