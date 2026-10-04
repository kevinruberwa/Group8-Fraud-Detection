"""
Kafka Producer -- streams transaction data FROM HDFS into Kafka to
simulate real-time incoming transactions.

Per the project guidelines, the producer reads the dataset directly
from Hadoop HDFS (/fraud_project/data/transactions.csv), not from a
local copy. It uses WebHDFS (HDFS's built-in REST API on port 9870),
so no Hadoop client needs to be installed on Windows.

How the HDFS read works:
  1. We ask the NameNode (localhost:9870) to OPEN the file.
  2. The NameNode replies with a redirect to the DataNode that holds
     the blocks. That redirect uses the DataNode's internal Docker
     hostname, which Windows can't resolve -- so we swap it for
     localhost:9864 (the DataNode port published in docker-compose).
  3. We stream the file line by line, so the 1.2GB file is never
     loaded into memory.

IMPORTANT: run this on your Windows machine (NOT inside a container),
so it connects to Kafka via the "host" listener: localhost:29092.

Requirements:
    pip install kafka-python requests

Usage:
    python kafka_producer.py --max-rows 5000 --delay 0.01
    python kafka_producer.py --start-row 5000 --max-rows 3000   (continue with fresh transactions)
    python kafka_producer.py --source local --csv ../data/transactions.csv   (fallback / debugging only)
"""

import argparse
import csv
import itertools
import json
import time
from urllib.parse import urlparse

import requests
from kafka import KafkaProducer


def open_hdfs_lines(namenode_url, datanode_host, hdfs_path, hdfs_user):
    """Return an iterator of text lines streamed from a file in HDFS via WebHDFS."""
    open_url = f"{namenode_url}/webhdfs/v1{hdfs_path}?op=OPEN&user.name={hdfs_user}"

    # Step 1: NameNode answers with a 307 redirect to the DataNode
    r = requests.get(open_url, allow_redirects=False, timeout=30)
    if r.status_code != 307 or "Location" not in r.headers:
        raise RuntimeError(
            f"Unexpected NameNode response ({r.status_code}): {r.text[:300]}\n"
            f"Check the file exists: docker exec -it namenode hdfs dfs -ls {hdfs_path}"
        )

    # Step 2: replace the internal Docker hostname with one Windows can reach
    redirect = urlparse(r.headers["Location"])
    datanode_url = redirect._replace(netloc=datanode_host).geturl()

    # Step 3: stream the file contents
    resp = requests.get(datanode_url, stream=True, timeout=60)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return (line for line in resp.iter_lines(decode_unicode=True) if line)


def open_local_lines(csv_path):
    """Fallback: read the local CSV copy (for debugging only)."""
    f = open(csv_path, newline="", encoding="utf-8")
    return (line.rstrip("\r\n") for line in f if line.strip())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["hdfs", "local"], default="hdfs",
                        help="Where to read the data from (default: hdfs, as the guidelines require)")
    parser.add_argument("--namenode-url", type=str, default="http://localhost:9870",
                        help="WebHDFS address of the NameNode")
    parser.add_argument("--datanode-host", type=str, default="localhost:9864",
                        help="DataNode address reachable from Windows")
    parser.add_argument("--hdfs-path", type=str, default="/fraud_project/data/transactions.csv")
    parser.add_argument("--hdfs-user", type=str, default="root")
    parser.add_argument("--csv", type=str, default="../data/transactions.csv",
                        help="Local CSV path (only used with --source local)")
    parser.add_argument("--bootstrap-server", type=str, default="localhost:29092",
                        help="Kafka bootstrap server reachable from the host machine")
    parser.add_argument("--topic", type=str, default="transactions_topic")
    parser.add_argument("--delay", type=float, default=0.01,
                        help="Seconds to wait between messages (simulates real-time arrival)")
    parser.add_argument("--start-row", type=int, default=0,
                        help="Skip this many data rows first, so each demo run streams NEW "
                             "transactions instead of repeating the start of the file")
    parser.add_argument("--max-rows", type=int, default=50000,
                        help="Stop after this many rows (full dataset has ~13.5M rows -- "
                             "sending all of them would take a very long time at any "
                             "realistic delay, so we stream a representative slice for "
                             "the demo/testing. Set to -1 to send the entire file.")
    args = parser.parse_args()

    if args.source == "hdfs":
        lines = open_hdfs_lines(args.namenode_url, args.datanode_host, args.hdfs_path, args.hdfs_user)
        print(f"Reading from HDFS: {args.namenode_url}{args.hdfs_path}")
    else:
        lines = open_local_lines(args.csv)
        print(f"Reading from LOCAL file: {args.csv}")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Connected to Kafka at {args.bootstrap_server}, streaming to topic '{args.topic}'")

    sent = 0
    start_time = time.time()

    reader = csv.DictReader(lines)
    if args.start_row > 0:
        print(f"Skipping the first {args.start_row:,} rows (starting at transaction {args.start_row:,})...")
        reader = itertools.islice(reader, args.start_row, None)

    for row in reader:
        # Kafka messages are JSON -- convert types that need it
        row["amount"] = float(row["amount"])
        row["card_present"] = row["card_present"] == "True"
        row["customer_age"] = int(row["customer_age"])
        row["customer_tenure_days"] = int(row["customer_tenure_days"])
        row["avg_txn_amount_last_30d"] = float(row["avg_txn_amount_last_30d"])
        row["transactions_last_1h"] = int(row["transactions_last_1h"])
        row["distance_from_home_km"] = float(row["distance_from_home_km"])
        row["is_fraud"] = int(row["is_fraud"])

        producer.send(args.topic, value=row)
        sent += 1

        if sent % 1000 == 0:
            elapsed = time.time() - start_time
            print(f"Sent {sent:,} messages ({elapsed:.1f}s elapsed)")

        if args.max_rows != -1 and sent >= args.max_rows:
            break

        time.sleep(args.delay)

    producer.flush()
    producer.close()
    print(f"\nDone. Sent {sent:,} messages to '{args.topic}' in {time.time() - start_time:.1f}s")


if __name__ == "__main__":
    main()
