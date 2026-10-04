The dataset is not included (1.25 GB). Recreate it exactly with:

    cd scripts
    python generate_fraud_dataset.py --target-gb 1.2 --out ../data/transactions.csv

The generator uses a fixed random seed (42), so it produces the identical 13.5 million rows used in the report.
