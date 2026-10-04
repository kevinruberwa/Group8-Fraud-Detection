# Card fraud monitor (Django dashboard, Step 6)

Reads the live predictions that the Spark Stage 3 consumer writes into
MySQL (`fraud_db.fraud_predictions`) and shows them on a web page that
refreshes every 5 seconds.

## Run it (Windows cmd)

    cd "C:\Users\kevin.risk\OneDrive\Desktop\BIG DATA ESSENTIAL PROJECT\django_dashboard"
    pip install -r requirements.txt
    python manage.py runserver

Then open http://127.0.0.1:8000 in your browser.

## Requirements
- The `mysql` Docker container must be running (published on localhost:3307).
- `models/training_metrics.json` (from the training step) fills the Model panel.
- No `migrate` is needed: Django only reads the table created by
  `scripts/create_tables.sql` (the model is `managed = False`).

## Files
- `fraud_dashboard/settings.py`: MySQL connection (127.0.0.1:3307, fraud_db)
- `monitor/models.py`: the `fraud_predictions` table as a Django model
- `monitor/views.py`: the page, plus `/api/summary/` (all numbers as JSON)
- `monitor/templates/monitor/index.html`: layout, styling and charts (Chart.js)
