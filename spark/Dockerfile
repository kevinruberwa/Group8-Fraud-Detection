# Custom Spark image for the fraud detection project.
# Official Apache Spark 3.5.3 + the Python libraries MLlib needs, baked in,
# so they survive container resets (no more manual "pip install" inside containers).
FROM apache/spark:3.5.3

USER root
RUN pip3 install --no-cache-dir "numpy<2" "pandas<2.1"
USER spark
