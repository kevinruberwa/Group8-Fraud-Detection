from django.db import models


class FraudPrediction(models.Model):
    """
    One transaction scored in real time by Spark (Stage 3 consumer).

    managed = False: the table is created by scripts/create_tables.sql and
    filled by Spark -- Django only READS it, it never creates or alters it.
    """
    id = models.BigAutoField(primary_key=True)
    transaction_id = models.BigIntegerField()
    customer_id = models.BigIntegerField(null=True)
    event_time = models.DateTimeField(null=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    merchant_category = models.CharField(max_length=30, null=True)
    device_type = models.CharField(max_length=30, null=True)
    txn_city = models.CharField(max_length=50, null=True)
    card_present = models.BooleanField(null=True)
    distance_from_home_km = models.FloatField(null=True)
    transactions_last_1h = models.IntegerField(null=True)
    amount_to_avg_ratio = models.FloatField(null=True)
    hour_of_day = models.IntegerField(null=True)
    is_high_risk_merchant = models.SmallIntegerField(null=True)
    is_far_from_home = models.SmallIntegerField(null=True)
    is_high_velocity = models.SmallIntegerField(null=True)
    fraud_probability = models.FloatField(null=True)
    prediction = models.SmallIntegerField(null=True)
    risk_level = models.CharField(max_length=10, null=True)
    is_fraud = models.SmallIntegerField(null=True)
    batch_id = models.BigIntegerField(null=True)
    processed_at = models.DateTimeField(null=True)

    class Meta:
        managed = False
        db_table = "fraud_predictions"
