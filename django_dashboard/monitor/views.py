"""
Dashboard views.

  /              -> the dashboard page (HTML)
  /api/summary/  -> all dashboard numbers as JSON; the page calls this every
                    few seconds so it updates live while Spark streams data in.
"""
import json

from django.conf import settings
from django.db.models import Avg, Count, Max, Q, Sum
from django.http import JsonResponse
from django.shortcuts import render

from .models import FraudPrediction

RISK_ORDER = ["HIGH", "MEDIUM", "LOW"]

# Plain-language names for the model's features (shown on the dashboard)
FEATURE_NAMES = {
    "distance_from_home_km": "Distance from home",
    "is_far_from_home": "Over 500 km from home",
    "amount_to_avg_ratio": "Spend vs 30-day average",
    "avg_txn_amount_last_30d": "30-day average spend",
    "amount": "Transaction amount",
    "transactions_last_1h": "Transactions in last hour",
    "is_high_velocity": "Burst of transactions",
    "is_high_risk_merchant": "High-risk merchant",
    "card_present_int": "Card physically present",
    "hour_of_day": "Hour of day",
    "customer_age": "Customer age",
    "customer_tenure_days": "Customer tenure",
}


def _feature_label(name):
    if name in FEATURE_NAMES:
        return FEATURE_NAMES[name]
    if name.startswith("merchant_category_vec_"):
        return name.replace("merchant_category_vec_", "").replace("_", " ").capitalize() + " merchant"
    if name.startswith("device_type_vec_"):
        return name.replace("device_type_vec_", "").replace("_", " ").capitalize() + " device"
    return name.replace("_", " ").capitalize()


def dashboard(request):
    return render(request, "monitor/index.html")


def _load_training_metrics():
    """Best model + test-set metrics from models/training_metrics.json (if present)."""
    try:
        with open(settings.TRAINING_METRICS_PATH) as f:
            m = json.load(f)
        best = m["best_model"]
        return {
            "best_model": best,
            "auc_roc": m["models"][best]["auc_roc"],
            "recall": m["models"][best]["recall"],
            "precision": m["models"][best]["precision"],
            "trained_on_rows": m.get("sampled_rows"),
            "top_features": [
                {"feature": _feature_label(f["feature"]), "importance": f["importance"]}
                for f in m.get("rf_feature_importance_top10", [])[:5]
            ],
        }
    except (OSError, KeyError, ValueError):
        return None


def summary_api(request):
    qs = FraudPrediction.objects.all()

    totals = qs.aggregate(
        scored=Count("id"),
        flagged=Sum("prediction"),
        actual_frauds=Sum("is_fraud"),
        caught=Count("id", filter=Q(prediction=1, is_fraud=1)),
        false_alarms=Count("id", filter=Q(prediction=1, is_fraud=0)),
        high=Count("id", filter=Q(risk_level="HIGH")),
        last_update=Max("processed_at"),
        flagged_amount=Sum("amount", filter=Q(prediction=1)),
    )
    scored = totals["scored"] or 0
    actual = totals["actual_frauds"] or 0
    flagged = totals["flagged"] or 0

    risk_counts = dict(qs.values_list("risk_level").annotate(n=Count("id")))

    by_merchant = list(
        qs.values("merchant_category")
        .annotate(total=Count("id"), flagged=Sum("prediction"), frauds=Sum("is_fraud"))
        .order_by("-flagged")
    )
    by_hour = {
        r["hour_of_day"]: r
        for r in qs.values("hour_of_day").annotate(
            total=Count("id"), flagged=Sum("prediction"), avg_prob=Avg("fraud_probability"))
    }
    by_city = list(
        qs.values("txn_city")
        .annotate(total=Count("id"), flagged=Sum("prediction"))
        .order_by("-flagged")
    )

    alerts = list(
        qs.filter(risk_level="HIGH")
        .order_by("-id")
        .values("transaction_id", "event_time", "amount", "merchant_category", "txn_city",
                "device_type", "distance_from_home_km", "amount_to_avg_ratio",
                "is_far_from_home", "is_high_velocity", "is_high_risk_merchant",
                "fraud_probability", "is_fraud")[:8]
    )
    for a in alerts:
        a["event_time"] = a["event_time"].strftime("%d %b %H:%M") if a["event_time"] else ""
        a["amount"] = float(a["amount"] or 0)
        reasons = []
        if a["is_far_from_home"]:
            reasons.append(f"{a['distance_from_home_km']:,.0f} km from home")
        if (a["amount_to_avg_ratio"] or 0) >= 3:
            reasons.append(f"{a['amount_to_avg_ratio']:.1f}× usual spend")
        if a["is_high_velocity"]:
            reasons.append("burst of transactions")
        if a["is_high_risk_merchant"]:
            reasons.append(f"{a['merchant_category']} purchase")
        a["reasons"] = reasons

    return JsonResponse({
        "totals": {
            "scored": scored,
            "flagged": flagged,
            "flag_rate": round(flagged / scored, 4) if scored else 0,
            "high": totals["high"],
            "actual_frauds": actual,
            "caught": totals["caught"],
            "false_alarms": totals["false_alarms"],
            "catch_rate": round(totals["caught"] / actual, 4) if actual else None,
            "flagged_amount": float(totals["flagged_amount"] or 0),
            "last_update": totals["last_update"].strftime("%H:%M:%S") if totals["last_update"] else None,
        },
        "risk": [{"level": lvl, "count": risk_counts.get(lvl, 0)} for lvl in RISK_ORDER],
        "by_merchant": by_merchant,
        "by_hour": [
            {"hour": h,
             "total": by_hour.get(h, {}).get("total", 0),
             "flagged": by_hour.get(h, {}).get("flagged", 0) or 0,
             "avg_prob": round(by_hour.get(h, {}).get("avg_prob") or 0, 4)}
            for h in range(24)
        ],
        "by_city": by_city,
        "alerts": alerts,
        "model": _load_training_metrics(),
    })
