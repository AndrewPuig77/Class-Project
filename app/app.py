from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime
import numpy as np

app = Flask(__name__)

# Connect to Mongo
client = MongoClient("mongodb://localhost:27017/")
db = client["water_quality_data"]
collection = db["asv_1"]


def _parse_iso_timestamp(ts_str):
    if ts_str is None:
        return None, None
    # Support trailing Z
    s = ts_str
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    parsed = datetime.fromisoformat(s)
    return ts_str, parsed

#----- Health Check -----
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


#----- Get Observations -----
@app.route("/api/observations", methods=["GET"])
def get_observations():
    # Build MongoDB query from supported query parameters
    q = {}

    # Timestamp range
    start = request.args.get("start")
    end = request.args.get("end")
    try:
        if start:
            start_str, _ = _parse_iso_timestamp(start)
            q.setdefault("timestamp", {})["$gte"] = start_str
        if end:
            end_str, _ = _parse_iso_timestamp(end)
            q.setdefault("timestamp", {})["$lte"] = end_str
    except ValueError:
        return jsonify({"error": "start/end must be valid ISO timestamps"}), 400

    # Numeric ranges helper
    def _add_range(field_name, min_arg, max_arg):
        min_v = request.args.get(min_arg)
        max_v = request.args.get(max_arg)
        if min_v is None and max_v is None:
            return
        try:
            if min_v is not None:
                min_f = float(min_v)
                q.setdefault(field_name, {})["$gte"] = min_f
            if max_v is not None:
                max_f = float(max_v)
                q.setdefault(field_name, {})["$lte"] = max_f
        except ValueError:
            raise

    try:
        _add_range("temperature", "min_temp", "max_temp")
        _add_range("salinity", "min_sal", "max_sal")
        _add_range("odo", "min_odo", "max_odo")
    except ValueError:
        return jsonify({"error": "min/max numeric parameters must be valid numbers"}), 400

    # Pagination: limit and skip
    limit_arg = request.args.get("limit")
    skip_arg = request.args.get("skip")
    try:
        limit = int(limit_arg) if limit_arg is not None else 100
    except ValueError:
        return jsonify({"error": "limit must be an integer"}), 400
    try:
        skip = int(skip_arg) if skip_arg is not None else 0
    except ValueError:
        return jsonify({"error": "skip must be an integer"}), 400

    if limit <= 0:
        return jsonify({"error": "limit must be > 0"}), 400
    if skip < 0:
        return jsonify({"error": "skip must be >= 0"}), 400
    # enforce cap
    limit = min(limit, 1000)

    # Count total matching documents (before pagination)
    try:
        total = collection.count_documents(q)
    except Exception:
        # In case of a query type mismatch in DB, return 400
        return jsonify({"error": "Invalid query parameters for stored document types"}), 400

    cursor = collection.find(q, {"_id": 0}).skip(skip).limit(limit)
    items = list(cursor)

    return jsonify({"count": total, "items": items})


#----- Get Stats -----
@app.route("/api/stats", methods=["GET"])
def get_stats():
    numeric_fields = ["temperature", "salinity", "odo"]
    stats = {}
    
    for field in numeric_fields:
        # Get all values for this field (excluding None/null)
        cursor = collection.find({field: {"$ne": None}}, {"_id": 0, field: 1})
        values = [doc[field] for doc in cursor if field in doc and doc[field] is not None]
        
        if len(values) == 0:
            stats[field] = {
                "count": 0,
                "mean": None,
                "min": None,
                "max": None,
                "percentiles": {
                    "25": None,
                    "50": None,
                    "75": None
                }
            }
        else:
            values_array = np.array(values)
            stats[field] = {
                "count": len(values),
                "mean": float(np.mean(values_array)),
                "min": float(np.min(values_array)),
                "max": float(np.max(values_array)),
                "percentiles": {
                    "25": float(np.percentile(values_array, 25)),
                    "50": float(np.percentile(values_array, 50)),
                    "75": float(np.percentile(values_array, 75))
                }
            }
    
    return jsonify(stats)


#----- Get Outliers -----
@app.route("/api/outliers", methods=["GET"])
def get_outliers():
    # Required parameters
    field = request.args.get("field")
    method = request.args.get("method", "iqr").lower()
    
    if not field:
        return jsonify({"error": "field parameter is required"}), 400
    
    if field not in ["temperature", "salinity", "odo"]:
        return jsonify({"error": "field must be one of: temperature, salinity, odo"}), 400
    
    if method not in ["iqr", "z-score"]:
        return jsonify({"error": "method must be 'iqr' or 'z-score'"}), 400
    
    # Get k parameter (threshold multiplier)
    k_param = request.args.get("k", "1.5")
    try:
        k = float(k_param)
    except ValueError:
        return jsonify({"error": "k must be a valid number"}), 400
    
    # Fetch all documents with the field
    cursor = collection.find({field: {"$ne": None}}, {"_id": 0})
    docs = list(cursor)
    
    if len(docs) == 0:
        return jsonify({"count": 0, "outliers": []})
    
    # Extract values
    values = np.array([doc[field] for doc in docs if field in doc and doc[field] is not None])
    
    if len(values) == 0:
        return jsonify({"count": 0, "outliers": []})
    
    # Detect outliers based on method
    outlier_indices = []
    
    if method == "iqr":
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        lower_bound = q1 - k * iqr
        upper_bound = q3 + k * iqr
        
        for i, val in enumerate(values):
            if val < lower_bound or val > upper_bound:
                outlier_indices.append(i)
    
    elif method == "z-score":
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            # No outliers if no variation
            return jsonify({"count": 0, "outliers": []})
        
        for i, val in enumerate(values):
            z_score = abs((val - mean) / std)
            if z_score > k:
                outlier_indices.append(i)
    
    # Get the outlier documents
    outliers = [docs[i] for i in outlier_indices]
    
    return jsonify({"count": len(outliers), "outliers": outliers})


if __name__ == "__main__":
    app.run(debug=True)