from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime

app = Flask(__name__)

# Connect to Mongo
client = MongoClient("mongodb://localhost:27017/")
db = client["water_quality_data"]
collection = db["asv_1"]


def _parse_iso_timestamp(ts_str):
    """Parse an ISO timestamp string for validation.

    Accepts timestamps with a trailing Z by converting it to +00:00 for
    datetime.fromisoformat. Raises ValueError on invalid format.
    so ISO lexical ordering still works) and the parsed datetime.
    """
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
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "min": {"$min": f"${field}"},
                    "max": {"$max": f"${field}"},
                    "avg": {"$avg": f"${field}"},
                    "stddev": {"$stdDevPop": f"${field}"}
                }
            }
        ]
        result = list(collection.aggregate(pipeline))
        if result:
            stats[field] = {
                "min": result[0]["min"],
                "max": result[0]["max"],
                "avg": result[0]["avg"],
                "stddev": result[0]["stddev"]
            }
        else:
            stats[field] = {
                "min": None,
                "max": None,
                "avg": None,
                "stddev": None
            }

    return jsonify(stats)

# ---- Get Outliers ----
# @app.route("/api/outliers", methods=["GET"])
# def get_outliers():
    

if __name__ == "__main__":
    app.run(debug=True)