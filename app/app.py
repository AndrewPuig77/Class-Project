from flask import Flask, jsonify, request
from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv
app = Flask(__name__)

# Connect to Mongo
load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI")
MONGO_PASS = os.getenv("MONGO_PASS")
MONGO_USER = os.getenv("MONGO_USER")

if not all([MONGO_URI, MONGO_USER, MONGO_PASS]):
    print("ERROR: Missing MongoDB credentials!")
    exit(1)

url= f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_URI}/?retryWrites=true&w=majority"

client = MongoClient(url)
try:
    # The ismaster command is cheap and does not require auth
    client.admin.command('ping')
    print("MongoDB connection successful!")
except Exception as e:
    print("MongoDB connection failed:", e)
    

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

# In Flask app.py
def clean_nan(obj):
    """Replace NaN with None for JSON serialization"""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(item) for item in obj]
    return obj

# Use it before returning
# items = clean_nan(items)
# return jsonify({"count": total, "items": items})

#----- Health Check -----
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


#----- Get Available Dates -----
@app.route("/api/dates", methods=["GET"])
def get_dates():
    try:
        dates = collection.distinct("Date")
        dates = sorted([d for d in dates if d])
        return jsonify({"dates": dates})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

#----- Get Observations -----
@app.route("/api/observations", methods=["GET"])
def get_observations():
    # Build MongoDB query
    q = {}

    # Date filtering (using your actual field name)
    date = request.args.get("date")
    if date:
        q["Date"] = date

    # Numeric ranges
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

    # Pagination
    try:
        limit = int(request.args.get("limit", 100))
        skip = int(request.args.get("skip", 0))
    except ValueError:
        return jsonify({"error": "limit and skip must be integers"}), 400

    if limit <= 0:
        return jsonify({"error": "limit must be > 0"}), 400
    if skip < 0:
        return jsonify({"error": "skip must be >= 0"}), 400
    
    limit = min(limit, 1000)

    # Query database
    try:
        total = collection.count_documents(q)
        cursor = collection.find(q, {"_id": 0}).skip(skip).limit(limit)
        items = list(cursor)
        
        return jsonify({
            "count": total,
            "returned": len(items),
            "items": items
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500



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