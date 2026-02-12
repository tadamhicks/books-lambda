import random
import functools
import time
import json
import os
import logging

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
import aws_lambda_wsgi

# ---------------------------------------------------------------------------
# OpenTelemetry setup – OTLP log export with trace correlation + metrics
# ---------------------------------------------------------------------------
logger_provider = None
meter = None

WORKLOAD = "books-flask"

try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk.resources import Resource

    resource = Resource.create({
        "service.name": os.environ.get("OTEL_SERVICE_NAME", WORKLOAD),
        "service.version": os.environ.get("OTEL_SERVICE_VERSION", "2.0.0"),
    })

    # --- Logs ---
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(
            OTLPLogExporter(endpoint="http://localhost:4318/v1/logs")
        )
    )
    otel_handler = LoggingHandler(logger_provider=logger_provider)

    # --- Metrics ---
    meter = metrics.get_meter(WORKLOAD, "2.0.0")

    OPENTELEMETRY_AVAILABLE = True

except ImportError as e:
    print(f"OpenTelemetry not available: {e}")
    otel_handler = None
    OPENTELEMETRY_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)

if otel_handler:
    logging.getLogger().addHandler(otel_handler)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Common label helpers
# ---------------------------------------------------------------------------
# Base labels present on every custom metric for consistent filtering
BASE_LABELS = {"workload": WORKLOAD}


def _labels(**kwargs):
    """Merge base labels with per-call labels."""
    return {**BASE_LABELS, **kwargs}


# ---------------------------------------------------------------------------
# Custom metrics instruments
#
# Labels rationale (informed by OTel semantic conventions and existing
# groundcover metrics like http_server_active_requests, http_server_duration):
#
#   workload        — hardcoded "books-flask"; allows filtering alongside
#                     k8s workloads in groundcover dashboards
#   db.system       — OTel semantic convention for database type
#   db.operation    — OTel semantic convention: select/insert/update/delete
#   http.method     — matches http_request_method on auto-instrumented metrics
#   http.route      — matches http_route on auto-instrumented metrics;
#                     uses Flask route patterns (e.g. /books/<id>)
#   http.status_code— matches http_response_status_code convention
#   error.type      — classification: validation, database_connection,
#                     database_query, integrity_violation, health_check
# ---------------------------------------------------------------------------
if meter:
    db_connection_duration = meter.create_histogram(
        name="db.client.connection.duration",
        description="Time to establish a database connection",
        unit="ms",
    )

    db_query_duration = meter.create_histogram(
        name="db.client.query.duration",
        description="Database query execution time",
        unit="ms",
    )

    app_error_counter = meter.create_counter(
        name="app.errors",
        description="Application errors by type, route, and status code",
        unit="1",
    )

    latency_injection_counter = meter.create_counter(
        name="app.latency_injection.count",
        description="Number of times artificial latency was injected",
        unit="1",
    )
    latency_injection_duration = meter.create_histogram(
        name="app.latency_injection.duration",
        description="Duration of injected artificial latency",
        unit="ms",
    )

    books_result_count = meter.create_histogram(
        name="app.books.result_count",
        description="Number of books returned per GET /books request",
        unit="1",
    )

    book_operations_counter = meter.create_counter(
        name="app.books.operations",
        description="Book CRUD operations by type and route",
        unit="1",
    )
else:
    db_connection_duration = None
    db_query_duration = None
    app_error_counter = None
    latency_injection_counter = None
    latency_injection_duration = None
    books_result_count = None
    book_operations_counter = None


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)

# ---------------------------------------------------------------------------
# Database helpers (instrumented with metrics)
# ---------------------------------------------------------------------------
db_config = None


def get_db_config():
    """Get database configuration from environment variables."""
    global db_config
    if db_config is None:
        db_config = {
            "host": os.environ["DB_HOST"],
            "port": int(os.environ["DB_PORT"]),
            "database": os.environ["DB_NAME"],
            "user": os.environ["DB_USER"],
            "password": get_db_password(),
        }
    return db_config


def get_db_password():
    """Get database password from AWS Secrets Manager."""
    try:
        secrets_client = boto3.client("secretsmanager")
        secret_response = secrets_client.get_secret_value(
            SecretId=os.environ["DB_PASSWORD_SECRET"]
        )
        secret_data = json.loads(secret_response["SecretString"])
        return secret_data["password"]
    except Exception as e:
        logger.error(f"Error getting database password: {e}", exc_info=True)
        raise


def get_db_connection():
    """Get database connection, recording connection time as a metric."""
    start = time.monotonic()
    try:
        config = get_db_config()
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            cursor_factory=RealDictCursor,
        )
        duration_ms = (time.monotonic() - start) * 1000
        if db_connection_duration:
            db_connection_duration.record(
                duration_ms, _labels(**{"db.system": "postgresql"})
            )
        return conn
    except Exception as e:
        duration_ms = (time.monotonic() - start) * 1000
        if db_connection_duration:
            db_connection_duration.record(
                duration_ms, _labels(**{"db.system": "postgresql", "error": True})
            )
        if app_error_counter:
            app_error_counter.add(
                1,
                _labels(
                    **{
                        "error.type": "database_connection",
                        "http.route": getattr(request, "path", "unknown"),
                    }
                ),
            )
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        raise


def execute_query(cursor, query, params=None, operation="select"):
    """Execute a query and record its duration as a metric."""
    start = time.monotonic()
    try:
        cursor.execute(query, params)
        duration_ms = (time.monotonic() - start) * 1000
        if db_query_duration:
            db_query_duration.record(
                duration_ms,
                _labels(**{"db.system": "postgresql", "db.operation": operation}),
            )
    except Exception:
        duration_ms = (time.monotonic() - start) * 1000
        if db_query_duration:
            db_query_duration.record(
                duration_ms,
                _labels(
                    **{
                        "db.system": "postgresql",
                        "db.operation": operation,
                        "error": True,
                    }
                ),
            )
        raise


# ---------------------------------------------------------------------------
# Latency injection (chaos testing) — instrumented
# ---------------------------------------------------------------------------
LATENCY_PROBABILITY = float(os.environ.get("LATENCY_PROBABILITY", "0.05"))
LATENCY_MAX_MS = int(os.environ.get("LATENCY_MAX_MS", "1300"))


def maybe_delay(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if random.random() < LATENCY_PROBABILITY:
            delay_ms = random.randint(100, LATENCY_MAX_MS)
            logger.info(f"[Latency Injector] Delaying request by {delay_ms} ms")
            if latency_injection_counter:
                latency_injection_counter.add(
                    1,
                    _labels(
                        **{
                            "http.route": request.path,
                            "http.method": request.method,
                        }
                    ),
                )
            if latency_injection_duration:
                latency_injection_duration.record(
                    delay_ms,
                    _labels(
                        **{
                            "http.route": request.path,
                            "http.method": request.method,
                        }
                    ),
                )
            time.sleep(delay_ms / 1000.0)
        return func(*args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# Helper: serialise a book row to JSON-safe dict
# ---------------------------------------------------------------------------
def _serialise_book(book):
    d = dict(book)
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat()
    if d.get("updated_at"):
        d["updated_at"] = d["updated_at"].isoformat()
    return d


# ---------------------------------------------------------------------------
# Error recording helper
# ---------------------------------------------------------------------------
def _record_error(error_type, http_route, http_status_code, http_method="GET"):
    """Record an application error with consistent labels."""
    if app_error_counter:
        app_error_counter.add(
            1,
            _labels(
                **{
                    "error.type": error_type,
                    "http.route": http_route,
                    "http.status_code": str(http_status_code),
                    "http.method": http_method,
                }
            ),
        )


# ---------------------------------------------------------------------------
# Operation recording helper
# ---------------------------------------------------------------------------
def _record_operation(operation, http_route, http_method="GET"):
    """Record a book CRUD operation with consistent labels."""
    if book_operations_counter:
        book_operations_counter.add(
            1,
            _labels(
                **{
                    "operation": operation,
                    "http.route": http_route,
                    "http.method": http_method,
                }
            ),
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/healthz", methods=["GET"])
@maybe_delay
def health_check():
    """Health check endpoint."""
    logger.info("Health check started")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(cursor, "SELECT 1", operation="health_check")
        cursor.fetchone()
        cursor.close()
        conn.close()
        logger.info("Health check completed successfully")

        return jsonify({
            "status": "healthy",
            "service": WORKLOAD,
            "version": "2.0.0",
            "database": "connected",
        })
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        _record_error("health_check", "/healthz", 500)
        return jsonify({
            "status": "unhealthy",
            "service": WORKLOAD,
            "version": "2.0.0",
            "database": "disconnected",
            "error": str(e),
        }), 500


@app.route("/test", methods=["GET"])
@maybe_delay
def test_endpoint():
    """Simple test endpoint without database access."""
    return jsonify({
        "status": "ok",
        "message": "Test endpoint working",
        "service": WORKLOAD,
        "version": "2.0.0",
    })


@app.route("/books", methods=["GET"])
@maybe_delay
def get_books():
    """Get all books."""
    logger.info("GET /books - Retrieving all books")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(
            cursor,
            """SELECT id, title, author, isbn, description, price, created_at, updated_at
               FROM books ORDER BY id""",
            operation="select",
        )
        books = cursor.fetchall()
        cursor.close()
        conn.close()

        result = [_serialise_book(b) for b in books]

        if books_result_count:
            books_result_count.record(
                len(result),
                _labels(**{"http.route": "/books", "http.method": "GET"}),
            )
        _record_operation("list", "/books", "GET")

        return jsonify(result)
    except Exception as e:
        logger.error(f"Error getting books: {e}")
        _record_error("database_query", "/books", 500)
        return jsonify({"error": "Failed to get books", "message": str(e)}), 500


@app.route("/books/<int:book_id>", methods=["GET"])
@maybe_delay
def get_book(book_id):
    """Get a specific book by ID."""
    logger.info(f"GET /books/{book_id} - Retrieving book")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(
            cursor,
            """SELECT id, title, author, isbn, description, price, created_at, updated_at
               FROM books WHERE id = %s""",
            (book_id,),
            operation="select",
        )
        book = cursor.fetchone()
        cursor.close()
        conn.close()

        _record_operation("get", "/books/<id>", "GET")

        if book:
            return jsonify(_serialise_book(book))
        return jsonify({"error": "Book not found"}), 404
    except Exception as e:
        logger.error(f"Error getting book {book_id}: {e}")
        _record_error("database_query", "/books/<id>", 500, "GET")
        return jsonify({"error": "Failed to get book", "message": str(e)}), 500


@app.route("/books", methods=["POST"])
def create_book():
    """Create a new book."""
    logger.info("POST /books - Creating new book")
    try:
        data = request.get_json()

        required_fields = ["title", "author", "isbn", "description", "price"]
        for field in required_fields:
            if field not in data:
                _record_error("validation", "/books", 400, "POST")
                return jsonify({"error": "Missing required field", "field": field}), 400

        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(
            cursor,
            """INSERT INTO books (title, author, isbn, description, price)
               VALUES (%s, %s, %s, %s, %s)
               RETURNING id, title, author, isbn, description, price, created_at, updated_at""",
            (
                data["title"],
                data["author"],
                data["isbn"],
                data["description"],
                float(data["price"]),
            ),
            operation="insert",
        )
        book = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        _record_operation("create", "/books", "POST")

        return jsonify(_serialise_book(book)), 201

    except psycopg2.IntegrityError as e:
        logger.warning(f"Database integrity error: {e}")
        _record_error("integrity_violation", "/books", 409, "POST")
        return jsonify({
            "error": "Book with this ISBN already exists",
            "message": str(e),
        }), 409
    except Exception as e:
        logger.error(f"Error creating book: {e}")
        _record_error("database_query", "/books", 500, "POST")
        return jsonify({"error": "Failed to create book", "message": str(e)}), 500


@app.route("/books/<int:book_id>", methods=["PUT"])
def update_book(book_id):
    """Update a book."""
    logger.info(f"PUT /books/{book_id} - Updating book")
    try:
        data = request.get_json()

        update_fields = []
        values = []
        for field in ["title", "author", "isbn", "description", "price"]:
            if field in data:
                update_fields.append(f"{field} = %s")
                values.append(float(data[field]) if field == "price" else data[field])

        if not update_fields:
            _record_error("validation", "/books/<id>", 400, "PUT")
            return jsonify({"error": "No fields to update"}), 400

        values.append(book_id)

        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(
            cursor,
            f"""UPDATE books
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                RETURNING id, title, author, isbn, description, price, created_at, updated_at""",
            values,
            operation="update",
        )
        book = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        _record_operation("update", "/books/<id>", "PUT")

        if book:
            return jsonify(_serialise_book(book))
        return jsonify({"error": "Book not found"}), 404

    except psycopg2.IntegrityError as e:
        logger.warning(f"Database integrity error: {e}")
        _record_error("integrity_violation", "/books/<id>", 409, "PUT")
        return jsonify({
            "error": "Book with this ISBN already exists",
            "message": str(e),
        }), 409
    except Exception as e:
        logger.error(f"Error updating book {book_id}: {e}")
        _record_error("database_query", "/books/<id>", 500, "PUT")
        return jsonify({"error": "Failed to update book", "message": str(e)}), 500


@app.route("/books/<int:book_id>", methods=["DELETE"])
def delete_book(book_id):
    """Delete a book."""
    logger.info(f"DELETE /books/{book_id} - Deleting book")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        execute_query(
            cursor,
            "DELETE FROM books WHERE id = %s RETURNING id",
            (book_id,),
            operation="delete",
        )
        deleted_book = cursor.fetchone()
        conn.commit()
        cursor.close()
        conn.close()

        _record_operation("delete", "/books/<id>", "DELETE")

        if deleted_book:
            return jsonify({"message": "Book deleted successfully", "id": book_id})
        return jsonify({"error": "Book not found"}), 404
    except Exception as e:
        logger.error(f"Error deleting book {book_id}: {e}")
        _record_error("database_query", "/books/<id>", 500, "DELETE")
        return jsonify({"error": "Failed to delete book", "message": str(e)}), 500


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    """AWS Lambda handler for Flask app."""
    try:
        return aws_lambda_wsgi.response(app, event, context)
    except Exception as e:
        logger.error(f"Lambda handler error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "Internal server error",
                "message": str(e),
            }),
        }
    finally:
        # Flush OTLP logs before Lambda freezes the execution environment
        if logger_provider:
            logger_provider.force_flush(timeout_millis=5000)


# ---------------------------------------------------------------------------
# Local development
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)