"""
Paperless-ngx Migration Web App
=================================
Flask-Anwendung mit Web-UI für die Migration zwischen Paperless-Instanzen.
"""

import json
import queue
import threading
import uuid
from flask import Flask, render_template, request, jsonify, Response

from migrator import PaperlessAPI, MigrationRunner

app = Flask(__name__)

# Aktive Migration-Sessions
sessions = {}
migration_lock = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/test-connection", methods=["POST"])
def test_connection():
    """Testet die Verbindung zu einer Paperless-Instanz."""
    data = request.get_json()
    url = data.get("url", "").strip().rstrip("/")
    token = data.get("token", "").strip()

    if not url or not token:
        return jsonify({"ok": False, "message": "URL und Token sind erforderlich."})

    api = PaperlessAPI(url, token)
    result = api.test_connection()

    # Wenn erfolgreich, auch Statistiken holen
    if result["ok"]:
        try:
            stats = {}
            for resource in ["tags", "correspondents", "document_types",
                             "storage_paths", "documents"]:
                items = api.get_all(resource)
                stats[resource] = len(items)
            result["stats"] = stats
        except Exception:
            pass

    return jsonify(result)


@app.route("/api/migrate/start", methods=["POST"])
def start_migration():
    """Startet die Migration."""
    # Prüfen ob bereits eine Migration läuft
    with migration_lock:
        for sid, session in sessions.items():
            if session["thread"].is_alive():
                return jsonify({
                    "ok": False,
                    "message": "Es läuft bereits eine Migration."
                }), 409

    data = request.get_json()

    source_url = data.get("source_url", "").strip().rstrip("/")
    source_token = data.get("source_token", "").strip()
    target_url = data.get("target_url", "").strip().rstrip("/")
    target_token = data.get("target_token", "").strip()
    migrate_options = data.get("migrate", {})

    if not all([source_url, source_token, target_url, target_token]):
        return jsonify({"ok": False, "message": "Alle Felder sind erforderlich."}), 400

    if not any(migrate_options.values()):
        return jsonify({"ok": False, "message": "Mindestens eine Option muss ausgewählt sein."}), 400

    session_id = str(uuid.uuid4())
    progress_queue = queue.Queue()
    cancel_event = threading.Event()

    runner = MigrationRunner(
        source_url=source_url,
        source_token=source_token,
        target_url=target_url,
        target_token=target_token,
        migrate_options=migrate_options,
        progress_queue=progress_queue,
        cancel_event=cancel_event,
    )

    thread = threading.Thread(target=runner.run, daemon=True)

    sessions[session_id] = {
        "thread": thread,
        "queue": progress_queue,
        "cancel": cancel_event,
    }

    thread.start()

    return jsonify({"ok": True, "session_id": session_id})


@app.route("/api/migrate/progress/<session_id>")
def migration_progress(session_id):
    """SSE-Endpoint für Live-Fortschritt."""
    session = sessions.get(session_id)
    if not session:
        return jsonify({"error": "Session nicht gefunden"}), 404

    def event_stream():
        q = session["queue"]
        while True:
            try:
                event = q.get(timeout=30)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

                # Bei 'complete' oder 'cancelled' Stream beenden
                if event.get("type") in ("complete", "cancelled"):
                    break
            except queue.Empty:
                # Keepalive
                yield ": keepalive\n\n"

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/migrate/cancel/<session_id>", methods=["POST"])
def cancel_migration(session_id):
    """Bricht eine laufende Migration ab."""
    session = sessions.get(session_id)
    if not session:
        return jsonify({"error": "Session nicht gefunden"}), 404

    session["cancel"].set()
    return jsonify({"ok": True, "message": "Abbruch angefordert"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
