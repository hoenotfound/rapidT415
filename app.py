import os
import subprocess
import threading

from flask import Flask, request, jsonify, abort

app = Flask(__name__)

run_lock = threading.Lock()


def run_bus_monitor():
    if not run_lock.acquire(blocking=False):
        print("[info] Bus monitor already running, skipping duplicate request.")
        return

    try:
        command = [
            "python",
            "bus_monitor.py",
            "--route",
            "765",
            "--telegram-token",
            os.environ["TELEGRAM_TOKEN"],
            "--telegram-chat-id",
            os.environ["TELEGRAM_CHAT_ID"],
        ]

        print("[info] Starting bus monitor...")

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=120,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        print(f"[info] Bus monitor finished with code {result.returncode}")

    except Exception as e:
        print(f"[error] Bus monitor failed: {e}")

    finally:
        run_lock.release()


@app.route("/")
def home():
    return "Bus Route Monitor is running"


@app.route("/check", methods=["GET", "POST"])
def check_bus():

    expected_secret = os.environ.get("CRON_SECRET")
    supplied_secret = request.headers.get("X-Cron-Secret")

    if not expected_secret or supplied_secret != expected_secret:
        abort(401)

    if run_lock.locked():
        return jsonify({
            "status": "already_running"
        }), 202

    thread = threading.Thread(
        target=run_bus_monitor,
        daemon=True
    )
    thread.start()

    return jsonify({
        "status": "started"
    }), 202
