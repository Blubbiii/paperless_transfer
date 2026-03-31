#!/usr/bin/env python3
"""
DATEV -> Paperless-ngx Uploader (GUI)
=======================================
Grafische Oberfläche zum Konfigurieren und Starten des Uploads.
Kann eine Windows-Aufgabe einrichten für täglichen automatischen Upload.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import json
import os
import sys
import subprocess
import hashlib
import time
import requests
from pathlib import Path
from datetime import datetime

# Konfig-Datei
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datev_config.json")
UPLOAD_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datev_uploaded.json")

ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".gif", ".bmp", ".webp"}

FOLDER_DOCTYPE_MAP = {
    "rechnungen": "Rechnung", "rechnung": "Rechnung", "invoices": "Rechnung",
    "gutschriften": "Gutschrift", "gutschrift": "Gutschrift", "credit": "Gutschrift",
    "angebote": "Angebot", "angebot": "Angebot",
    "verträge": "Vertrag", "vertrag": "Vertrag",
    "quittungen": "Quittung", "quittung": "Quittung",
    "kontoauszüge": "Kontoauszug", "kontoauszug": "Kontoauszug",
    "mahnungen": "Mahnung", "mahnung": "Mahnung",
    "lieferscheine": "Lieferschein", "lieferschein": "Lieferschein",
}

TASK_NAME = "DATEV_Paperless_Upload"


# ============================================================
# Paperless API
# ============================================================
class PaperlessAPI:
    def __init__(self, url: str, token: str):
        self.url = url.strip().rstrip("/")
        if not self.url.startswith("http"):
            self.url = "http://" + self.url
        self.token = token.strip()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {self.token}",
            "Accept": "application/json",
        })
        self._tag_cache = {}
        self._doctype_cache = {}

    def test_connection(self) -> dict:
        try:
            resp = self.session.get(f"{self.url}/api/tags/?page_size=1", timeout=10)
            resp.raise_for_status()
            data = resp.json()
            tag_count = data.get("count", 0)

            resp2 = self.session.get(f"{self.url}/api/documents/?page_size=1", timeout=10)
            resp2.raise_for_status()
            doc_count = resp2.json().get("count", 0)

            return {
                "ok": True,
                "message": f"Verbunden! {doc_count} Dokumente, {tag_count} Tags vorhanden.",
            }
        except requests.exceptions.ConnectionError:
            return {"ok": False, "message": f"Server nicht erreichbar: {self.url}"}
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                return {"ok": False, "message": "Token ungültig!"}
            return {"ok": False, "message": f"HTTP-Fehler: {e}"}
        except Exception as e:
            return {"ok": False, "message": str(e)}

    def get_or_create_tag(self, name: str) -> int | None:
        if name in self._tag_cache:
            return self._tag_cache[name]
        try:
            resp = self.session.get(
                f"{self.url}/api/tags/?name__iexact={requests.utils.quote(name, safe='')}",
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                self._tag_cache[name] = results[0]["id"]
                return results[0]["id"]

            resp = self.session.post(f"{self.url}/api/tags/", json={"name": name}, timeout=10)
            resp.raise_for_status()
            tag_id = resp.json()["id"]
            self._tag_cache[name] = tag_id
            return tag_id
        except Exception:
            return None

    def get_or_create_doctype(self, name: str) -> int | None:
        if name in self._doctype_cache:
            return self._doctype_cache[name]
        try:
            resp = self.session.get(
                f"{self.url}/api/document_types/?name__iexact={requests.utils.quote(name, safe='')}",
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                self._doctype_cache[name] = results[0]["id"]
                return results[0]["id"]

            resp = self.session.post(f"{self.url}/api/document_types/", json={"name": name}, timeout=10)
            resp.raise_for_status()
            dt_id = resp.json()["id"]
            self._doctype_cache[name] = dt_id
            return dt_id
        except Exception:
            return None

    def upload(self, filepath: str, tags: list = None, document_type: int = None) -> bool:
        try:
            filename = os.path.basename(filepath)
            with open(filepath, "rb") as f:
                files = {"document": (filename, f)}
                data = {"title": Path(filename).stem}

                resp = self.session.post(
                    f"{self.url}/api/documents/post_document/",
                    files=files, data=data, timeout=300,
                )
                resp.raise_for_status()
                task_id = resp.text.strip().strip('"')

                if tags or document_type:
                    doc_id = self._wait_for_task(task_id)
                    if doc_id:
                        update = {}
                        if tags:
                            update["tags"] = tags
                        if document_type:
                            update["document_type"] = document_type
                        self.session.patch(
                            f"{self.url}/api/documents/{doc_id}/",
                            json=update, timeout=30,
                        )
                return True
        except Exception:
            return False

    def _wait_for_task(self, task_id: str, timeout: int = 120) -> int | None:
        start = time.time()
        while time.time() - start < timeout:
            try:
                resp = self.session.get(f"{self.url}/api/tasks/?task_id={task_id}", timeout=10)
                resp.raise_for_status()
                tasks = resp.json()
                task_list = tasks if isinstance(tasks, list) else tasks.get("results", [])
                if task_list:
                    task = task_list[0]
                    if task.get("status") == "SUCCESS":
                        related = task.get("related_document")
                        if isinstance(related, int):
                            return related
                        if isinstance(related, str) and related.isdigit():
                            return int(related)
                        result = task.get("result", "")
                        if isinstance(result, str):
                            for word in result.split():
                                if word.isdigit():
                                    return int(word)
                        return None
                    elif task.get("status") == "FAILURE":
                        return None
            except Exception:
                pass
            time.sleep(3)
        return None


# ============================================================
# Helper
# ============================================================
def file_hash(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_upload_log() -> dict:
    if os.path.exists(UPLOAD_LOG_FILE):
        try:
            with open(UPLOAD_LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_upload_log(log: dict):
    with open(UPLOAD_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)


def detect_doctype(filepath: str) -> str | None:
    for part in Path(filepath).parts:
        lower = part.lower()
        if lower in FOLDER_DOCTYPE_MAP:
            return FOLDER_DOCTYPE_MAP[lower]
    return None


def scan_folder(folder: str) -> list[str]:
    files = []
    for root, dirs, filenames in os.walk(folder):
        for filename in filenames:
            if Path(filename).suffix.lower() in ALLOWED_EXTENSIONS:
                files.append(os.path.join(root, filename))
    return sorted(files)


def load_config() -> dict:
    defaults = {
        "url": "http://192.168.178.40:8000",
        "token": "",
        "folder": "",
        "auto_tag": "DATEV",
        "schedule_enabled": False,
        "schedule_time": "02:00",
    }
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                defaults.update(saved)
        except Exception:
            pass
    return defaults


def save_config(config: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


# ============================================================
# Windows Task Scheduler
# ============================================================
def get_python_path() -> str:
    return sys.executable


def get_script_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "datev_upload.py")


def create_scheduled_task(schedule_time: str, config: dict) -> tuple[bool, str]:
    """Erstellt eine Windows-Aufgabe für den täglichen Upload."""
    try:
        hour, minute = schedule_time.split(":")
        python = get_python_path()
        script = get_script_path()

        args = f'"{script}" --url "{config["url"]}" --token "{config["token"]}" --folder "{config["folder"]}"'

        # Erst alte Task löschen falls vorhanden
        subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW,
        )

        # Neue Task erstellen
        result = subprocess.run(
            [
                "schtasks", "/Create",
                "/TN", TASK_NAME,
                "/TR", f'"{python}" {args}',
                "/SC", "DAILY",
                "/ST", f"{hour}:{minute}",
                "/RL", "HIGHEST",
                "/F",
            ],
            capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
        )

        if result.returncode == 0:
            return True, f"Aufgabe erstellt: Täglich um {schedule_time} Uhr"
        else:
            return False, f"Fehler: {result.stderr.strip()}"
    except Exception as e:
        return False, str(e)


def delete_scheduled_task() -> tuple[bool, str]:
    """Löscht die Windows-Aufgabe."""
    try:
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if result.returncode == 0:
            return True, "Aufgabe gelöscht"
        else:
            return False, f"Fehler: {result.stderr.strip()}"
    except Exception as e:
        return False, str(e)


def check_scheduled_task() -> str | None:
    """Prüft ob die Windows-Aufgabe existiert."""
    try:
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", TASK_NAME, "/FO", "LIST"],
            capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                if "Next Run Time" in line or "Nächste Laufzeit" in line:
                    return line.split(":", 1)[-1].strip()
            return "Eingerichtet"
        return None
    except Exception:
        return None


# ============================================================
# GUI
# ============================================================
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("DATEV -> Paperless-ngx Uploader")
        self.root.geometry("750x780")
        self.root.configure(bg="#1a1a2e")
        self.root.resizable(True, True)

        self.config = load_config()
        self.is_running = False

        self._setup_styles()
        self._build_ui()
        self._load_values()
        self._check_task_status()

    def _setup_styles(self):
        self.colors = {
            "bg": "#1a1a2e",
            "card": "#16213e",
            "accent": "#0f3460",
            "green": "#22c55e",
            "red": "#ef4444",
            "yellow": "#eab308",
            "text": "#e2e8f0",
            "text_dim": "#94a3b8",
            "input_bg": "#0f3460",
            "border": "#1e3a5f",
        }

        style = ttk.Style()
        style.theme_use("clam")

        style.configure("TFrame", background=self.colors["bg"])
        style.configure("Card.TFrame", background=self.colors["card"])
        style.configure("TLabel", background=self.colors["bg"], foreground=self.colors["text"],
                         font=("Segoe UI", 10))
        style.configure("Card.TLabel", background=self.colors["card"], foreground=self.colors["text"])
        style.configure("Dim.TLabel", background=self.colors["card"], foreground=self.colors["text_dim"],
                         font=("Segoe UI", 9))
        style.configure("Header.TLabel", background=self.colors["bg"], foreground=self.colors["text"],
                         font=("Segoe UI", 12, "bold"))
        style.configure("Title.TLabel", background=self.colors["bg"], foreground=self.colors["green"],
                         font=("Segoe UI", 16, "bold"))
        style.configure("Status.TLabel", background=self.colors["card"], font=("Segoe UI", 9))

        style.configure("Green.TButton", background=self.colors["green"], foreground="white",
                         font=("Segoe UI", 10, "bold"), padding=(15, 8))
        style.map("Green.TButton", background=[("active", "#16a34a")])

        style.configure("TButton", background=self.colors["accent"], foreground=self.colors["text"],
                         font=("Segoe UI", 10), padding=(12, 6))
        style.map("TButton", background=[("active", self.colors["border"])])

        style.configure("Red.TButton", background=self.colors["red"], foreground="white",
                         font=("Segoe UI", 10), padding=(12, 6))
        style.map("Red.TButton", background=[("active", "#dc2626")])

        style.configure("TEntry", fieldbackground=self.colors["input_bg"],
                         foreground=self.colors["text"], insertcolor=self.colors["text"])

        style.configure("TCheckbutton", background=self.colors["card"],
                         foreground=self.colors["text"], font=("Segoe UI", 10))

    def _build_ui(self):
        # Scrollbarer Container
        main = ttk.Frame(self.root)
        main.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        # Titel
        ttk.Label(main, text="DATEV -> Paperless-ngx", style="Title.TLabel").pack(anchor="w")
        ttk.Label(main, text="Belege automatisch hochladen",
                  style="TLabel").pack(anchor="w", pady=(0, 15))

        # --- Verbindung ---
        self._section(main, "Paperless-ngx Verbindung")
        conn_frame = ttk.Frame(main, style="Card.TFrame")
        conn_frame.pack(fill=tk.X, pady=(0, 15))
        conn_inner = ttk.Frame(conn_frame, style="Card.TFrame")
        conn_inner.pack(fill=tk.X, padx=15, pady=12)

        row1 = ttk.Frame(conn_inner, style="Card.TFrame")
        row1.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(row1, text="URL:", style="Card.TLabel", width=8).pack(side=tk.LEFT)
        self.url_var = tk.StringVar()
        url_entry = ttk.Entry(row1, textvariable=self.url_var, width=45)
        url_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        row2 = ttk.Frame(conn_inner, style="Card.TFrame")
        row2.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(row2, text="Token:", style="Card.TLabel", width=8).pack(side=tk.LEFT)
        self.token_var = tk.StringVar()
        token_entry = ttk.Entry(row2, textvariable=self.token_var, show="*", width=45)
        token_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        btn_row = ttk.Frame(conn_inner, style="Card.TFrame")
        btn_row.pack(fill=tk.X)
        ttk.Button(btn_row, text="Verbindung testen", command=self._test_connection).pack(side=tk.LEFT)
        self.conn_status = ttk.Label(btn_row, text="", style="Status.TLabel")
        self.conn_status.pack(side=tk.LEFT, padx=(15, 0))

        # --- Ordner ---
        self._section(main, "Quell-Ordner")
        folder_frame = ttk.Frame(main, style="Card.TFrame")
        folder_frame.pack(fill=tk.X, pady=(0, 15))
        folder_inner = ttk.Frame(folder_frame, style="Card.TFrame")
        folder_inner.pack(fill=tk.X, padx=15, pady=12)

        folder_row = ttk.Frame(folder_inner, style="Card.TFrame")
        folder_row.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(folder_row, text="Ordner:", style="Card.TLabel", width=8).pack(side=tk.LEFT)
        self.folder_var = tk.StringVar()
        ttk.Entry(folder_row, textvariable=self.folder_var, width=38).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(folder_row, text="...", command=self._browse_folder, width=3).pack(side=tk.LEFT)

        tag_row = ttk.Frame(folder_inner, style="Card.TFrame")
        tag_row.pack(fill=tk.X)
        ttk.Label(tag_row, text="Auto-Tag:", style="Card.TLabel", width=8).pack(side=tk.LEFT)
        self.tag_var = tk.StringVar()
        ttk.Entry(tag_row, textvariable=self.tag_var, width=20).pack(side=tk.LEFT)
        ttk.Label(tag_row, text="(wird automatisch zugewiesen)", style="Dim.TLabel").pack(side=tk.LEFT, padx=(10, 0))

        # --- Zeitplan ---
        self._section(main, "Automatischer Upload")
        sched_frame = ttk.Frame(main, style="Card.TFrame")
        sched_frame.pack(fill=tk.X, pady=(0, 15))
        sched_inner = ttk.Frame(sched_frame, style="Card.TFrame")
        sched_inner.pack(fill=tk.X, padx=15, pady=12)

        sched_row = ttk.Frame(sched_inner, style="Card.TFrame")
        sched_row.pack(fill=tk.X, pady=(0, 8))
        self.schedule_var = tk.BooleanVar()
        ttk.Checkbutton(sched_row, text="Täglicher Upload um",
                         variable=self.schedule_var).pack(side=tk.LEFT)
        self.time_var = tk.StringVar()
        time_entry = ttk.Entry(sched_row, textvariable=self.time_var, width=6)
        time_entry.pack(side=tk.LEFT, padx=(5, 5))
        ttk.Label(sched_row, text="Uhr (HH:MM)", style="Dim.TLabel").pack(side=tk.LEFT)

        sched_btn_row = ttk.Frame(sched_inner, style="Card.TFrame")
        sched_btn_row.pack(fill=tk.X)
        ttk.Button(sched_btn_row, text="Aufgabe einrichten",
                   command=self._setup_schedule).pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(sched_btn_row, text="Aufgabe entfernen",
                   command=self._remove_schedule).pack(side=tk.LEFT, padx=(0, 10))
        self.sched_status = ttk.Label(sched_btn_row, text="", style="Status.TLabel")
        self.sched_status.pack(side=tk.LEFT)

        # --- Aktionen ---
        action_frame = ttk.Frame(main)
        action_frame.pack(fill=tk.X, pady=(5, 10))

        ttk.Button(action_frame, text="Vorschau (Dry-Run)",
                   command=self._dry_run).pack(side=tk.LEFT, padx=(0, 10))
        self.upload_btn = ttk.Button(action_frame, text="Jetzt hochladen",
                                      style="Green.TButton", command=self._start_upload)
        self.upload_btn.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(action_frame, text="Einstellungen speichern",
                   command=self._save_settings).pack(side=tk.RIGHT)

        # --- Log ---
        log_label = ttk.Label(main, text="Protokoll", style="Header.TLabel")
        log_label.pack(anchor="w", pady=(5, 5))

        self.log_text = scrolledtext.ScrolledText(
            main, height=12, bg=self.colors["card"], fg=self.colors["text"],
            insertbackground=self.colors["text"], font=("Consolas", 9),
            relief=tk.FLAT, borderwidth=0, padx=10, pady=8,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.configure(state=tk.DISABLED)

        # Tag-Farben im Log
        self.log_text.tag_configure("ok", foreground=self.colors["green"])
        self.log_text.tag_configure("error", foreground=self.colors["red"])
        self.log_text.tag_configure("warn", foreground=self.colors["yellow"])
        self.log_text.tag_configure("info", foreground=self.colors["text_dim"])
        self.log_text.tag_configure("phase", foreground="#60a5fa")

    def _section(self, parent, text):
        ttk.Label(parent, text=text, style="Header.TLabel").pack(anchor="w", pady=(0, 5))

    def _load_values(self):
        self.url_var.set(self.config.get("url", ""))
        self.token_var.set(self.config.get("token", ""))
        self.folder_var.set(self.config.get("folder", ""))
        self.tag_var.set(self.config.get("auto_tag", "DATEV"))
        self.schedule_var.set(self.config.get("schedule_enabled", False))
        self.time_var.set(self.config.get("schedule_time", "02:00"))

    def _get_config(self) -> dict:
        return {
            "url": self.url_var.get().strip(),
            "token": self.token_var.get().strip(),
            "folder": self.folder_var.get().strip(),
            "auto_tag": self.tag_var.get().strip(),
            "schedule_enabled": self.schedule_var.get(),
            "schedule_time": self.time_var.get().strip(),
        }

    def _save_settings(self):
        config = self._get_config()
        save_config(config)
        self.config = config
        self.log("Einstellungen gespeichert", "ok")

    def _browse_folder(self):
        folder = filedialog.askdirectory(title="DATEV Ordner auswählen")
        if folder:
            self.folder_var.set(folder)

    def _check_task_status(self):
        status = check_scheduled_task()
        if status:
            self.sched_status.configure(text=f"Aktiv: {status}", foreground=self.colors["green"])
        else:
            self.sched_status.configure(text="Nicht eingerichtet", foreground=self.colors["text_dim"])

    def log(self, message: str, tag: str = "info"):
        self.log_text.configure(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%H:%M:%S")
        prefix_map = {"ok": "[OK]", "error": "[!!]", "warn": "[??]", "info": "[..]", "phase": "[>>]"}
        prefix = prefix_map.get(tag, "[..]")
        self.log_text.insert(tk.END, f"{timestamp} {prefix} {message}\n", tag)
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _test_connection(self):
        config = self._get_config()
        if not config["url"] or not config["token"]:
            self.conn_status.configure(text="URL und Token eingeben!", foreground=self.colors["red"])
            return

        self.conn_status.configure(text="Teste...", foreground=self.colors["text_dim"])
        self.root.update()

        api = PaperlessAPI(config["url"], config["token"])
        result = api.test_connection()

        if result["ok"]:
            self.conn_status.configure(text=result["message"], foreground=self.colors["green"])
            self.log(result["message"], "ok")
        else:
            self.conn_status.configure(text=result["message"], foreground=self.colors["red"])
            self.log(result["message"], "error")

    def _setup_schedule(self):
        config = self._get_config()
        if not config["url"] or not config["token"] or not config["folder"]:
            messagebox.showwarning("Fehler", "Bitte erst alle Felder ausfüllen.")
            return

        # Einstellungen speichern
        save_config(config)

        time_str = config["schedule_time"]
        try:
            h, m = time_str.split(":")
            int(h)
            int(m)
        except (ValueError, AttributeError):
            messagebox.showwarning("Fehler", "Ungültiges Zeitformat. Bitte HH:MM eingeben.")
            return

        ok, msg = create_scheduled_task(time_str, config)
        if ok:
            self.log(msg, "ok")
            self.sched_status.configure(text=f"Täglich um {time_str} Uhr", foreground=self.colors["green"])
        else:
            self.log(msg, "error")
            messagebox.showerror("Fehler", msg)

        self._check_task_status()

    def _remove_schedule(self):
        ok, msg = delete_scheduled_task()
        if ok:
            self.log("Windows-Aufgabe entfernt", "ok")
        else:
            self.log(msg, "error")
        self._check_task_status()

    def _dry_run(self):
        self._run_upload(dry_run=True)

    def _start_upload(self):
        self._run_upload(dry_run=False)

    def _run_upload(self, dry_run=False):
        if self.is_running:
            return

        config = self._get_config()
        if not config["folder"]:
            messagebox.showwarning("Fehler", "Bitte Ordner auswählen.")
            return
        if not os.path.exists(config["folder"]):
            messagebox.showwarning("Fehler", f"Ordner existiert nicht:\n{config['folder']}")
            return
        if not dry_run and (not config["url"] or not config["token"]):
            messagebox.showwarning("Fehler", "Bitte URL und Token eingeben.")
            return

        # Einstellungen speichern
        save_config(config)

        self.is_running = True
        self.upload_btn.configure(state=tk.DISABLED)

        thread = threading.Thread(target=self._upload_worker, args=(config, dry_run), daemon=True)
        thread.start()

    def _upload_worker(self, config: dict, dry_run: bool):
        try:
            folder = config["folder"]
            self.log(f"Scanne: {folder}", "phase")

            files = scan_folder(folder)
            if not files:
                self.log("Keine Dateien gefunden", "warn")
                return

            self.log(f"{len(files)} Dateien gefunden", "info")

            upload_log = load_upload_log()

            api = None
            tag_id = None
            if not dry_run:
                api = PaperlessAPI(config["url"], config["token"])
                if not api.test_connection()["ok"]:
                    self.log("Verbindung fehlgeschlagen!", "error")
                    return

                if config.get("auto_tag"):
                    tag_id = api.get_or_create_tag(config["auto_tag"])

            new = 0
            uploaded = 0
            skipped = 0
            failed = 0

            for filepath in files:
                try:
                    fhash = file_hash(filepath)
                except Exception:
                    failed += 1
                    continue

                if fhash in upload_log:
                    skipped += 1
                    continue

                new += 1
                rel_path = os.path.relpath(filepath, folder)
                file_size = os.path.getsize(filepath) / 1024
                doctype_name = detect_doctype(filepath)

                if dry_run:
                    dtype = f" [{doctype_name}]" if doctype_name else ""
                    self.log(f"{rel_path} ({file_size:.0f} KB){dtype}", "info")
                    continue

                self.log(f"Upload: {rel_path} ({file_size:.0f} KB)", "info")

                doctype_id = None
                if doctype_name:
                    doctype_id = api.get_or_create_doctype(doctype_name)

                tags = [tag_id] if tag_id else None
                if api.upload(filepath, tags=tags, document_type=doctype_id):
                    upload_log[fhash] = {
                        "file": rel_path,
                        "uploaded_at": datetime.now().isoformat(),
                        "size": os.path.getsize(filepath),
                    }
                    save_upload_log(upload_log)
                    uploaded += 1
                    self.log(f"  {rel_path}", "ok")
                else:
                    failed += 1
                    self.log(f"  {rel_path} fehlgeschlagen", "error")

            # Zusammenfassung
            if dry_run:
                self.log(f"Vorschau: {new} neue, {skipped} bereits vorhanden", "phase")
            else:
                self.log(f"Fertig: {uploaded} hochgeladen, {skipped} bereits vorhanden, {failed} fehlgeschlagen", "phase")

        except Exception as e:
            self.log(f"Fehler: {e}", "error")
        finally:
            self.is_running = False
            self.root.after(0, lambda: self.upload_btn.configure(state=tk.NORMAL))


def main():
    root = tk.Tk()

    # Icon setzen (optional)
    try:
        root.iconbitmap(default="")
    except Exception:
        pass

    app = App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
