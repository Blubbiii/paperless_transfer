#!/usr/bin/env python3
"""
DATEV -> Paperless-ngx Auto-Uploader
======================================
Scannt einen Ordner (rekursiv) und lädt neue Dateien zu Paperless-ngx hoch.
Merkt sich bereits hochgeladene Dateien um Duplikate zu vermeiden.

Usage:
    python datev_upload.py                    # Einmalig ausführen
    python datev_upload.py --watch            # Dauerhaft überwachen
    python datev_upload.py --dry-run          # Nur anzeigen was hochgeladen würde

Kann als Windows-Aufgabe (Task Scheduler) eingerichtet werden.
"""

import os
import sys
import json
import hashlib
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime


# ============================================================
# KONFIGURATION
# ============================================================
PAPERLESS_URL = os.getenv("PAPERLESS_URL", "http://192.168.178.40:8000")
PAPERLESS_TOKEN = os.getenv("PAPERLESS_TOKEN", "DEIN_TOKEN")

# Ordner der überwacht werden soll (kann auch Unterordner haben)
WATCH_FOLDER = os.getenv("DATEV_FOLDER", r"C:\DATEV\Belege")

# Welche Dateitypen hochgeladen werden sollen
ALLOWED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".gif", ".bmp", ".webp"}

# Datei die sich merkt was schon hochgeladen wurde
UPLOAD_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datev_uploaded.json")

# Überwachungs-Intervall in Sekunden (für --watch Modus)
WATCH_INTERVAL = 60

# Optional: Tag der automatisch zugewiesen wird (Name, nicht ID)
AUTO_TAG = os.getenv("DATEV_TAG", "DATEV")

# Optional: Ordnername -> Dokumenttyp-Mapping
# Wenn eine Datei in einem Ordner liegt dessen Name hier vorkommt,
# wird der entsprechende Dokumenttyp zugewiesen
FOLDER_DOCTYPE_MAP = {
    "rechnungen": "Rechnung",
    "rechnung": "Rechnung",
    "invoices": "Rechnung",
    "gutschriften": "Gutschrift",
    "gutschrift": "Gutschrift",
    "credit": "Gutschrift",
    "angebote": "Angebot",
    "angebot": "Angebot",
    "verträge": "Vertrag",
    "vertrag": "Vertrag",
    "quittungen": "Quittung",
    "quittung": "Quittung",
    "kontoauszüge": "Kontoauszug",
    "kontoauszug": "Kontoauszug",
    "mahnungen": "Mahnung",
    "mahnung": "Mahnung",
}


# ============================================================

class PaperlessUploader:
    def __init__(self, url: str, token: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {token}",
            "Accept": "application/json",
        })
        self._tag_cache = {}
        self._doctype_cache = {}

    def test_connection(self) -> bool:
        try:
            resp = self.session.get(f"{self.url}/api/tags/?page_size=1", timeout=10)
            resp.raise_for_status()
            return True
        except Exception as e:
            print(f"[FEHLER] Verbindung fehlgeschlagen: {e}")
            return False

    def get_or_create_tag(self, name: str) -> int | None:
        """Holt oder erstellt einen Tag und gibt die ID zurück."""
        if name in self._tag_cache:
            return self._tag_cache[name]

        try:
            # Suchen
            resp = self.session.get(
                f"{self.url}/api/tags/?name__iexact={requests.utils.quote(name, safe='')}",
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                self._tag_cache[name] = results[0]["id"]
                return results[0]["id"]

            # Erstellen
            resp = self.session.post(
                f"{self.url}/api/tags/",
                json={"name": name},
                timeout=10,
            )
            resp.raise_for_status()
            tag_id = resp.json()["id"]
            self._tag_cache[name] = tag_id
            print(f"  Tag '{name}' erstellt (ID: {tag_id})")
            return tag_id
        except Exception as e:
            print(f"  [WARNUNG] Tag '{name}' konnte nicht erstellt werden: {e}")
            return None

    def get_or_create_doctype(self, name: str) -> int | None:
        """Holt oder erstellt einen Dokumenttyp."""
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

            resp = self.session.post(
                f"{self.url}/api/document_types/",
                json={"name": name},
                timeout=10,
            )
            resp.raise_for_status()
            dt_id = resp.json()["id"]
            self._doctype_cache[name] = dt_id
            print(f"  Dokumenttyp '{name}' erstellt (ID: {dt_id})")
            return dt_id
        except Exception as e:
            print(f"  [WARNUNG] Dokumenttyp '{name}' konnte nicht erstellt werden: {e}")
            return None

    def upload(self, filepath: str, tags: list[int] = None, document_type: int = None) -> bool:
        """Lädt eine Datei zu Paperless hoch."""
        try:
            filename = os.path.basename(filepath)
            with open(filepath, "rb") as f:
                files = {"document": (filename, f)}
                data = {}

                # Titel = Dateiname ohne Endung
                data["title"] = Path(filename).stem

                resp = self.session.post(
                    f"{self.url}/api/documents/post_document/",
                    files=files,
                    data=data,
                    timeout=300,
                )
                resp.raise_for_status()

                task_id = resp.text.strip().strip('"')

                # Warte auf Verarbeitung und weise dann Tags/Typ zu
                if tags or document_type:
                    doc_id = self._wait_for_task(task_id)
                    if doc_id:
                        update_data = {}
                        if tags:
                            update_data["tags"] = tags
                        if document_type:
                            update_data["document_type"] = document_type
                        self.session.patch(
                            f"{self.url}/api/documents/{doc_id}/",
                            json=update_data,
                            timeout=30,
                        )

                return True
        except Exception as e:
            print(f"  [FEHLER] Upload fehlgeschlagen: {e}")
            return False

    def _wait_for_task(self, task_id: str, timeout: int = 120) -> int | None:
        start = time.time()
        while time.time() - start < timeout:
            try:
                resp = self.session.get(
                    f"{self.url}/api/tasks/?task_id={task_id}",
                    timeout=10,
                )
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


def file_hash(filepath: str) -> str:
    """Berechnet SHA-256 Hash einer Datei."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_upload_log() -> dict:
    """Lädt das Upload-Log (bereits hochgeladene Dateien)."""
    if os.path.exists(UPLOAD_LOG):
        try:
            with open(UPLOAD_LOG, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_upload_log(log: dict):
    """Speichert das Upload-Log."""
    with open(UPLOAD_LOG, "w", encoding="utf-8") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)


def detect_doctype_from_path(filepath: str) -> str | None:
    """Erkennt den Dokumenttyp anhand des Ordnernamens."""
    parts = Path(filepath).parts
    for part in parts:
        lower = part.lower()
        if lower in FOLDER_DOCTYPE_MAP:
            return FOLDER_DOCTYPE_MAP[lower]
    return None


def scan_folder(folder: str) -> list[str]:
    """Scannt einen Ordner rekursiv nach Dateien."""
    files = []
    for root, dirs, filenames in os.walk(folder):
        for filename in filenames:
            ext = Path(filename).suffix.lower()
            if ext in ALLOWED_EXTENSIONS:
                files.append(os.path.join(root, filename))
    return sorted(files)


def run_sync(uploader: PaperlessUploader, dry_run: bool = False):
    """Führt einen Sync-Durchlauf durch."""
    if not os.path.exists(WATCH_FOLDER):
        print(f"[FEHLER] Ordner existiert nicht: {WATCH_FOLDER}")
        return

    # Dateien scannen
    files = scan_folder(WATCH_FOLDER)
    if not files:
        print(f"Keine Dateien gefunden in: {WATCH_FOLDER}")
        return

    print(f"Gefunden: {len(files)} Dateien in {WATCH_FOLDER}")

    # Upload-Log laden
    log = load_upload_log()

    # Auto-Tag vorbereiten
    tag_id = None
    if AUTO_TAG and not dry_run:
        tag_id = uploader.get_or_create_tag(AUTO_TAG)

    new_files = 0
    uploaded = 0
    skipped = 0
    failed = 0

    for filepath in files:
        # Hash berechnen
        try:
            fhash = file_hash(filepath)
        except Exception as e:
            print(f"  [FEHLER] Kann nicht lesen: {filepath}: {e}")
            failed += 1
            continue

        # Bereits hochgeladen?
        if fhash in log:
            skipped += 1
            continue

        new_files += 1
        rel_path = os.path.relpath(filepath, WATCH_FOLDER)
        file_size = os.path.getsize(filepath) / 1024

        # Dokumenttyp aus Ordner erkennen
        doctype_name = detect_doctype_from_path(filepath)
        doctype_id = None

        if dry_run:
            dtype_info = f" -> Typ: {doctype_name}" if doctype_name else ""
            print(f"  [DRY] {rel_path} ({file_size:.0f} KB){dtype_info}")
            continue

        print(f"  Uploading: {rel_path} ({file_size:.0f} KB)")

        # Dokumenttyp holen/erstellen
        if doctype_name:
            doctype_id = uploader.get_or_create_doctype(doctype_name)

        # Upload
        tags = [tag_id] if tag_id else None
        if uploader.upload(filepath, tags=tags, document_type=doctype_id):
            log[fhash] = {
                "file": rel_path,
                "uploaded_at": datetime.now().isoformat(),
                "size": os.path.getsize(filepath),
            }
            save_upload_log(log)
            uploaded += 1
            print(f"  [OK] {rel_path}")
        else:
            failed += 1
            print(f"  [FEHLER] {rel_path}")

    # Zusammenfassung
    print(f"\nErgebnis: {new_files} neue Dateien, {uploaded} hochgeladen, {skipped} bereits vorhanden, {failed} fehlgeschlagen")


def main():
    parser = argparse.ArgumentParser(description="DATEV -> Paperless-ngx Auto-Uploader")
    parser.add_argument("--watch", action="store_true", help="Dauerhaft überwachen")
    parser.add_argument("--dry-run", action="store_true", help="Nur anzeigen, nichts hochladen")
    parser.add_argument("--folder", type=str, help="Ordner der überwacht werden soll")
    parser.add_argument("--url", type=str, help="Paperless URL")
    parser.add_argument("--token", type=str, help="Paperless API Token")
    args = parser.parse_args()

    global WATCH_FOLDER, PAPERLESS_URL, PAPERLESS_TOKEN
    if args.folder:
        WATCH_FOLDER = args.folder
    if args.url:
        PAPERLESS_URL = args.url
    if args.token:
        PAPERLESS_TOKEN = args.token

    print("=" * 50)
    print("  DATEV -> Paperless-ngx Uploader")
    print("=" * 50)
    print(f"  Paperless: {PAPERLESS_URL}")
    print(f"  Ordner:    {WATCH_FOLDER}")

    if "DEIN_" in PAPERLESS_TOKEN:
        print("\n[FEHLER] Bitte Token konfigurieren!")
        print("  Im Script, als Umgebungsvariable, oder mit --token")
        sys.exit(1)

    uploader = PaperlessUploader(PAPERLESS_URL, PAPERLESS_TOKEN)

    if not args.dry_run:
        print("\nVerbindungstest...")
        if not uploader.test_connection():
            sys.exit(1)
        print("  [OK] Verbunden")

    if args.watch:
        print(f"\nÜberwache Ordner alle {WATCH_INTERVAL} Sekunden... (Strg+C zum Beenden)")
        try:
            while True:
                print(f"\n--- Scan {datetime.now().strftime('%H:%M:%S')} ---")
                run_sync(uploader, dry_run=args.dry_run)
                time.sleep(WATCH_INTERVAL)
        except KeyboardInterrupt:
            print("\nBeendet.")
    else:
        print()
        run_sync(uploader, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
