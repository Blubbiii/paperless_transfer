#!/usr/bin/env python3
"""
Paperless-ngx Full Migration Script
=====================================
Migriert ALLES von einer Paperless-ngx Instanz auf eine andere:
  - Tags, Correspondents, Document Types, Storage Paths
  - Custom Fields, Saved Views
  - Mail Accounts, Mail Rules, Workflows
  - Dokumente (PDFs inkl. Metadaten, Tags, Notizen, Custom Field Values)

Usage:
    python migrate_settings.py

Konfiguration über die Variablen unten oder Umgebungsvariablen.
"""

import requests
import json
import sys
import os
import time
from pathlib import Path
from datetime import datetime

# ============================================================
# KONFIGURATION - Hier anpassen!
# ============================================================
SOURCE_URL = os.getenv("PAPERLESS_SOURCE_URL", "http://192.168.1.100:8000")
SOURCE_TOKEN = os.getenv("PAPERLESS_SOURCE_TOKEN", "DEIN_SOURCE_TOKEN")

TARGET_URL = os.getenv("PAPERLESS_TARGET_URL", "http://192.168.1.101:8000")
TARGET_TOKEN = os.getenv("PAPERLESS_TARGET_TOKEN", "DEIN_TARGET_TOKEN")

# Was soll migriert werden? True/False zum An-/Abschalten
MIGRATE = {
    "tags": True,
    "correspondents": True,
    "document_types": True,
    "storage_paths": True,
    "custom_fields": True,
    "saved_views": True,
    "mail_accounts": True,
    "mail_rules": True,
    "workflows": True,
    "documents": True,       # Dokumente (PDFs + Metadaten)
    "document_notes": True,  # Notizen/Kommentare auf Dokumenten
}

# Trockenlauf - zeigt nur was passieren würde
DRY_RUN = False

# Dokument-Download Verzeichnis (temporär)
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")

# Wie lange warten bis ein hochgeladenes Dokument verarbeitet ist (Sekunden)
DOCUMENT_POLL_TIMEOUT = 300
DOCUMENT_POLL_INTERVAL = 5

# Parallele Downloads / Uploads begrenzen (schont den Server)
BATCH_SIZE = 5

# ============================================================

# Felder die NICHT kopiert werden sollen (sind instanz-spezifisch)
SKIP_FIELDS = {
    "id", "document_count", "last_correspondence",
    "owner", "permissions", "user_can_change",
}

# Felder die Referenzen auf andere Objekte sind (ID-Mapping nötig)
REFERENCE_FIELDS = {
    "saved_views": {
        "filter_rules": "complex",
    },
    "mail_rules": {
        "account": "mail_accounts",
        "assign_tags": "tags",
        "assign_correspondent": "correspondents",
        "assign_document_type": "document_types",
    },
    "workflows": {
        "triggers": "complex",
        "actions": "complex",
    },
}

# Reihenfolge ist wichtig wegen Abhängigkeiten!
MIGRATION_ORDER = [
    "tags",
    "correspondents",
    "document_types",
    "storage_paths",
    "custom_fields",
    "saved_views",
    "mail_accounts",
    "mail_rules",
    "workflows",
    "documents",
]


class PaperlessAPI:
    """Wrapper für die Paperless-ngx REST API."""

    def __init__(self, base_url: str, token: str, name: str = ""):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.name = name
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {token}",
            "Accept": "application/json",
        })

    def test_connection(self) -> bool:
        """Testet ob die Verbindung funktioniert."""
        try:
            resp = self.session.get(f"{self.base_url}/api/", timeout=10)
            resp.raise_for_status()
            print(f"  [OK] {self.name} ({self.base_url}) erreichbar")
            return True
        except Exception as e:
            print(f"  [FEHLER] {self.name} ({self.base_url}): {e}")
            return False

    def get_all(self, resource: str) -> list:
        """Holt alle Einträge eines Ressourcentyps (mit Pagination)."""
        results = []
        url = f"{self.base_url}/api/{resource}/?page_size=100"
        while url:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "results" in data:
                results.extend(data["results"])
                url = data.get("next")
            else:
                if isinstance(data, list):
                    results.extend(data)
                break
        return results

    def get_one(self, resource: str, resource_id: int) -> dict | None:
        """Holt einen einzelnen Eintrag."""
        try:
            resp = self.session.get(
                f"{self.base_url}/api/{resource}/{resource_id}/",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def create(self, resource: str, data: dict) -> dict | None:
        """Erstellt einen neuen Eintrag (JSON)."""
        try:
            resp = self.session.post(
                f"{self.base_url}/api/{resource}/",
                json=data,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            print(f"    [FEHLER] Erstellen von {resource}: {e}")
            if e.response is not None:
                try:
                    print(f"    Detail: {e.response.json()}")
                except Exception:
                    print(f"    Detail: {e.response.text[:500]}")
            return None

    def update(self, resource: str, resource_id: int, data: dict) -> dict | None:
        """Aktualisiert einen Eintrag (PATCH)."""
        try:
            resp = self.session.patch(
                f"{self.base_url}/api/{resource}/{resource_id}/",
                json=data,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            print(f"    [FEHLER] Update {resource}/{resource_id}: {e}")
            if e.response is not None:
                try:
                    print(f"    Detail: {e.response.json()}")
                except Exception:
                    print(f"    Detail: {e.response.text[:500]}")
            return None

    def download_document(self, doc_id: int, filepath: str) -> bool:
        """Lädt das Original-Dokument herunter."""
        try:
            resp = self.session.get(
                f"{self.base_url}/api/documents/{doc_id}/download/",
                stream=True,
                timeout=120,
            )
            resp.raise_for_status()
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            print(f"    [FEHLER] Download Dokument {doc_id}: {e}")
            return False

    def upload_document(self, filepath: str, metadata: dict) -> int | None:
        """
        Lädt ein Dokument hoch mit Metadaten.
        Gibt die Task-ID zurück (Paperless verarbeitet async).
        """
        try:
            # Multipart form data - kein JSON Content-Type Header!
            files = {
                "document": open(filepath, "rb"),
            }
            form_data = {}

            # Titel
            if metadata.get("title"):
                form_data["title"] = metadata["title"]

            # Erstelldatum
            if metadata.get("created"):
                form_data["created"] = metadata["created"]

            # Correspondent
            if metadata.get("correspondent"):
                form_data["correspondent"] = str(metadata["correspondent"])

            # Document Type
            if metadata.get("document_type"):
                form_data["document_type"] = str(metadata["document_type"])

            # Storage Path
            if metadata.get("storage_path"):
                form_data["storage_path"] = str(metadata["storage_path"])

            # ASN (Archive Serial Number)
            if metadata.get("archive_serial_number"):
                form_data["archive_serial_number"] = str(metadata["archive_serial_number"])

            resp = self.session.post(
                f"{self.base_url}/api/documents/post_document/",
                files=files,
                data=form_data,
                timeout=120,
            )
            files["document"].close()
            resp.raise_for_status()

            # Antwort ist eine Task-UUID
            task_id = resp.text.strip().strip('"')
            return task_id

        except Exception as e:
            print(f"    [FEHLER] Upload {filepath}: {e}")
            return None

    def get_task_status(self, task_id: str) -> dict | None:
        """Prüft den Status eines Upload-Tasks."""
        try:
            resp = self.session.get(
                f"{self.base_url}/api/tasks/?task_id={task_id}",
                timeout=30,
            )
            resp.raise_for_status()
            tasks = resp.json()
            if isinstance(tasks, list) and len(tasks) > 0:
                return tasks[0]
            elif isinstance(tasks, dict) and "results" in tasks:
                results = tasks["results"]
                if results:
                    return results[0]
            return None
        except Exception:
            return None

    def wait_for_task(self, task_id: str) -> int | None:
        """
        Wartet bis ein Upload-Task fertig ist.
        Gibt die neue Dokument-ID zurück.
        """
        start = time.time()
        while time.time() - start < DOCUMENT_POLL_TIMEOUT:
            task = self.get_task_status(task_id)
            if task:
                status = task.get("status", "")
                if status == "SUCCESS":
                    # Dokument-ID aus dem Task-Result extrahieren
                    related_doc = task.get("related_document")
                    if related_doc:
                        # related_document kann eine ID oder ein String sein
                        if isinstance(related_doc, str) and related_doc.isdigit():
                            return int(related_doc)
                        elif isinstance(related_doc, int):
                            return related_doc
                    # Fallback: result parsen
                    result = task.get("result")
                    if result and isinstance(result, str):
                        # "Success. New document id X created"
                        for word in result.split():
                            if word.isdigit():
                                return int(word)
                    return None
                elif status == "FAILURE":
                    print(f"    [FEHLER] Task fehlgeschlagen: {task.get('result', '?')}")
                    return None
            time.sleep(DOCUMENT_POLL_INTERVAL)

        print(f"    [TIMEOUT] Task {task_id} nicht innerhalb von {DOCUMENT_POLL_TIMEOUT}s fertig")
        return None

    def get_document_notes(self, doc_id: int) -> list:
        """Holt Notizen eines Dokuments."""
        try:
            resp = self.session.get(
                f"{self.base_url}/api/documents/{doc_id}/notes/",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return []

    def add_document_note(self, doc_id: int, note: str) -> bool:
        """Fügt eine Notiz zu einem Dokument hinzu."""
        try:
            resp = self.session.post(
                f"{self.base_url}/api/documents/{doc_id}/notes/",
                json={"note": note},
                timeout=30,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            print(f"    [FEHLER] Notiz für Dok {doc_id}: {e}")
            return False

    def get_document_custom_fields(self, doc_id: int) -> list:
        """Holt Custom Field Werte eines Dokuments (Teil der Dokument-Daten)."""
        doc = self.get_one("documents", doc_id)
        if doc:
            return doc.get("custom_fields", [])
        return []

    def search_documents(self, title: str) -> list:
        """Sucht Dokumente nach Titel."""
        try:
            resp = self.session.get(
                f"{self.base_url}/api/documents/?title__icontains={requests.utils.quote(title)}&page_size=10",
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception:
            return []


def clean_data(data: dict, resource: str) -> dict:
    """Entfernt instanz-spezifische Felder aus den Daten."""
    cleaned = {}
    for key, value in data.items():
        if key in SKIP_FIELDS:
            continue
        cleaned[key] = value
    return cleaned


def find_by_name(items: list, name: str) -> dict | None:
    """Findet ein Item anhand des Namens."""
    for item in items:
        if item.get("name") == name:
            return item
    return None


def migrate_resource(
    source: PaperlessAPI,
    target: PaperlessAPI,
    resource: str,
    id_mapping: dict,
) -> dict:
    """
    Migriert einen Ressourcentyp von Source nach Target.
    Gibt ein ID-Mapping zurück: {alte_id: neue_id}
    """
    print(f"\n{'='*60}")
    print(f"  Migriere: {resource}")
    print(f"{'='*60}")

    mapping = {}

    try:
        source_items = source.get_all(resource)
    except Exception as e:
        print(f"  [WARNUNG] Konnte {resource} nicht von Source lesen: {e}")
        return mapping

    try:
        target_items = target.get_all(resource)
    except Exception as e:
        print(f"  [WARNUNG] Konnte {resource} nicht von Target lesen: {e}")
        target_items = []

    target_names = {item.get("name"): item for item in target_items}

    print(f"  Source: {len(source_items)} Einträge")
    print(f"  Target: {len(target_items)} Einträge vorhanden")

    created = 0
    skipped = 0

    for item in source_items:
        name = item.get("name", "???")

        # Prüfen ob schon vorhanden
        if name in target_names:
            print(f"    [SKIP] '{name}' existiert bereits auf Target")
            mapping[item["id"]] = target_names[name]["id"]
            skipped += 1
            continue

        # Daten bereinigen
        data = clean_data(item, resource)

        # Referenzen umschreiben (z.B. tag IDs)
        data = remap_references(data, resource, id_mapping)

        if DRY_RUN:
            print(f"    [DRY] Würde erstellen: '{name}'")
            created += 1
            continue

        result = target.create(resource, data)
        if result:
            mapping[item["id"]] = result["id"]
            print(f"    [OK] '{name}' erstellt (ID {item['id']} -> {result['id']})")
            created += 1
        else:
            print(f"    [FEHLER] '{name}' konnte nicht erstellt werden")

    print(f"  Ergebnis: {created} erstellt, {skipped} übersprungen")
    return mapping


def remap_references(data: dict, resource: str, id_mapping: dict) -> dict:
    """Schreibt Referenz-IDs auf die neuen IDs um."""
    refs = REFERENCE_FIELDS.get(resource, {})

    for field, ref_type in refs.items():
        if field not in data:
            continue

        if ref_type == "complex":
            if resource == "saved_views" and field == "filter_rules":
                data[field] = remap_filter_rules(data[field], id_mapping)
            continue

        value = data[field]

        if isinstance(value, list):
            ref_map = id_mapping.get(ref_type, {})
            data[field] = [ref_map.get(v, v) for v in value]
        elif isinstance(value, int):
            ref_map = id_mapping.get(ref_type, {})
            data[field] = ref_map.get(value, value)

    return data


def remap_filter_rules(rules: list, id_mapping: dict) -> list:
    """Mapped IDs in saved_view filter_rules um."""
    RULE_TYPE_MAP = {
        3: "correspondents",
        4: "document_types",
        6: "tags",
        17: "tags",
        24: "storage_paths",
        26: "custom_fields",
    }

    remapped = []
    for rule in rules:
        rule = dict(rule)
        rule_type = rule.get("rule_type")
        if rule_type in RULE_TYPE_MAP:
            ref_type = RULE_TYPE_MAP[rule_type]
            ref_map = id_mapping.get(ref_type, {})
            value = rule.get("value")
            if value and value.isdigit():
                old_id = int(value)
                new_id = ref_map.get(old_id, old_id)
                rule["value"] = str(new_id)
        remapped.append(rule)
    return remapped


def migrate_documents(
    source: PaperlessAPI,
    target: PaperlessAPI,
    id_mapping: dict,
) -> dict:
    """
    Migriert alle Dokumente inkl. Dateien und Metadaten.
    Gibt ein Dokument-ID-Mapping zurück.
    """
    print(f"\n{'='*60}")
    print(f"  Migriere: Dokumente")
    print(f"{'='*60}")

    doc_mapping = {}

    # Download-Verzeichnis erstellen
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # Alle Dokumente von Source holen
    print("  Lade Dokumentenliste von Source...")
    source_docs = source.get_all("documents")
    print(f"  Source: {len(source_docs)} Dokumente")

    # Bereits vorhandene Dokumente auf Target prüfen
    print("  Lade Dokumentenliste von Target...")
    target_docs = target.get_all("documents")
    # Index nach Titel + Erstelldatum für Duplikat-Erkennung
    target_doc_index = {}
    for doc in target_docs:
        key = f"{doc.get('title', '')}_{doc.get('created', '')}"
        target_doc_index[key] = doc

    print(f"  Target: {len(target_docs)} Dokumente vorhanden")

    created = 0
    skipped = 0
    failed = 0

    for i, doc in enumerate(source_docs, 1):
        title = doc.get("title", "Unbenannt")
        doc_id = doc["id"]
        created_date = doc.get("created", "")

        print(f"\n  [{i}/{len(source_docs)}] '{title}' (ID: {doc_id})")

        # Duplikat-Check
        dup_key = f"{title}_{created_date}"
        if dup_key in target_doc_index:
            existing = target_doc_index[dup_key]
            print(f"    [SKIP] Existiert bereits auf Target (ID: {existing['id']})")
            doc_mapping[doc_id] = existing["id"]
            skipped += 1
            continue

        if DRY_RUN:
            print(f"    [DRY] Würde migrieren: '{title}'")
            created += 1
            continue

        # 1. Dokument herunterladen
        # Dateiendung aus original_file_name oder Fallback
        original_name = doc.get("original_file_name", f"doc_{doc_id}.pdf")
        ext = Path(original_name).suffix or ".pdf"
        download_path = os.path.join(DOWNLOAD_DIR, f"{doc_id}{ext}")

        print(f"    Downloading... ({original_name})")
        if not source.download_document(doc_id, download_path):
            failed += 1
            continue

        file_size = os.path.getsize(download_path)
        print(f"    Heruntergeladen: {file_size / 1024:.1f} KB")

        # 2. Metadaten vorbereiten (IDs remappen)
        metadata = {
            "title": title,
            "created": created_date,
        }

        # Correspondent remappen
        if doc.get("correspondent"):
            corr_map = id_mapping.get("correspondents", {})
            new_corr = corr_map.get(doc["correspondent"])
            if new_corr:
                metadata["correspondent"] = new_corr

        # Document Type remappen
        if doc.get("document_type"):
            dt_map = id_mapping.get("document_types", {})
            new_dt = dt_map.get(doc["document_type"])
            if new_dt:
                metadata["document_type"] = new_dt

        # Storage Path remappen
        if doc.get("storage_path"):
            sp_map = id_mapping.get("storage_paths", {})
            new_sp = sp_map.get(doc["storage_path"])
            if new_sp:
                metadata["storage_path"] = new_sp

        # ASN
        if doc.get("archive_serial_number"):
            metadata["archive_serial_number"] = doc["archive_serial_number"]

        # 3. Dokument hochladen
        print(f"    Uploading...")
        task_id = target.upload_document(download_path, metadata)
        if not task_id:
            failed += 1
            # Temporäre Datei aufräumen
            try:
                os.remove(download_path)
            except OSError:
                pass
            continue

        # 4. Warten bis Dokument verarbeitet ist
        print(f"    Warte auf Verarbeitung (Task: {task_id[:8]}...)")
        new_doc_id = target.wait_for_task(task_id)

        # Temporäre Datei aufräumen
        try:
            os.remove(download_path)
        except OSError:
            pass

        if not new_doc_id:
            print(f"    [WARNUNG] Dokument wurde hochgeladen, aber ID konnte nicht ermittelt werden")
            # Versuche per Titel zu finden
            found = target.search_documents(title)
            if found:
                new_doc_id = found[0]["id"]
                print(f"    [OK] Per Suche gefunden: ID {new_doc_id}")

        if new_doc_id:
            doc_mapping[doc_id] = new_doc_id
            print(f"    [OK] Dokument erstellt (ID {doc_id} -> {new_doc_id})")

            # 5. Tags zuweisen (geht nur nach dem Upload per PATCH)
            if doc.get("tags"):
                tag_map = id_mapping.get("tags", {})
                new_tags = [tag_map.get(t, t) for t in doc["tags"] if tag_map.get(t)]
                if new_tags:
                    target.update("documents", new_doc_id, {"tags": new_tags})
                    print(f"    [OK] {len(new_tags)} Tags zugewiesen")

            # 6. Custom Fields zuweisen
            if doc.get("custom_fields"):
                cf_map = id_mapping.get("custom_fields", {})
                new_cfs = []
                for cf in doc["custom_fields"]:
                    old_field_id = cf.get("field")
                    new_field_id = cf_map.get(old_field_id)
                    if new_field_id:
                        new_cfs.append({
                            "field": new_field_id,
                            "value": cf.get("value"),
                        })
                if new_cfs:
                    target.update("documents", new_doc_id, {"custom_fields": new_cfs})
                    print(f"    [OK] {len(new_cfs)} Custom Fields zugewiesen")

            created += 1
        else:
            print(f"    [FEHLER] Konnte neue Dokument-ID nicht ermitteln")
            failed += 1

    # Download-Verzeichnis aufräumen
    try:
        if os.path.exists(DOWNLOAD_DIR) and not os.listdir(DOWNLOAD_DIR):
            os.rmdir(DOWNLOAD_DIR)
    except OSError:
        pass

    print(f"\n  Dokument-Ergebnis: {created} erstellt, {skipped} übersprungen, {failed} fehlgeschlagen")
    return doc_mapping


def migrate_document_notes(
    source: PaperlessAPI,
    target: PaperlessAPI,
    doc_mapping: dict,
):
    """Migriert Notizen/Kommentare von allen Dokumenten."""
    print(f"\n{'='*60}")
    print(f"  Migriere: Dokument-Notizen")
    print(f"{'='*60}")

    if not doc_mapping:
        print("  [SKIP] Kein Dokument-Mapping vorhanden")
        return

    total_notes = 0
    docs_with_notes = 0

    for old_id, new_id in doc_mapping.items():
        notes = source.get_document_notes(old_id)
        if not notes:
            continue

        docs_with_notes += 1

        # Bestehende Notizen auf Target prüfen
        existing_notes = target.get_document_notes(new_id)
        existing_texts = {n.get("note", "") for n in existing_notes}

        for note in notes:
            note_text = note.get("note", "")
            if not note_text or note_text in existing_texts:
                continue

            if DRY_RUN:
                print(f"    [DRY] Würde Notiz zu Dok {new_id} hinzufügen")
                total_notes += 1
                continue

            if target.add_document_note(new_id, note_text):
                total_notes += 1

    print(f"  Ergebnis: {total_notes} Notizen für {docs_with_notes} Dokumente migriert")


def export_backup(source: PaperlessAPI):
    """Exportiert alle Einstellungen als JSON-Backup."""
    backup = {}
    all_resources = MIGRATION_ORDER.copy()

    for resource in all_resources:
        if not MIGRATE.get(resource, False):
            continue
        try:
            items = source.get_all(resource)
            backup[resource] = items
            print(f"  [OK] {resource}: {len(items)} Einträge")
        except Exception as e:
            print(f"  [WARNUNG] {resource}: {e}")
            backup[resource] = []

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"paperless_backup_{timestamp}.json"
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(backup, f, indent=2, ensure_ascii=False)
    print(f"\n  Backup gespeichert: {filepath}")
    return filepath


def print_summary(id_mapping: dict, doc_mapping: dict):
    """Gibt eine Zusammenfassung der Migration aus."""
    print("\n" + "=" * 60)
    print("  Migration abgeschlossen!")
    print("=" * 60)

    for resource in MIGRATION_ORDER:
        if resource == "documents":
            count = len(doc_mapping)
        elif resource in id_mapping:
            count = len(id_mapping[resource])
        else:
            continue
        print(f"  {resource:20s}: {count} gemappt")


def main():
    print("=" * 60)
    print("  Paperless-ngx Full Migration")
    print("=" * 60)
    print(f"  Source: {SOURCE_URL}")
    print(f"  Target: {TARGET_URL}")

    if DRY_RUN:
        print("\n  *** TROCKENLAUF - Es werden keine Änderungen vorgenommen ***")

    # Platzhalter-Check
    if "DEIN_" in SOURCE_TOKEN or "DEIN_" in TARGET_TOKEN:
        print("\n[FEHLER] Bitte konfiguriere die Tokens!")
        print("  Entweder direkt im Script oder über Umgebungsvariablen:")
        print("    export PAPERLESS_SOURCE_URL=http://192.168.x.x:8000")
        print("    export PAPERLESS_SOURCE_TOKEN=dein_token_hier")
        print("    export PAPERLESS_TARGET_URL=http://192.168.x.x:8000")
        print("    export PAPERLESS_TARGET_TOKEN=dein_token_hier")
        print("\n  Token findest du in Paperless unter:")
        print("    Profil (oben rechts) -> 'My Profile' -> API Token (Kreispfeil-Button)")
        sys.exit(1)

    # Instanzen initialisieren
    source = PaperlessAPI(SOURCE_URL, SOURCE_TOKEN, "Source (Alt)")
    target = PaperlessAPI(TARGET_URL, TARGET_TOKEN, "Target (Neu)")

    # Verbindung testen
    print("\nVerbindungstest:")
    if not source.test_connection() or not target.test_connection():
        print("\n[FEHLER] Verbindung fehlgeschlagen. Bitte URLs und Tokens prüfen.")
        sys.exit(1)

    # Backup erstellen
    print("\nErstelle Backup der Source-Einstellungen...")
    export_backup(source)

    # Phase 1: Einstellungen migrieren
    print("\n" + "#" * 60)
    print("  PHASE 1: Einstellungen migrieren")
    print("#" * 60)

    id_mapping = {}

    for resource in MIGRATION_ORDER:
        if resource == "documents":
            continue  # Dokumente kommen in Phase 2
        if not MIGRATE.get(resource, False):
            print(f"\n  [SKIP] {resource} (deaktiviert)")
            continue

        mapping = migrate_resource(source, target, resource, id_mapping)
        id_mapping[resource] = mapping

    # Phase 2: Dokumente migrieren
    doc_mapping = {}
    if MIGRATE.get("documents", False):
        print("\n" + "#" * 60)
        print("  PHASE 2: Dokumente migrieren")
        print("#" * 60)

        doc_mapping = migrate_documents(source, target, id_mapping)

        # Phase 3: Notizen migrieren
        if MIGRATE.get("document_notes", False) and doc_mapping:
            print("\n" + "#" * 60)
            print("  PHASE 3: Dokument-Notizen migrieren")
            print("#" * 60)

            migrate_document_notes(source, target, doc_mapping)

    # Zusammenfassung
    print_summary(id_mapping, doc_mapping)

    if DRY_RUN:
        print("\n  *** Dies war ein Trockenlauf. ***")
        print("  *** Setze DRY_RUN=False für echte Migration ***")

    # Mappings speichern
    mapping_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "id_mapping.json")
    all_mappings = {**id_mapping, "documents": doc_mapping}
    with open(mapping_file, "w", encoding="utf-8") as f:
        # Konvertiere int-Keys zu strings für JSON
        serializable = {}
        for key, val in all_mappings.items():
            serializable[key] = {str(k): v for k, v in val.items()}
        json.dump(serializable, f, indent=2)
    print(f"\n  ID-Mapping gespeichert: {mapping_file}")
    print(f"\n  Fertig!")


if __name__ == "__main__":
    main()
