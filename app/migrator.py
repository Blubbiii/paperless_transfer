"""
Paperless-ngx Migration Engine
================================
Migriert alles von einer Paperless-ngx Instanz auf eine andere:
  - Tags, Correspondents, Document Types, Storage Paths
  - Custom Fields, Saved Views
  - Mail Accounts, Mail Rules, Workflows
  - Dokumente (PDFs inkl. Metadaten, Tags, Notizen, Custom Field Values)
"""

import requests
import json
import os
import time
import queue
import threading
from pathlib import Path
from datetime import datetime


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
]

RESOURCE_LABELS = {
    "tags": "Tags",
    "correspondents": "Korrespondenten",
    "document_types": "Dokumenttypen",
    "storage_paths": "Speicherpfade",
    "custom_fields": "Benutzerdefinierte Felder",
    "saved_views": "Gespeicherte Ansichten",
    "mail_accounts": "Mail-Konten",
    "mail_rules": "Mail-Regeln",
    "workflows": "Workflows",
    "documents": "Dokumente",
    "document_notes": "Dokument-Notizen",
}

DOWNLOAD_DIR = "/tmp/paperless_downloads"
DOCUMENT_POLL_TIMEOUT = 300
DOCUMENT_POLL_INTERVAL = 5


class PaperlessAPI:
    """Wrapper für die Paperless-ngx REST API."""

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {token}",
            "Accept": "application/json",
        })

    def test_connection(self) -> dict:
        """Testet ob die Verbindung funktioniert. Gibt Status zurück."""
        try:
            resp = self.session.get(f"{self.base_url}/api/", timeout=10)
            resp.raise_for_status()
            return {"ok": True, "message": "Verbindung erfolgreich"}
        except requests.exceptions.ConnectionError:
            return {"ok": False, "message": f"Verbindung zu {self.base_url} fehlgeschlagen. Server nicht erreichbar."}
        except requests.exceptions.Timeout:
            return {"ok": False, "message": "Timeout - Server antwortet nicht rechtzeitig."}
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                return {"ok": False, "message": "Authentifizierung fehlgeschlagen. Token ungültig."}
            if e.response is not None and e.response.status_code == 403:
                return {"ok": False, "message": "Zugriff verweigert. Token hat keine Berechtigung."}
            return {"ok": False, "message": f"HTTP-Fehler: {e}"}
        except Exception as e:
            return {"ok": False, "message": f"Fehler: {e}"}

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
        try:
            resp = self.session.post(
                f"{self.base_url}/api/{resource}/",
                json=data,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            detail = ""
            if e.response is not None:
                try:
                    detail = str(e.response.json())
                except Exception:
                    detail = e.response.text[:300]
            raise RuntimeError(f"Erstellen fehlgeschlagen: {e} - {detail}")

    def update(self, resource: str, resource_id: int, data: dict) -> dict | None:
        try:
            resp = self.session.patch(
                f"{self.base_url}/api/{resource}/{resource_id}/",
                json=data,
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    def download_document(self, doc_id: int, filepath: str) -> bool:
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
        except Exception:
            return False

    def upload_document(self, filepath: str, metadata: dict) -> str | None:
        try:
            files = {"document": open(filepath, "rb")}
            form_data = {}
            for key in ("title", "created", "correspondent", "document_type",
                        "storage_path", "archive_serial_number"):
                if metadata.get(key):
                    form_data[key] = str(metadata[key])

            resp = self.session.post(
                f"{self.base_url}/api/documents/post_document/",
                files=files,
                data=form_data,
                timeout=120,
            )
            files["document"].close()
            resp.raise_for_status()
            return resp.text.strip().strip('"')
        except Exception:
            return None

    def wait_for_task(self, task_id: str) -> int | None:
        start = time.time()
        while time.time() - start < DOCUMENT_POLL_TIMEOUT:
            try:
                resp = self.session.get(
                    f"{self.base_url}/api/tasks/?task_id={task_id}",
                    timeout=30,
                )
                resp.raise_for_status()
                tasks = resp.json()
                task_list = tasks if isinstance(tasks, list) else tasks.get("results", [])
                if task_list:
                    task = task_list[0]
                    status = task.get("status", "")
                    if status == "SUCCESS":
                        related_doc = task.get("related_document")
                        if isinstance(related_doc, int):
                            return related_doc
                        if isinstance(related_doc, str) and related_doc.isdigit():
                            return int(related_doc)
                        result = task.get("result", "")
                        if isinstance(result, str):
                            for word in result.split():
                                if word.isdigit():
                                    return int(word)
                        return None
                    elif status == "FAILURE":
                        return None
            except Exception:
                pass
            time.sleep(DOCUMENT_POLL_INTERVAL)
        return None

    def get_document_notes(self, doc_id: int) -> list:
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
        try:
            resp = self.session.post(
                f"{self.base_url}/api/documents/{doc_id}/notes/",
                json={"note": note},
                timeout=30,
            )
            resp.raise_for_status()
            return True
        except Exception:
            return False

    def search_documents(self, title: str) -> list:
        try:
            resp = self.session.get(
                f"{self.base_url}/api/documents/?title__icontains={requests.utils.quote(title)}&page_size=10",
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json().get("results", [])
        except Exception:
            return []


def clean_data(data: dict) -> dict:
    return {k: v for k, v in data.items() if k not in SKIP_FIELDS}


def remap_references(data: dict, resource: str, id_mapping: dict) -> dict:
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
    RULE_TYPE_MAP = {
        3: "correspondents", 4: "document_types",
        6: "tags", 17: "tags",
        24: "storage_paths", 26: "custom_fields",
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
                rule["value"] = str(ref_map.get(int(value), int(value)))
        remapped.append(rule)
    return remapped


class MigrationRunner:
    """Führt die Migration durch und sendet Fortschritt-Events."""

    def __init__(
        self,
        source_url: str,
        source_token: str,
        target_url: str,
        target_token: str,
        migrate_options: dict,
        progress_queue: queue.Queue,
        cancel_event: threading.Event,
    ):
        self.source = PaperlessAPI(source_url, source_token)
        self.target = PaperlessAPI(target_url, target_token)
        self.options = migrate_options
        self.queue = progress_queue
        self.cancel = cancel_event
        self.id_mapping = {}
        self.doc_mapping = {}
        self.summary = {}

    def emit(self, event_type: str, **kwargs):
        """Sendet ein Event an die Queue."""
        self.queue.put({"type": event_type, **kwargs})

    def is_cancelled(self) -> bool:
        return self.cancel.is_set()

    def run(self):
        """Hauptmethode - führt die gesamte Migration durch."""
        try:
            self.emit("start", message="Migration gestartet")

            # Phase 1: Einstellungen
            self.emit("phase", message="Phase 1: Einstellungen migrieren", phase=1)

            for resource in MIGRATION_ORDER:
                if self.is_cancelled():
                    self.emit("cancelled", message="Migration abgebrochen")
                    return

                if not self.options.get(resource, False):
                    continue

                label = RESOURCE_LABELS.get(resource, resource)
                self.emit("resource_start", resource=resource, label=label)
                mapping = self._migrate_resource(resource)
                self.id_mapping[resource] = mapping
                self.emit("resource_done", resource=resource, label=label,
                          count=len(mapping))

            # Phase 2: Dokumente
            if self.options.get("documents", False):
                if self.is_cancelled():
                    self.emit("cancelled", message="Migration abgebrochen")
                    return

                self.emit("phase", message="Phase 2: Dokumente migrieren", phase=2)
                self.doc_mapping = self._migrate_documents()

                # Phase 3: Notizen
                if self.options.get("document_notes", False) and self.doc_mapping:
                    if self.is_cancelled():
                        self.emit("cancelled", message="Migration abgebrochen")
                        return

                    self.emit("phase", message="Phase 3: Dokument-Notizen migrieren", phase=3)
                    self._migrate_document_notes()

            # Zusammenfassung
            self.emit("complete", message="Migration abgeschlossen!", summary=self.summary)

        except Exception as e:
            self.emit("error", message=f"Unerwarteter Fehler: {e}")
            self.emit("complete", message="Migration mit Fehlern beendet", summary=self.summary)

    def _migrate_resource(self, resource: str) -> dict:
        mapping = {}
        label = RESOURCE_LABELS.get(resource, resource)

        try:
            source_items = self.source.get_all(resource)
        except Exception as e:
            self.emit("item", status="error",
                      message=f"Konnte {label} nicht laden: {e}")
            return mapping

        try:
            target_items = self.target.get_all(resource)
        except Exception:
            target_items = []

        target_names = {item.get("name"): item for item in target_items}

        self.emit("item", status="info",
                  message=f"{label}: {len(source_items)} auf Source, {len(target_items)} auf Target")

        created = 0
        skipped = 0
        failed = 0

        for item in source_items:
            if self.is_cancelled():
                return mapping

            name = item.get("name", "???")

            if name in target_names:
                mapping[item["id"]] = target_names[name]["id"]
                skipped += 1
                continue

            data = clean_data(item)
            data = remap_references(data, resource, self.id_mapping)

            try:
                result = self.target.create(resource, data)
                if result:
                    mapping[item["id"]] = result["id"]
                    self.emit("item", status="ok",
                              message=f"'{name}' erstellt")
                    created += 1
                else:
                    failed += 1
            except Exception as e:
                self.emit("item", status="error",
                          message=f"'{name}': {e}")
                failed += 1

        self.summary[resource] = {
            "created": created, "skipped": skipped, "failed": failed
        }
        self.emit("item", status="info",
                  message=f"{label}: {created} erstellt, {skipped} übersprungen, {failed} fehlgeschlagen")
        return mapping

    def _migrate_documents(self) -> dict:
        doc_mapping = {}
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)

        try:
            source_docs = self.source.get_all("documents")
        except Exception as e:
            self.emit("item", status="error", message=f"Dokumentenliste laden fehlgeschlagen: {e}")
            return doc_mapping

        try:
            target_docs = self.target.get_all("documents")
        except Exception:
            target_docs = []

        target_doc_index = {}
        for doc in target_docs:
            key = f"{doc.get('title', '')}_{doc.get('created', '')}"
            target_doc_index[key] = doc

        total = len(source_docs)
        self.emit("item", status="info",
                  message=f"Dokumente: {total} auf Source, {len(target_docs)} auf Target")

        created = 0
        skipped = 0
        failed = 0

        for i, doc in enumerate(source_docs, 1):
            if self.is_cancelled():
                break

            title = doc.get("title", "Unbenannt")
            doc_id = doc["id"]
            created_date = doc.get("created", "")

            self.emit("document_progress", current=i, total=total,
                      message=f"[{i}/{total}] '{title}'")

            # Duplikat-Check
            dup_key = f"{title}_{created_date}"
            if dup_key in target_doc_index:
                doc_mapping[doc_id] = target_doc_index[dup_key]["id"]
                skipped += 1
                continue

            # Download
            original_name = doc.get("original_file_name", f"doc_{doc_id}.pdf")
            ext = Path(original_name).suffix or ".pdf"
            download_path = os.path.join(DOWNLOAD_DIR, f"{doc_id}{ext}")

            if not self.source.download_document(doc_id, download_path):
                self.emit("item", status="error", message=f"Download fehlgeschlagen: '{title}'")
                failed += 1
                continue

            # Metadaten vorbereiten
            metadata = {"title": title, "created": created_date}

            if doc.get("correspondent"):
                new_id = self.id_mapping.get("correspondents", {}).get(doc["correspondent"])
                if new_id:
                    metadata["correspondent"] = new_id

            if doc.get("document_type"):
                new_id = self.id_mapping.get("document_types", {}).get(doc["document_type"])
                if new_id:
                    metadata["document_type"] = new_id

            if doc.get("storage_path"):
                new_id = self.id_mapping.get("storage_paths", {}).get(doc["storage_path"])
                if new_id:
                    metadata["storage_path"] = new_id

            if doc.get("archive_serial_number"):
                metadata["archive_serial_number"] = doc["archive_serial_number"]

            # Upload
            task_id = self.target.upload_document(download_path, metadata)

            # Temp-Datei aufräumen
            try:
                os.remove(download_path)
            except OSError:
                pass

            if not task_id:
                self.emit("item", status="error", message=f"Upload fehlgeschlagen: '{title}'")
                failed += 1
                continue

            # Auf Verarbeitung warten
            new_doc_id = self.target.wait_for_task(task_id)

            if not new_doc_id:
                found = self.target.search_documents(title)
                if found:
                    new_doc_id = found[0]["id"]

            if new_doc_id:
                doc_mapping[doc_id] = new_doc_id

                # Tags zuweisen
                if doc.get("tags"):
                    tag_map = self.id_mapping.get("tags", {})
                    new_tags = [tag_map[t] for t in doc["tags"] if t in tag_map]
                    if new_tags:
                        self.target.update("documents", new_doc_id, {"tags": new_tags})

                # Custom Fields zuweisen
                if doc.get("custom_fields"):
                    cf_map = self.id_mapping.get("custom_fields", {})
                    new_cfs = []
                    for cf in doc["custom_fields"]:
                        new_field_id = cf_map.get(cf.get("field"))
                        if new_field_id:
                            new_cfs.append({"field": new_field_id, "value": cf.get("value")})
                    if new_cfs:
                        self.target.update("documents", new_doc_id, {"custom_fields": new_cfs})

                self.emit("item", status="ok", message=f"'{title}' migriert")
                created += 1
            else:
                self.emit("item", status="error",
                          message=f"'{title}' hochgeladen, aber ID nicht ermittelbar")
                failed += 1

        # Aufräumen
        try:
            if os.path.exists(DOWNLOAD_DIR) and not os.listdir(DOWNLOAD_DIR):
                os.rmdir(DOWNLOAD_DIR)
        except OSError:
            pass

        self.summary["documents"] = {
            "created": created, "skipped": skipped, "failed": failed
        }
        self.emit("item", status="info",
                  message=f"Dokumente: {created} erstellt, {skipped} übersprungen, {failed} fehlgeschlagen")
        return doc_mapping

    def _migrate_document_notes(self):
        if not self.doc_mapping:
            return

        total_notes = 0
        for old_id, new_id in self.doc_mapping.items():
            if self.is_cancelled():
                break

            notes = self.source.get_document_notes(old_id)
            if not notes:
                continue

            existing_notes = self.target.get_document_notes(new_id)
            existing_texts = {n.get("note", "") for n in existing_notes}

            for note in notes:
                note_text = note.get("note", "")
                if not note_text or note_text in existing_texts:
                    continue
                if self.target.add_document_note(new_id, note_text):
                    total_notes += 1

        self.summary["document_notes"] = {"created": total_notes, "skipped": 0, "failed": 0}
        self.emit("item", status="info", message=f"Notizen: {total_notes} migriert")
