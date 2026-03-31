# Paperless-ngx Migration Tool

Docker-Webanwendung zum Migrieren aller Einstellungen und Dokumente zwischen zwei Paperless-ngx Instanzen.

## Was wird migriert?

- **Einstellungen:** Tags, Korrespondenten, Dokumenttypen, Speicherpfade, Custom Fields, Gespeicherte Ansichten, Mail-Konten, Mail-Regeln, Workflows
- **Dokumente:** Alle Original-Dateien (PDFs etc.) inkl. Metadaten, Tags, Custom Field Werte
- **Notizen:** Kommentare auf Dokumenten

## Schnellstart

```bash
docker compose up -d --build
```

Dann im Browser: **http://localhost:5000**

## Benutzung

1. **Quell-Instanz** konfigurieren (URL + API Token der alten Paperless-Instanz)
2. **Ziel-Instanz** konfigurieren (URL + API Token der neuen Paperless-Instanz)
3. **Verbindung testen** - zeigt Statistiken beider Instanzen
4. **Optionen wählen** - was soll migriert werden
5. **Migration starten** - Live-Fortschritt im Browser

### API Token finden

In Paperless-ngx: **Profil** (oben rechts) → **My Profile** → **API Token** (Kreispfeil-Button)

## Hinweise

- Bereits vorhandene Einträge (gleicher Name) werden übersprungen
- ID-Referenzen werden automatisch umgeschrieben (z.B. Tags in Saved Views)
- Dokument-Migration kann bei vielen/großen Dokumenten länger dauern
- Falls die Paperless-Instanzen aus dem Container nicht erreichbar sind, `network_mode: host` in `docker-compose.yml` einkommentieren
