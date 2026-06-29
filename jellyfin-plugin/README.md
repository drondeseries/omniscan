# Omniscan Jellyfin Plugin

A Jellyfin plugin that exposes targeted scan endpoints so **Omniscan** can instantly
create new library items without triggering a full library scan.

## Endpoints

| Method | URL | Description |
|--------|-----|-------------|
| `POST` | `/Library/ScanPath` | Scan a single file or folder path |
| `POST` | `/Library/ScanPaths` | Scan multiple paths in one batch request |

### ScanPath — Request

```json
{ "Path": "/media/TV/Breaking Bad/Season 1/S01E01.mkv" }
```

### ScanPath — Response

```json
{
  "ItemId":   "abc123...",
  "ItemName": "Pilot",
  "Status":   "Created",
  "Path":     "/media/TV/Breaking Bad/Season 1/S01E01.mkv",
  "Message":  "Episode 'Pilot' created successfully."
}
```

Status values: `Created`, `Existing`, `Error`

### ScanPaths — Request

```json
{
  "Paths": [
    "/media/TV/Show/Season 1/S01E01.mkv",
    "/media/Movies/Inception (2010)/Inception.mkv"
  ]
}
```

### ScanPaths — Response

```json
{
  "Results": [
    { "ItemId": "...", "Status": "Created", "Path": "...", "Message": "..." },
    { "ItemId": "...", "Status": "Existing", "Path": "...", "Message": "..." }
  ]
}
```

---

## How it works

The plugin implements a **walk-up ancestor** algorithm:

1. Check if the path already exists in the Jellyfin database.
2. If not, walk up the directory tree until a known parent is found.
3. Create each missing intermediate item (Show → Season → Episode or Movie)
   using Jellyfin's internal `ResolvePath` + `CreateItem` APIs — the same
   path resolution logic the full library scanner uses, but applied to just
   the one path.
4. Trigger a lightweight `ValidationOnly` metadata refresh on the new item.

This is far faster than a full library scan (`ValidateChildren`) and does not
disturb any existing library items.

---

## Installation

### Option A — Manual (recommended for now)

1. Build the DLL or download it from the latest GitHub Actions artifact.
2. Place `OmniscanPlugin.dll` in your Jellyfin plugin directory:
   - **Docker (linuxserver):** `/config/plugins/OmniscanPlugin/`
   - **Native Linux:** `~/.local/share/jellyfin/plugins/OmniscanPlugin/`
3. Restart Jellyfin.

### Option B — Build yourself

```bash
cd jellyfin-plugin
dotnet build --configuration Release
# DLL is at: bin/Release/net8.0/OmniscanPlugin.dll
```

Copy the DLL to the plugin directory and restart Jellyfin.

---

## Omniscan configuration

No extra configuration is needed in Omniscan — the scanner automatically
detects whether the plugin is installed by attempting `POST /Library/ScanPath`.
If the endpoint returns 404 (plugin not installed), it falls back gracefully to
the standard `POST /Library/Media/Updated` endpoint.

To verify the plugin is active, check the Omniscan logs for:

```
🔎 Targeted plugin scan successful for: /path/to/file (ItemId: abc123...)
```

---

## Emby

Emby uses a different (ServiceStack-based) plugin API.  
See `../emby-plugin/` for the Emby equivalent (coming soon, or use the
targeted-scans Emby plugin in the meantime).
