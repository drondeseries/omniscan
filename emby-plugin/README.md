# Omniscan Emby Plugin

An Emby plugin that exposes targeted scan endpoints so **Omniscan** can instantly
create new library items without triggering a full library scan.

## Endpoints

| Method | URL | Description |
|--------|-----|-------------|
| `POST` | `/Library/ScanPath` | Scan a single file or folder path |
| `POST` | `/Library/ScanPaths` | Scan multiple paths in one batch request |

---

## How it works

The plugin implements a **walk-up ancestor** algorithm:

1. Check if the path already exists in the Emby database.
2. If not, walk up the directory tree until a known parent folder is found.
3. Resolve and create the missing items top-down using Emby's internal `ResolvePath` + `CreateItem` APIs.
4. Trigger a lightweight metadata refresh on the new item.

---

## Installation

### Building the Plugin DLL

To build this plugin, you will need to copy the following core assemblies from your Emby Server installation into a directory named `sdk/` inside this directory:
- `MediaBrowser.Common.dll`
- `MediaBrowser.Controller.dll`
- `MediaBrowser.Model.dll`

Once copied, compile the project using:

```bash
cd emby-plugin
dotnet build --configuration Release
# DLL will be located at: bin/Release/netstandard2.0/OmniscanEmbyPlugin.dll
```

Copy the compiled `OmniscanEmbyPlugin.dll` to your Emby `plugins` directory and restart Emby.
