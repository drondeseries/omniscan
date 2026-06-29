using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.IO;
using MediaBrowser.Model.Logging;

namespace OmniscanEmbyPlugin.Services
{
    public class TargetedScanService
    {
        private static readonly ConcurrentDictionary<string, SemaphoreSlim> _parentLocks =
            new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);

        private readonly ILibraryManager _libraryManager;
        private readonly IProviderManager _providerManager;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger _logger;

        public TargetedScanService(
            ILibraryManager libraryManager,
            IProviderManager providerManager,
            IFileSystem fileSystem,
            ILogger logger)
        {
            _libraryManager = libraryManager;
            _providerManager = providerManager;
            _fileSystem = fileSystem;
            _logger = logger;
        }

        public ScanPathResult ScanPath(string path, Dictionary<string, BaseItem> cache = null)
        {
            _logger.Info("OmniscanEmbyPlugin: scanning path {0}", path);

            // 1. Verify path exists on filesystem
            if (!Directory.Exists(path) && !File.Exists(path))
            {
                // Check if there's a stale database entry to clean up (file deleted scenario)
                var staleItem = _libraryManager.FindByPath(path, null);
                if (staleItem != null)
                {
                    _logger.Info(
                        "OmniscanEmbyPlugin: removing stale item {0} ({1}) — file no longer exists: {2}",
                        staleItem.Name, staleItem.InternalId, path);
                    var parent = staleItem.GetParent();
                    _libraryManager.DeleteItem(staleItem, new DeleteOptions
                    {
                        DeleteFileLocation = false,
                        DeleteFromExternalProvider = false
                    }, parent, false);
                    if (cache != null)
                    {
                        cache.Remove(path);
                    }
                    return new ScanPathResult
                    {
                        Status = ScanStatus.Removed,
                        ItemId = staleItem.InternalId.ToString(),
                        ItemName = staleItem.Name,
                        Path = path
                    };
                }

                _logger.Warn("OmniscanEmbyPlugin: path does not exist on filesystem: {0}", path);
                return new ScanPathResult { Status = ScanStatus.PathNotFound, Path = path };
            }

            // 2. Check if item already exists in library
            var existing = _libraryManager.FindByPath(path, null);
            if (existing != null)
            {
                if (cache != null)
                {
                    cache[path] = existing;
                }

                _logger.Info("OmniscanEmbyPlugin: item already exists in library: {0}", path);
                _providerManager.QueueRefresh(
                    existing.InternalId,
                    new MetadataRefreshOptions(new DirectoryService(_logger, _fileSystem))
                    {
                        MetadataRefreshMode = MetadataRefreshMode.ValidationOnly,
                        ReplaceAllMetadata = false
                    },
                    RefreshPriority.High);

                return new ScanPathResult
                {
                    Status = ScanStatus.Refreshed,
                    ItemId = existing.InternalId.ToString(),
                    ItemName = existing.Name,
                    Path = path
                };
            }

            // 3. Walk up the directory tree to find the nearest known ancestor
            var (missingPaths, knownAncestor) = WalkUpToAncestor(path, cache);
            if (knownAncestor == null)
            {
                _logger.Warn("OmniscanEmbyPlugin: path is not under any known library folder: {0}", path);
                return new ScanPathResult { Status = ScanStatus.ParentNotFound, Path = path };
            }

            // 4. Create missing items from ancestor down to leaf
            var sem = _parentLocks.GetOrAdd(knownAncestor.Path, _ => new SemaphoreSlim(1, 1));
            sem.Wait();
            try
            {
                return CreateItems(path, missingPaths, knownAncestor, cache);
            }
            finally
            {
                sem.Release();
            }
        }

        public List<ScanPathResult> ScanPaths(IEnumerable<string> paths)
        {
            var unique = paths
                .Where(p => !string.IsNullOrWhiteSpace(p))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(p => p.Count(c => c == Path.DirectorySeparatorChar || c == Path.AltDirectorySeparatorChar))
                .ToList();

            _logger.Info("OmniscanEmbyPlugin: batch scanning {0} paths", unique.Count);

            var pathCache = new Dictionary<string, BaseItem>(StringComparer.OrdinalIgnoreCase);
            var results = new List<ScanPathResult>();

            foreach (var path in unique)
            {
                var result = ScanPath(path, pathCache);
                result.Path = path;
                results.Add(result);
            }

            return results;
        }

        private (List<string> MissingPaths, Folder KnownAncestor) WalkUpToAncestor(string path, Dictionary<string, BaseItem> cache = null)
        {
            var missingPaths = new List<string> { path };
            Folder knownAncestor = null;
            var current = Path.GetDirectoryName(path);

            while (!string.IsNullOrEmpty(current))
            {
                BaseItem found;
                if (cache != null && cache.TryGetValue(current, out var cached))
                {
                    found = cached;
                }
                else
                {
                    found = _libraryManager.FindByPath(current, null);
                    if (cache != null && found != null)
                    {
                        cache[current] = found;
                    }
                }

                if (found is Folder folder)
                {
                    // Skip plain Folder items that are generic movie subfolders
                    if (found.GetType() == typeof(Folder))
                    {
                        var parent = folder.GetParent();
                        if (parent != null && parent.GetType() != typeof(Folder))
                        {
                            knownAncestor = folder;
                            break;
                        }

                        missingPaths.Add(current);
                        current = Path.GetDirectoryName(current);
                        continue;
                    }

                    knownAncestor = folder;
                    break;
                }

                missingPaths.Add(current);
                current = Path.GetDirectoryName(current);
            }

            return (missingPaths, knownAncestor);
        }

        private ScanPathResult CreateItems(string path, List<string> missingPaths, Folder knownAncestor, Dictionary<string, BaseItem> cache = null)
        {
            var existing = _libraryManager.FindByPath(path, null);
            if (existing != null)
            {
                if (cache != null)
                {
                    cache[path] = existing;
                }

                _logger.Info("OmniscanEmbyPlugin: item created concurrently ({0})", existing.InternalId);
                return new ScanPathResult
                {
                    Status = ScanStatus.Refreshed,
                    ItemId = existing.InternalId.ToString(),
                    ItemName = existing.Name,
                    Path = path
                };
            }

            missingPaths.Reverse();

            Folder currentParent = knownAncestor;
            BaseItem lastCreated = null;
            foreach (var missingPath in missingPaths)
            {
                var alreadyExists = _libraryManager.FindByPath(missingPath, null);
                if (alreadyExists != null)
                {
                    if (cache != null)
                    {
                        cache[missingPath] = alreadyExists;
                    }

                    if (alreadyExists is Folder existingFolder)
                    {
                        currentParent = existingFolder;
                        lastCreated = alreadyExists;
                        continue;
                    }
                    else
                    {
                        lastCreated = alreadyExists;
                        break;
                    }
                }

                var fileInfo = _fileSystem.GetFileSystemInfo(missingPath);
                var newItem = _libraryManager.ResolvePath(fileInfo, currentParent);

                if (newItem == null)
                {
                    _logger.Warn("OmniscanEmbyPlugin: ResolvePath returned null for: {0}", missingPath);
                    return new ScanPathResult { Status = ScanStatus.Failed, Path = path };
                }

                _libraryManager.CreateItem(newItem, currentParent);

                _providerManager.QueueRefresh(
                    newItem.InternalId,
                    new MetadataRefreshOptions(new DirectoryService(_logger, _fileSystem))
                    {
                        MetadataRefreshMode = MetadataRefreshMode.ValidationOnly,
                        ReplaceAllMetadata = false
                    },
                    RefreshPriority.High);

                if (cache != null)
                {
                    cache[missingPath] = newItem;
                }

                lastCreated = newItem;

                if (newItem is Folder folder)
                {
                    currentParent = folder;
                }
                else
                {
                    break;
                }
            }

            if (lastCreated == null)
            {
                return new ScanPathResult { Status = ScanStatus.Failed, Path = path };
            }

            return new ScanPathResult
            {
                Status = ScanStatus.Created,
                ItemId = lastCreated.InternalId.ToString(),
                ItemName = lastCreated.Name,
                Path = path
            };
        }
    }

    public class ScanPathResult
    {
        public ScanStatus Status { get; set; }
        public string ItemId { get; set; }
        public string ItemName { get; set; }
        public string Path { get; set; }
    }

    public enum ScanStatus
    {
        Created,
        Refreshed,
        PathNotFound,
        ParentNotFound,
        Failed,
        Removed
    }
}
