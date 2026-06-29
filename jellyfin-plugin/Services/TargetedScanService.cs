using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Data.Enums;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.IO;
using Microsoft.Extensions.Logging;

namespace OmniscanPlugin.Services;

/// <summary>
/// Core logic for Omniscan targeted library scanning.
///
/// Algorithm (mirrors the targeted-scans plugin behaviour):
///   1.  Walk the filesystem path upward until we find the nearest item that
///       Jellyfin already knows about (the "known ancestor").
///   2.  From that ancestor downward, call ResolvePath + AddVirtualItem on every
///       intermediate directory and finally the leaf (file or folder) itself.
///   3.  For each newly created item, trigger a quick metadata refresh.
///
/// This creates Series → Season → Episode (or Movie) hierarchies in one shot,
/// without triggering a full ValidateChildren scan on the library root.
/// </summary>
public class TargetedScanService
{
    // Per-parent-path semaphores prevent simultaneous creation of the same
    // intermediate folder from two concurrent requests.
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> _parentLocks =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly ILibraryManager _libraryManager;
    private readonly IProviderManager _providerManager;
    private readonly IFileSystem _fileSystem;
    private readonly ILogger<TargetedScanService> _logger;

    /// <summary>Initializes a new instance of <see cref="TargetedScanService"/>.</summary>
    public TargetedScanService(
        ILibraryManager libraryManager,
        IProviderManager providerManager,
        IFileSystem fileSystem,
        ILogger<TargetedScanService> logger)
    {
        _libraryManager = libraryManager;
        _providerManager = providerManager;
        _fileSystem = fileSystem;
        _logger = logger;
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /// <summary>
    /// Scan a single filesystem path (file or directory), creating library items
    /// for any paths that are not yet known to Jellyfin.
    /// </summary>
    /// <param name="path">Absolute filesystem path to scan.</param>
    /// <param name="cache">Optional FindByPath result cache shared across a batch call.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="ScanPathResult"/> describing the outcome.</returns>
    public async Task<ScanPathResult> ScanPathAsync(
        string path,
        Dictionary<string, BaseItem?>? cache = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return ScanPathResult.Error(path, "Path is empty.");
        }

        // --- 1. Check if item already exists in library ---
        var existing = _libraryManager.FindByPath(path, null);
        if (existing is not null)
        {
            _logger.LogDebug("OmniscanPlugin: already in library: {Path}", path);
            // Still refresh metadata so any changes (e.g. updated NFO) are picked up.
            await RefreshItemAsync(existing, cancellationToken).ConfigureAwait(false);
            return ScanPathResult.Existing(existing);
        }

        // --- 2. Walk upward to find nearest known ancestor ---
        var (missingPaths, knownAncestor) = WalkUpToAncestor(path, cache);

        if (knownAncestor is null)
        {
            _logger.LogWarning("OmniscanPlugin: no known ancestor found for: {Path}", path);
            return ScanPathResult.Error(path, "Path is not under any known library location.");
        }

        _logger.LogInformation(
            "OmniscanPlugin: known ancestor '{Ancestor}' for path '{Path}', creating {Count} missing items.",
            knownAncestor.Path, path, missingPaths.Count);

        // --- 3. Create missing items from ancestor downward ---
        // missingPaths is built from leaf → ancestor, so reverse it.
        missingPaths.Reverse();

        BaseItem? lastCreated = null;
        foreach (var missingPath in missingPaths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            lastCreated = await CreateItemAsync(missingPath, knownAncestor, cancellationToken)
                .ConfigureAwait(false)
                ?? lastCreated;
        }

        if (lastCreated is null)
        {
            return ScanPathResult.Error(path, "Item could not be created.");
        }

        return ScanPathResult.Created(lastCreated);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Walk the directory tree upward from <paramref name="path"/> until we find
    /// an item that Jellyfin already knows about.
    /// </summary>
    private (List<string> MissingPaths, Folder? KnownAncestor) WalkUpToAncestor(
        string path,
        Dictionary<string, BaseItem?>? cache)
    {
        var missingPaths = new List<string> { path };
        Folder? knownAncestor = null;

        var current = Path.GetDirectoryName(path);
        while (!string.IsNullOrEmpty(current))
        {
            BaseItem? found;
            if (cache is not null && cache.TryGetValue(current, out var cached))
            {
                found = cached;
            }
            else
            {
                found = _libraryManager.FindByPath(current, null);
                cache?.TryAdd(current, found);
            }

            if (found is Folder folder)
            {
                // Skip plain Folder items that are generic movie sub-folders
                // (e.g. /media/Movies/Inception (2010)/).  These would cause a
                // video file to be resolved as a generic Video instead of Movie.
                // We keep walking until we hit a properly-typed container
                // (CollectionFolder, Series, Season, BoxSet, …) or the library root.
                if (found.GetType() == typeof(Folder))
                {
                    var parent = folder.GetParent();
                    // If the folder's parent is *not* a plain Folder, this folder IS
                    // a library-root location — safe to stop here.
                    if (parent is not null && parent.GetType() != typeof(Folder))
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

    /// <summary>
    /// Create (or retrieve if already present) the library item for
    /// <paramref name="path"/> under <paramref name="parent"/>.
    /// Uses a per-parent semaphore to avoid duplicate creation races.
    /// </summary>
    private async Task<BaseItem?> CreateItemAsync(
        string path,
        Folder parent,
        CancellationToken cancellationToken)
    {
        // Check once more — another concurrent request may have just created it.
        var existing = _libraryManager.FindByPath(path, null);
        if (existing is not null)
        {
            _logger.LogDebug("OmniscanPlugin: item already exists (race): {Path}", path);
            return existing;
        }

        var sem = _parentLocks.GetOrAdd(parent.Path, _ => new SemaphoreSlim(1, 1));
        await sem.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Double-check after acquiring lock.
            existing = _libraryManager.FindByPath(path, null);
            if (existing is not null)
            {
                _logger.LogDebug("OmniscanPlugin: item already exists (post-lock): {Path}", path);
                return existing;
            }

            bool isFolder = Directory.Exists(path);
            var fileSystemInfo = isFolder
                ? _fileSystem.GetDirectoryInfo(path)
                : _fileSystem.GetFileInfo(path);

            if (!fileSystemInfo.Exists)
            {
                _logger.LogWarning("OmniscanPlugin: path does not exist on disk: {Path}", path);
                return null;
            }

            // ResolvePath asks Jellyfin to determine the correct item type
            // (Movie, Series, Season, Episode, …) based on the library's
            // naming rules and content type.
            var resolvedItem = _libraryManager.ResolvePath(fileSystemInfo, parent);
            if (resolvedItem is null)
            {
                _logger.LogWarning("OmniscanPlugin: ResolvePath returned null for: {Path}", path);
                return null;
            }

            // Persist the item to the database.
            await _libraryManager.CreateItem(resolvedItem, parent, cancellationToken)
                .ConfigureAwait(false);

            _logger.LogInformation(
                "OmniscanPlugin: created {Type} '{Name}' at {Path}",
                resolvedItem.GetType().Name, resolvedItem.Name, path);

            // Run a non-blocking metadata refresh so artwork & info appear quickly.
            _ = RefreshItemAsync(resolvedItem, CancellationToken.None);

            return resolvedItem;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "OmniscanPlugin: failed to create item for: {Path}", path);
            return null;
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>
    /// Trigger a light metadata refresh on an existing item.
    /// Uses ValidationOnly so it's fast and doesn't re-fetch remote metadata.
    /// </summary>
    private async Task RefreshItemAsync(BaseItem item, CancellationToken cancellationToken)
    {
        try
        {
            await item.RefreshMetadata(
                new MetadataRefreshOptions(new DirectoryService(_fileSystem))
                {
                    MetadataRefreshMode = MetadataRefreshMode.ValidationOnly,
                    ImageRefreshMode    = MetadataRefreshMode.ValidationOnly,
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "OmniscanPlugin: metadata refresh failed for: {Path}", item.Path);
        }
    }
}

// -------------------------------------------------------------------------
// Result types
// -------------------------------------------------------------------------

/// <summary>Result returned by <see cref="TargetedScanService.ScanPathAsync"/>.</summary>
public sealed class ScanPathResult
{
    private ScanPathResult() { }

    /// <summary>Jellyfin item ID (empty string if not found/created yet).</summary>
    public string ItemId   { get; private set; } = string.Empty;

    /// <summary>Human-readable item name.</summary>
    public string ItemName { get; private set; } = string.Empty;

    /// <summary>One of: <c>Created</c>, <c>Existing</c>, <c>Error</c>.</summary>
    public string Status   { get; private set; } = string.Empty;

    /// <summary>The path that was scanned.</summary>
    public string Path     { get; private set; } = string.Empty;

    /// <summary>Additional human-readable detail.</summary>
    public string Message  { get; private set; } = string.Empty;

    internal static ScanPathResult Created(BaseItem item) => new()
    {
        ItemId   = item.Id.ToString("N"),
        ItemName = item.Name,
        Status   = "Created",
        Path     = item.Path,
        Message  = $"{item.GetType().Name} '{item.Name}' created successfully.",
    };

    internal static ScanPathResult Existing(BaseItem item) => new()
    {
        ItemId   = item.Id.ToString("N"),
        ItemName = item.Name,
        Status   = "Existing",
        Path     = item.Path,
        Message  = $"{item.GetType().Name} '{item.Name}' already in library.",
    };

    internal static ScanPathResult Error(string path, string message) => new()
    {
        Status  = "Error",
        Path    = path,
        Message = message,
    };
}
