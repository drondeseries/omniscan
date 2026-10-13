using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
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

    // Bounded concurrent refreshes for newly created items (best-effort:
    // semaphore limits overlap without blocking the creation path).
    private static readonly SemaphoreSlim _refreshLimiter = new(3, 3);

    private readonly ILibraryManager _libraryManager;
    private readonly IProviderManager _providerManager;
    private readonly IFileSystem _fileSystem;
    private readonly ILogger<TargetedScanService> _logger;

    private static readonly ConcurrentDictionary<Type, MethodInfo> _resolvePathMethods = new();

    private static MethodInfo? FindResolvePathMethod(Type type)
    {
        var allMethods = type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Concat(type.GetInterfaces().SelectMany(i => i.GetMethods()));

        return allMethods
            .Where(m => (m.Name == "ResolvePath" || m.Name.EndsWith(".ResolvePath", StringComparison.Ordinal)) &&
                        (m.ReturnType.Name == "BaseItem" || typeof(BaseItem).IsAssignableFrom(m.ReturnType)))
            .OrderByDescending(m => m.GetParameters().Length)
            .FirstOrDefault(m =>
            {
                var p = m.GetParameters();
                return p.Length >= 2 &&
                       (p[0].ParameterType.Name == "FileSystemMetadata" || p[0].ParameterType.IsAssignableFrom(typeof(FileSystemMetadata))) &&
                       (p[1].ParameterType.Name == "Folder" || p[1].ParameterType.IsAssignableFrom(typeof(Folder)));
            });
    }

    /// <summary>
    /// Invokes <c>ILibraryManager.ResolvePath</c> using runtime reflection.
    /// Works across all Jellyfin versions:
    /// - Jellyfin 10.8-10.11: ResolvePath(FileSystemMetadata, Folder, IDirectoryService) [3 parameters]
    /// - Jellyfin 12.0+: ResolvePath(FileSystemMetadata, Folder, IDirectoryService, CollectionType?) [4 parameters]
    /// - Fallback: ResolvePath(FileSystemMetadata, Folder) [2 parameters]
    /// </summary>
    private BaseItem? ResolvePath(FileSystemMetadata fileSystemInfo, Folder parent)
    {
        var targetType = _libraryManager.GetType();
        var method = _resolvePathMethods.GetOrAdd(targetType, t =>
            FindResolvePathMethod(t) ??
            FindResolvePathMethod(typeof(ILibraryManager)) ??
            throw new MissingMethodException(nameof(ILibraryManager), "ResolvePath"));

        var parameters = method.GetParameters();
        var args = new object?[parameters.Length];
        args[0] = fileSystemInfo;
        args[1] = parent;
        for (int i = 2; i < parameters.Length; i++)
        {
            if (typeof(IDirectoryService).IsAssignableFrom(parameters[i].ParameterType) ||
                parameters[i].ParameterType.Name == "IDirectoryService")
            {
                args[i] = new DirectoryService(_fileSystem);
            }
            else if (parameters[i].HasDefaultValue)
            {
                args[i] = parameters[i].DefaultValue;
            }
            else
            {
                args[i] = null;
            }
        }

        try
        {
            return (BaseItem?)method.Invoke(_libraryManager, args);
        }
        catch (TargetInvocationException tie) when (tie.InnerException is not null)
        {
            throw tie.InnerException;
        }
    }

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
            await RefreshItemAsync(existing, fullRefresh: false, cancellationToken).ConfigureAwait(false);
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
        var currentParent = knownAncestor;
        for (int i = 0; i < missingPaths.Count; i++)
        {
            var missingPath = missingPaths[i];
            cancellationToken.ThrowIfCancellationRequested();
            var created = await CreateItemAsync(
                missingPath,
                currentParent,
                cancellationToken)
                .ConfigureAwait(false);

            if (created is null)
            {
                _logger.LogWarning("OmniscanPlugin: failed to create item at '{Path}'. Aborting descent.", missingPath);
                return ScanPathResult.Error(missingPath, $"Item could not be created for path: {missingPath}");
            }

            lastCreated = created;
            if (lastCreated is Folder folder)
            {
                currentParent = folder;
            }
            else
            {
                // Reached a leaf item (Movie, Episode, Video, etc.).
                // If there are unprocessed child paths remaining, this non-Folder item is only valid
                // if it directly represents the requested target video (e.g. a Movie container resolved for
                // the immediate parent movie directory where the target path is that Movie's primary media file or part).
                bool isTargetOrContainer =
                    string.Equals(missingPath, path, StringComparison.OrdinalIgnoreCase) ||
                    (lastCreated is MediaBrowser.Controller.Entities.Movies.Movie movie &&
                     string.Equals(Path.GetDirectoryName(path), missingPath, StringComparison.OrdinalIgnoreCase) &&
                     (string.Equals(movie.Path, path, StringComparison.OrdinalIgnoreCase) ||
                      (movie.AdditionalParts is not null && movie.AdditionalParts.Any(p => string.Equals(p, path, StringComparison.OrdinalIgnoreCase)))));

                if (!isTargetOrContainer && i < missingPaths.Count - 1)
                {
                    _logger.LogWarning(
                        "OmniscanPlugin: created non-Folder item '{Type}' at '{ItemPath}', but target '{Target}' has remaining child paths. Aborting.",
                        lastCreated.GetType().Name, missingPath, path);
                    return ScanPathResult.Error(missingPath, "Resolved intermediate item is not a folder container.");
                }
                break;
            }
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
            var resolvedItem = ResolvePath(fileSystemInfo, parent);
            if (resolvedItem is null)
            {
                _logger.LogWarning("OmniscanPlugin: ResolvePath returned null for: {Path}", path);
                return null;
            }

            // Persist the item to the database.
            _libraryManager.CreateItem(resolvedItem, parent);


            _logger.LogInformation(
                "OmniscanPlugin: created {Type} '{Name}' at {Path}",
                resolvedItem.GetType().Name, resolvedItem.Name, path);

            /// Run a bounded non-blocking metadata refresh so artwork & info appear quickly.
            // For newly created items, use FullRefresh so remote metadata and image providers are queried.
            // ReplaceAllMetadata=true mirrors the manual "Replace all metadata" action:
            // without it, FullRefresh preserves existing values such as the
            // filename-derived title (issue #8 follow-up).
            // Bounded to max 3 concurrent refreshes via _refreshLimiter, and
            // the request cancellationToken is passed through so shutdown
            // cancels the pending refresh (OperationCanceledException skips).
            _ = Task.Run(async () =>
            {
                bool acquired = false;
                try
                {
                    // Best-effort bound: skip the refresh if 3 are already running.
                    acquired = await _refreshLimiter.WaitAsync(0, cancellationToken).ConfigureAwait(false);
                    if (!acquired)
                    {
                        _logger.LogDebug("OmniscanPlugin: refresh skipped (limiter full) for: {Path}", path);
                        return;
                    }
                    try
                    {
                        // Brief delay so provider identification (which runs async
                        // after creation) has a chance to attach ProviderIds before
                        // the refresh merges remote metadata.
                        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false);
                        await RefreshItemAsync(resolvedItem, fullRefresh: true, cancellationToken: cancellationToken, replaceAll: true).ConfigureAwait(false);
                    }
                    finally
                    {
                        _refreshLimiter.Release();
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("OmniscanPlugin: refresh cancelled for: {Path}", path);
                }
            });

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
    /// Trigger a metadata refresh on an item.
    /// When fullRefresh is true (e.g. newly created items), FullRefresh is used so remote metadata and image providers are queried.
    /// When fullRefresh is false (e.g. existing items), ValidationOnly is used for lightweight fast validation.
    /// When replaceAll is true, remote providers overwrite existing values (mirrors
    /// Jellyfin's manual "Replace all metadata" action) instead of preserving them.
    /// </summary>
    private async Task RefreshItemAsync(BaseItem item, bool fullRefresh, CancellationToken cancellationToken, bool replaceAll = false)
    {
        try
        {
            var mode = fullRefresh ? MetadataRefreshMode.FullRefresh : MetadataRefreshMode.ValidationOnly;
            await item.RefreshMetadata(
                new MetadataRefreshOptions(new DirectoryService(_fileSystem))
                {
                    MetadataRefreshMode = mode,
                    ImageRefreshMode    = mode,
                    ReplaceAllMetadata  = replaceAll,
                    ReplaceAllImages    = false,
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "OmniscanPlugin: metadata refresh failed for: {Path}", item.Path);
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
