using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net.Mime;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.Providers;
using MediaBrowser.Model.IO;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using OmniscanPlugin.Services;

namespace OmniscanPlugin.Controllers;

// ---------------------------------------------------------------------------
// Request / Response models
// ---------------------------------------------------------------------------

/// <summary>Request body for <c>POST /Library/ScanPath</c>.</summary>
public sealed class ScanPathRequest
{
    /// <summary>Absolute filesystem path to scan (file or directory).</summary>
    [Required]
    public string Path { get; set; } = string.Empty;
}

/// <summary>Request body for <c>POST /Library/ScanPaths</c>.</summary>
public sealed class ScanPathsRequest
{
    /// <summary>List of absolute filesystem paths to scan.</summary>
    [Required]
    public List<string> Paths { get; set; } = new();
}

/// <summary>Per-path result within a batch response.</summary>
public sealed class ScanPathsResponseItem
{
    /// <summary>Jellyfin item ID (empty if error).</summary>
    public string ItemId   { get; set; } = string.Empty;

    /// <summary>Human-readable name.</summary>
    public string ItemName { get; set; } = string.Empty;

    /// <summary>One of: <c>Created</c>, <c>Existing</c>, <c>Error</c>.</summary>
    public string Status   { get; set; } = string.Empty;

    /// <summary>The path that was processed.</summary>
    public string Path     { get; set; } = string.Empty;

    /// <summary>Additional detail message.</summary>
    public string Message  { get; set; } = string.Empty;
}

/// <summary>Response body for <c>POST /Library/ScanPaths</c>.</summary>
public sealed class ScanPathsResponse
{
    /// <summary>Per-path results.</summary>
    public List<ScanPathsResponseItem> Results { get; set; } = new();
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

/// <summary>
/// ASP.NET Core controller that exposes Omniscan's targeted scan endpoints.
///
/// <list type="bullet">
///   <item><c>POST /Library/ScanPath</c>  — scan a single path.</item>
///   <item><c>POST /Library/ScanPaths</c> — scan multiple paths in one request.</item>
/// </list>
///
/// Both endpoints require a valid Jellyfin API token (<c>Authorize</c>).
/// </summary>
[ApiController]
[Route("Library")]
[Authorize]
public class ScanPathController : ControllerBase
{
    private readonly TargetedScanService _scanService;
    private readonly ILogger<ScanPathController> _logger;

    /// <summary>
    /// Initializes a new instance of <see cref="ScanPathController"/>.
    /// All parameters are injected by Jellyfin's DI container.
    /// </summary>
    public ScanPathController(
        ILibraryManager libraryManager,
        IProviderManager providerManager,
        IFileSystem fileSystem,
        ILogger<ScanPathController> logger,
        ILogger<TargetedScanService> scanServiceLogger)
    {
        _scanService = new TargetedScanService(libraryManager, providerManager, fileSystem, scanServiceLogger);
        _logger = logger;
    }

    // -----------------------------------------------------------------------

    /// <summary>
    /// Scan a single filesystem path and create the library item if missing.
    /// </summary>
    /// <param name="request">JSON body containing the path.</param>
    /// <param name="cancellationToken">Request cancellation token.</param>
    /// <returns>A JSON object with <c>ItemId</c>, <c>Status</c>, etc.</returns>
    /// <response code="200">Item found, created, or error details.</response>
    /// <response code="400">Request body is missing or malformed.</response>
    /// <response code="401">Invalid or missing API token.</response>
    [HttpPost("ScanPath")]
    [Produces(MediaTypeNames.Application.Json)]
    [ProducesResponseType(typeof(ScanPathResult), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    public async Task<ActionResult<ScanPathResult>> ScanPath(
        [FromBody] ScanPathRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Path))
        {
            return BadRequest("Path must not be empty.");
        }

        _logger.LogInformation("OmniscanPlugin: ScanPath request for: {Path}", request.Path);
        var result = await _scanService.ScanPathAsync(request.Path, null, cancellationToken)
            .ConfigureAwait(false);

        return Ok(result);
    }

    // -----------------------------------------------------------------------

    /// <summary>
    /// Scan multiple filesystem paths in a single batch request.
    /// Paths are processed concurrently; results are returned in the same order.
    /// </summary>
    /// <param name="request">JSON body containing a list of paths.</param>
    /// <param name="cancellationToken">Request cancellation token.</param>
    /// <returns>A JSON object with a <c>Results</c> array.</returns>
    /// <response code="200">Batch results for every path.</response>
    /// <response code="400">Request body is missing or <c>Paths</c> is empty.</response>
    /// <response code="401">Invalid or missing API token.</response>
    [HttpPost("ScanPaths")]
    [Produces(MediaTypeNames.Application.Json)]
    [ProducesResponseType(typeof(ScanPathsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    public async Task<ActionResult<ScanPathsResponse>> ScanPaths(
        [FromBody] ScanPathsRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Paths is null || request.Paths.Count == 0)
        {
            return BadRequest("Paths must not be empty.");
        }

        _logger.LogInformation(
            "OmniscanPlugin: ScanPaths batch request for {Count} path(s).", request.Paths.Count);

        // Shared FindByPath cache so repeated ancestor lookups hit the cache.
        var cache = new Dictionary<string, MediaBrowser.Controller.Entities.BaseItem?>(
            System.StringComparer.OrdinalIgnoreCase);

        var tasks = request.Paths
            .Select(p => _scanService.ScanPathAsync(p, cache, cancellationToken));

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);

        var response = new ScanPathsResponse
        {
            Results = results.Select(r => new ScanPathsResponseItem
            {
                ItemId   = r.ItemId,
                ItemName = r.ItemName,
                Status   = r.Status,
                Path     = r.Path,
                Message  = r.Message,
            }).ToList(),
        };

        return Ok(response);
    }
}
