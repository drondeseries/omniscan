using System;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Plugins;
using MediaBrowser.Model.Serialization;

namespace OmniscanPlugin;

/// <summary>
/// Omniscan Jellyfin plugin entry point.
/// Registers the POST /Library/ScanPath and POST /Library/ScanPaths endpoints
/// so Omniscan can trigger instant, targeted item creation without a full scan.
/// </summary>
public class Plugin : BasePlugin<PluginConfiguration>
{
    /// <summary>The unique plugin GUID.</summary>
    public static readonly Guid PluginId = new("f47ac10b-58cc-4372-a567-0e02b2c3d479");

    /// <summary>
    /// Initializes a new instance of the <see cref="Plugin"/> class.
    /// </summary>
    public Plugin(IApplicationPaths applicationPaths, IXmlSerializer xmlSerializer)
        : base(applicationPaths, xmlSerializer)
    {
        Instance = this;
    }

    /// <summary>Gets the singleton plugin instance.</summary>
    public static Plugin? Instance { get; private set; }

    /// <inheritdoc/>
    public override string Name => "OmniscanPlugin";

    /// <inheritdoc/>
    public override Guid Id => PluginId;

    /// <inheritdoc/>
    public override string Description =>
        "Exposes targeted scan endpoints (POST /Library/ScanPath, POST /Library/ScanPaths) " +
        "for use with the Omniscan media monitor.";
}
