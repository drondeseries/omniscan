using System;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Plugins;
using MediaBrowser.Model.Serialization;

namespace OmniscanEmbyPlugin
{
    /// <summary>
    /// Omniscan Emby plugin entry point.
    /// Registers the targeted scan endpoints via ServiceStack routes.
    /// </summary>
    public class Plugin : BasePlugin<PluginConfiguration>
    {
        public static readonly Guid PluginId = new("d08b3e8c-8be9-411a-85b2-32a121bf9024");

        public Plugin(IApplicationPaths applicationPaths, IXmlSerializer xmlSerializer)
            : base(applicationPaths, xmlSerializer)
        {
            Instance = this;
        }

        public static Plugin Instance { get; private set; }

        public override string Name => "OmniscanEmbyPlugin";

        public override Guid Id => PluginId;

        public override string Description =>
            "Exposes targeted scan endpoints (POST /Library/ScanPath, POST /Library/ScanPaths) " +
            "for use with the Omniscan media monitor.";
    }
}
