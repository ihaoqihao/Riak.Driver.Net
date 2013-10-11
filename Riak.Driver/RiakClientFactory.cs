using System;
using System.Configuration;
using System.IO;
using System.Net;

namespace Riak.Driver
{
    /// <summary>
    /// client factory
    /// </summary>
    public sealed class RiakClientFactory
    {
        /// <summary>
        /// create <see cref="RiakClient"/>
        /// </summary>
        /// <param name="configFile"></param>
        /// <param name="endpointName"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">endpointName is null or empty.</exception>
        static public RiakClient Create(string configFile, string endpointName)
        {
            if (string.IsNullOrEmpty(endpointName)) throw new ArgumentNullException("endpointName");

            Config.RiakConfigSection config = null;
            if (string.IsNullOrEmpty(configFile)) config = ConfigurationManager.GetSection("riak") as Config.RiakConfigSection;
            else
            {
                config = ConfigurationManager.OpenMappedExeConfiguration(
                    new ExeConfigurationFileMap { ExeConfigFilename = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, configFile) },
                    ConfigurationUserLevel.None).GetSection("riak") as Config.RiakConfigSection;
            }

            var clientConfig = config.Clients.Get(endpointName);
            var riakSocket = new RiakSocketClient(clientConfig.SocketBufferSize,
                clientConfig.MessageBufferSize,
                clientConfig.MillisecondsSendTimeout,
                clientConfig.MillisecondsReceiveTimeout);
            riakSocket.MaxPoolSize = clientConfig.MaxPoolSize;
            //register server.
            foreach (Config.ServerConfig server in clientConfig.Servers)
                riakSocket.RegisterServerNode(string.Concat(server.Host, server.Port), new IPEndPoint(IPAddress.Parse(server.Host), server.Port));

            return new RiakClient(riakSocket);
        }
    }
}