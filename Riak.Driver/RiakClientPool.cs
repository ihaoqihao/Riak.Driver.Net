using System;
using System.Collections.Concurrent;

namespace Riak.Driver
{
    /// <summary>
    /// riak client pool
    /// </summary>
    public sealed class RiakClientPool
    {
        /// <summary>
        /// key:string.Concat(configFile, endpointName)
        /// </summary>
        static private readonly ConcurrentDictionary<string, Lazy<RiakClient>> _dic =
            new ConcurrentDictionary<string, Lazy<RiakClient>>();

        /// <summary>
        /// get <see cref="RedisClient"/>
        /// </summary>
        /// <param name="endpointName"></param>
        /// <returns></returns>
        static public RiakClient Get(string endpointName)
        {
            return Get(null, endpointName);
        }
        /// <summary>
        /// get <see cref="RedisClient"/>
        /// </summary>
        /// <param name="configFile"></param>
        /// <param name="endpointName"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">endpointName is null or empty.</exception>
        static public RiakClient Get(string configFile, string endpointName)
        {
            if (string.IsNullOrEmpty(endpointName)) throw new ArgumentNullException("endpointName");
            if (configFile == null) configFile = string.Empty;

            return _dic.GetOrAdd(string.Concat(configFile, endpointName),
                key => new Lazy<RiakClient>(() => RiakClientFactory.Create(configFile, endpointName), true)).Value;
        }
    }
}