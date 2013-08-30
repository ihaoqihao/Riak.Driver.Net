using System.Configuration;

namespace Riak.Driver.Config
{
    /// <summary>
    /// riak config section
    /// </summary>
    public class RiakConfigSection : ConfigurationSection
    {
        /// <summary>
        /// endpoint collection。
        /// </summary>
        [ConfigurationProperty("client", IsRequired = true)]
        public EndpointCollection Clients
        {
            get { return this["client"] as EndpointCollection; }
        }
    }
}