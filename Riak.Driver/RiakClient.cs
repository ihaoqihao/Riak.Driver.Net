using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using Sodao.FastSocket.Client;

namespace Riak.Driver
{
    /// <summary>
    /// riak client
    /// </summary>
    public sealed class RiakClient : PooledSocketClient<RiakResponse>
    {
        /// <summary>
        /// new
        /// </summary>
        /// <param name="socketBufferSize"></param>
        /// <param name="messageBufferSize"></param>
        /// <param name="millisecondsSendTimeout"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public RiakClient(int socketBufferSize,
            int messageBufferSize,
            int millisecondsSendTimeout,
            int millisecondsReceiveTimeout)
            : base(new RiakPbProtocol(), socketBufferSize, messageBufferSize, millisecondsSendTimeout, millisecondsReceiveTimeout)
        {
        }

        /// <summary>
        /// init server pool
        /// </summary>
        /// <returns></returns>
        protected override Sodao.FastSocket.Client.IServerPool InitServerPool()
        {
            return new RiakServerPool(this);
        }
    }
}