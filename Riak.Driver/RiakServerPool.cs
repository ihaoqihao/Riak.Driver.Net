using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;
using Sodao.FastSocket.SocketBase.Utils;

namespace Riak.Driver
{
    /// <summary>
    /// riak server pool
    /// </summary>
    public sealed class RiakServerPool : Sodao.FastSocket.Client.IServerPool
    {
        #region Private Members
        private readonly SocketConnector[] _nodes = null;
        private readonly InterlockedStack<IConnection> _connections = new InterlockedStack<IConnection>();
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        /// <param name="servers"></param>
        /// <exception cref="ArgumentNullException">servers is null.</exception>
        public RiakServerPool(IHost host, IEnumerable<EndPoint> servers)
        {
            if (servers == null) throw new ArgumentNullException("servers");

            this._nodes = servers.Select(c => new SocketConnector(c.ToString(), c, host, this.OnConnected, this.OnDisconnected)).ToArray();
            for (int i = 0, l = this._nodes.Length; i < l; i++) this._nodes[i].Start();
        }
        #endregion

        #region IServerPool Members
        /// <summary>
        /// connected event
        /// </summary>
        public event Action<string, IConnection> Connected;
        /// <summary>
        /// server available event
        /// </summary>
        public event Action ServerAvailable;

        /// <summary>
        /// acquire by hash
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public IConnection Acquire(byte[] hash)
        {
            throw new NotImplementedException();
        }
        /// acquire
        public IConnection Acquire()
        {
            IConnection connection;
            if (this._connections.TryPop(out connection)) return connection;
            return null;
        }
        /// <summary>
        /// get all node names
        /// </summary>
        /// <returns></returns>
        public string[] GetAllNodeNames()
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// register node
        /// </summary>
        /// <param name="name"></param>
        /// <param name="endPoint"></param>
        /// <returns></returns>
        public bool TryRegisterNode(string name, System.Net.EndPoint endPoint)
        {
            throw new NotImplementedException();
        }
        /// <summary>
        /// unregister node
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool UnRegisterNode(string name)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Private Methods
        /// <summary>
        /// OnConnected
        /// </summary>
        /// <param name="node"></param>
        /// <param name="connection"></param>
        private void OnConnected(SocketConnector node, IConnection connection)
        {
            this._connections.Push(connection);
        }
        /// <summary>
        /// OnDisconnected
        /// </summary>
        /// <param name="node"></param>
        /// <param name="connection"></param>
        private void OnDisconnected(SocketConnector node, IConnection connection)
        {
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// release connection
        /// </summary>
        /// <param name="connection"></param>
        public void Release(IConnection connection)
        {
            if (connection == null || !connection.Active) return;
            this._connections.Push(connection);
        }
        #endregion
    }
}