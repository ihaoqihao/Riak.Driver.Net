using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;
using Sodao.FastSocket.SocketBase.Utils;
using System.Threading;

namespace Riak.Driver
{
    /// <summary>
    /// riak server pool
    /// </summary>
    public sealed class RiakServerPool : Sodao.FastSocket.Client.IServerPool
    {
        #region Private Members
        private readonly IHost _host = null;

        private readonly Dictionary<string, EndPoint> _dicServers = new Dictionary<string, EndPoint>();
        private List<Tuple<string, EndPoint>> _listServers = null;
        private readonly List<SocketConnector> _listConnectors = new List<SocketConnector>();

        private volatile int _connectionCount = 0;
        /// <summary>
        /// key:server name + guid
        /// </summary>
        private readonly Dictionary<string, IConnection> _dicConnections = new Dictionary<string, IConnection>();
        private readonly InterlockedStack<IConnection> _connPool = new InterlockedStack<IConnection>();
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        /// <param name="host"></param>
        /// <exception cref="ArgumentNullException">host is null.</exception>
        public RiakServerPool(IHost host)
        {
            if (host == null) throw new ArgumentNullException("host");
            this._host = host;
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
        /// <summary>
        /// acquire
        /// </summary>
        /// <returns></returns>
        public IConnection Acquire()
        {
            IConnection connection;
            if (this._connPool.TryPop(out connection)) return connection;

            if (this._connectionCount > 30) return null;

            lock (this)
            {
                if (this._dicServers.Count == 0) return null;
            }
        }
        /// <summary>
        /// get all node names
        /// </summary>
        /// <returns></returns>
        public string[] GetAllNodeNames()
        {
            lock (this) return this._dicServers.Keys.ToArray();
        }
        /// <summary>
        /// register node
        /// </summary>
        /// <param name="name"></param>
        /// <param name="endPoint"></param>
        /// <returns></returns>
        public bool TryRegisterNode(string name, EndPoint endPoint)
        {
            lock (this)
            {
                if (this._dicServers.ContainsKey(name)) return false;
                this._dicServers[name] = endPoint;
                return true;
            }
        }
        /// <summary>
        /// unregister node
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool UnRegisterNode(string name)
        {
            lock (this)
            {
                if (!this._dicServers.ContainsKey(name)) return false;
                this._dicServers.Remove(name);
                return true;
            }
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
            this._connPool.Push(connection);
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
            this._connPool.Push(connection);
        }
        #endregion
    }
}