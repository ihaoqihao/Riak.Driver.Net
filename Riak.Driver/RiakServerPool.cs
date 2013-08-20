using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;
using System.Threading;
using System.Collections.Concurrent;

namespace Riak.Driver
{
    /// <summary>
    /// riak server pool
    /// </summary>
    public sealed class RiakServerPool : Sodao.FastSocket.Client.IServerPool
    {
        #region Private Members
        private readonly IHost _host = null;

        /// <summary>
        /// key:node name
        /// </summary>
        private readonly Dictionary<string, EndPoint> _dicNodes =
            new Dictionary<string, EndPoint>();
        /// <summary>
        /// node array
        /// </summary>
        private Tuple<string, EndPoint>[] _nodes = null;

        private volatile int _connectedCount = 0;
        /// <summary>
        /// key:node name + guid
        /// </summary>
        private readonly Dictionary<string, IConnection> _dicConnections =
            new Dictionary<string, IConnection>();
        /// <summary>
        /// key:node name + guid
        /// </summary>
        private readonly Dictionary<string, SocketConnector> _dicConnector =
            new Dictionary<string, SocketConnector>();
        /// <summary>
        /// connection stack pool.
        /// </summary>
        private readonly ConcurrentStack<IConnection> _connectionPool =
            new ConcurrentStack<IConnection>();
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
            if (this._connectionPool.TryPop(out connection)) return connection;

            if (this._connectedCount > 30) return null;

            SocketConnector connector = null;
            lock (this)
            {
                if (this._nodes.Length == 0) return null;

                var node = this._nodes[new Random().Next(this._nodes.Length)];
                connector = new SocketConnector(string.Concat(node.Item1, "_", Guid.NewGuid().ToString()), node.Item2, this._host,
                    this.OnConnected, this.OnDisconnected);
                this._dicConnector.Add(connector.Name, connector);
            }
            connector.Start();
            return null;
        }
        /// <summary>
        /// get all node names
        /// </summary>
        /// <returns></returns>
        public string[] GetAllNodeNames()
        {
            lock (this) return this._dicNodes.Keys.ToArray();
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
                if (this._dicNodes.ContainsKey(name)) return false;

                this._dicNodes[name] = endPoint;
                this._dicNodes.Select(c => new Tuple<string, EndPoint>(c.Key, c.Value)).ToArray();
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
                if (!this._dicNodes.ContainsKey(name)) return false;

                this._dicNodes.Remove(name);
                this._dicNodes.Select(c => new Tuple<string, EndPoint>(c.Key, c.Value)).ToArray();

                //stop connectors
                var keys = this._dicConnector.Keys.Where(c => c.StartsWith(name)).ToArray();
                for (int i = 0, l = keys.Length; i < l; i++)
                {
                    var connector = this._dicConnector[keys[i]];
                    this._dicConnections.Remove(keys[i]);
                    connector.Stop();
                }
                //disconnect
                keys = this._dicConnections.Keys.Where(c => c.StartsWith(name)).ToArray();
                for (int i = 0, l = keys.Length; i < l; i++)
                {
                    var connection = this._dicConnections[keys[i]];
                    this._dicConnections.Remove(keys[i]);
                    connection.BeginDisconnect();
                }

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
            lock (this)
            {

            }
            this._connectionPool.Push(connection);
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
        /// <exception cref="ArgumentNullException">connection is null.</exception>
        public void Release(IConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            if (connection.Active) this._connectionPool.Push(connection);
        }
        #endregion
    }
}