using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;

namespace Riak.Driver
{
    /// <summary>
    /// riak server pool
    /// </summary>
    public sealed class RiakServerPool : Sodao.FastSocket.Client.IServerPool
    {
        #region Private Members
        private int _maxPoolSize = 30;
        private readonly IHost _host = null;

        private readonly Dictionary<string, EndPoint> _dicNodes = new Dictionary<string, EndPoint>();
        private Tuple<string, EndPoint>[] _nodes = null;

        private int _connectedCount = 0;
        private readonly List<Tuple<string, IConnection>> _listConnections = new List<Tuple<string, IConnection>>();
        private readonly ConcurrentStack<IConnection> _connectionPool = new ConcurrentStack<IConnection>();
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
        public event Action<string, IConnection> ServerAvailable;

        /// <summary>
        /// acquire by hash
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        public IConnection Acquire(byte[] hash)
        {
            return this.Acquire();
        }
        /// <summary>
        /// acquire
        /// </summary>
        /// <returns></returns>
        public IConnection Acquire()
        {
            IConnection connection;
            if (this._connectionPool.TryPop(out connection)) return connection;
            if (Thread.VolatileRead(ref this._connectedCount) >= this._maxPoolSize) return null;

            Tuple<string, EndPoint> node = null;
            lock (this)
            {
                if (Thread.VolatileRead(ref this._connectedCount) >= this._maxPoolSize) return null;
                if (this._nodes == null || this._nodes.Length == 0) return null;

                Interlocked.Increment(ref this._connectedCount);
                node = this._nodes[new Random().Next(this._nodes.Length)];
            }

            this.BeginConnect(node.Item1, node.Item2);
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
                this._nodes = this._dicNodes.Select(c => new Tuple<string, EndPoint>(c.Key, c.Value)).ToArray();
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
            bool flag;
            Tuple<string, IConnection>[] arr = null;

            lock (this)
            {
                //remove node.
                if (flag = this._dicNodes.ContainsKey(name))
                {
                    this._dicNodes.Remove(name);
                    this._nodes = this._dicNodes.Select(c => new Tuple<string, EndPoint>(c.Key, c.Value)).ToArray();
                }
                //remove connections
                arr = this._listConnections.Where(c => c.Item1 == name).ToArray();
                for (int i = 0, l = arr.Length; i < l; i++) this._listConnections.Remove(arr[i]);
            }

            //disconnect conection
            for (int i = 0, l = arr.Length; i < l; i++) arr[i].Item2.BeginDisconnect();
            return flag;
        }
        #endregion

        #region Private Methods
        /// <summary>
        /// begin connect
        /// </summary>
        /// <param name="name"></param>
        /// <param name="endPoint"></param>
        private void BeginConnect(string name, EndPoint endPoint)
        {
            SocketConnector.BeginConnect(endPoint, this._host, connection =>
            {
                //connect failed.
                if (connection == null) { Interlocked.Decrement(ref this._connectedCount); return; }

                //handle dissconnected
                connection.Disconnected += (conn, ex) =>
                {
                    lock (this)
                    {
                        var hit = this._listConnections.Find(c => c.Item2 == conn);
                        if (hit == null) return;
                        Interlocked.Decrement(ref this._connectedCount);
                        this._listConnections.Remove(hit);
                    }
                };

                this.Connected(name, connection);

                bool isActive = false;
                lock (this)
                {
                    if (isActive = this._dicNodes.ContainsKey(name))
                        this._listConnections.Add(new Tuple<string, IConnection>(name, connection));
                }
                if (isActive)
                {
                    this._connectionPool.Push(connection);
                    this.ServerAvailable(name, connection);
                    return;
                }

                connection.BeginDisconnect();
            });
        }
        #endregion

        #region Public Properties
        /// <summary>
        /// get or set max pool size.
        /// </summary>
        public int MaxPoolSize
        {
            get { return this._maxPoolSize; }
            set { this._maxPoolSize = value; }
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