using System;
using System.IO;
using System.Text;
using System.Threading;
using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;
using Sodao.FastSocket.SocketBase.Utils;

namespace Riak.Driver
{
    /// <summary>
    /// riak socket client
    /// </summary>
    public sealed class RiakSocketClient : PooledSocketClient<RiakResponse>
    {
        #region Private Members
        private RiakServerPool _serverPool = null;
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        public RiakSocketClient()
            : this(8192, 8192, 3000, 3000)
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="socketBufferSize"></param>
        /// <param name="messageBufferSize"></param>
        /// <param name="millisecondsSendTimeout"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public RiakSocketClient(int socketBufferSize,
            int messageBufferSize,
            int millisecondsSendTimeout,
            int millisecondsReceiveTimeout)
            : base(new RiakPbProtocol(), socketBufferSize, messageBufferSize, millisecondsSendTimeout, millisecondsReceiveTimeout)
        {
        }
        #endregion

        #region Override Methods
        /// <summary>
        /// init server pool
        /// </summary>
        /// <returns></returns>
        protected override IServerPool InitServerPool()
        {
            return this._serverPool = new RiakServerPool(this);
        }
        /// <summary>
        /// OnServerPoolServerAvailable
        /// </summary>
        /// <param name="name"></param>
        /// <param name="connection"></param>
        protected override void OnServerPoolServerAvailable(string name, IConnection connection)
        {
            var request = base.DequeueFromPendingQueue();
            if (request == null) return;
            base.Send(request);
        }
        /// <summary>
        /// OnStartSending
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="packet"></param>
        protected override void OnStartSending(IConnection connection, Packet packet)
        {
            connection.UserData = packet;
            packet.Tag = connection;
            base.OnStartSending(connection, packet);
        }
        /// <summary>
        /// OnResponse
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="response"></param>
        protected override void OnResponse(IConnection connection, RiakResponse response)
        {
            var packet = connection.UserData as Packet;
            if (packet != null) packet.Tag = null;
            connection.UserData = null;

            //try send next request from pending queue.
            ThreadPool.QueueUserWorkItem(_ =>
            {
                var request = base.DequeueFromPendingQueue();
                if (request == null) { this._serverPool.Release(connection); return; };
                connection.BeginSend(request);
            });
        }
        #endregion

        #region Public Properties
        /// <summary>
        /// get or set max pool size
        /// </summary>
        public int MaxPoolSize
        {
            get { return this._serverPool.MaxPoolSize; }
            set { this._serverPool.MaxPoolSize = value; }
        }
        #endregion

        #region Execute
        /// <summary>
        /// Serialize
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="request"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">request is null</exception>
        private byte[] Serialize<T>(byte reqCode, T request)
        {
            if (request == null) throw new ArgumentNullException("request");

            byte[] bytes = null;
            using (var ms = new MemoryStream())
            {
                ms.Position = 5;
                ProtoBuf.Serializer.Serialize(ms, request);
                bytes = ms.ToArray();
            }
            bytes[4] = reqCode;
            Buffer.BlockCopy(NetworkBitConverter.GetBytes(bytes.Length - 4), 0, bytes, 0, 4);

            return bytes;
        }
        /// <summary>
        /// Deserialize
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="response"></param>
        /// <returns></returns>
        private T Deserialize<T>(RiakResponse response)
        {
            if (response.Payload == null || response.Payload.Length == 0) return default(T);
            using (var ms = new MemoryStream(response.Payload)) return ProtoBuf.Serializer.Deserialize<T>(ms);
        }
        /// <summary>
        /// Execute
        /// </summary>
        /// <param name="cmdName"></param>
        /// <param name="bytes"></param>
        /// <param name="respCode"></param>
        /// <param name="onError"></param>
        /// <param name="onResponse"></param>
        /// <exception cref="ArgumentNullException">bytes is null</exception>
        /// <exception cref="ArgumentNullException">onError is null</exception>
        /// <exception cref="ArgumentNullException">onResponse is null</exception>
        private void Execute(string cmdName, byte[] bytes, byte respCode, Action<Exception> onError, Action<RiakResponse> onResponse)
        {
            if (bytes == null) throw new ArgumentNullException("bytes");
            if (onError == null) throw new ArgumentNullException("onError");
            if (onResponse == null) throw new ArgumentNullException("onResponse");

            Request<RiakResponse> socketRequest = null;
            this.Send(socketRequest = new Request<RiakResponse>(base.NextRequestSeqID(), cmdName, bytes, ex =>
            {
                var rex = ex as RequestException;
                //当receive timeout时，强制断开当前链接
                if (rex != null && rex.Error == RequestException.Errors.ReceiveTimeout)
                {
                    var connection = socketRequest.Tag as IConnection;
                    if (connection != null) connection.BeginDisconnect();
                }
                onError(ex);
            },
            response =>
            {
                if (response.MessageCode == Messages.Codes.ErrorResp)
                {
                    var error = this.Deserialize<Messages.RpbErrorResp>(response);
                    onError(new RiakException(error.errcode, Encoding.UTF8.GetString(error.errmsg)));
                    return;
                }
                if (response.MessageCode != respCode)
                {
                    onError(new RiakException(string.Concat("invalid response, expected is ", respCode.ToString(), ", but was is ", response.MessageCode.ToString())));
                    return;
                }
                onResponse(response);
            }));
        }
        /// <summary>
        /// Execute
        /// </summary>
        /// <typeparam name="TRequest"></typeparam>
        /// <typeparam name="TResponse"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="request"></param>
        /// <param name="onError"></param>
        /// <param name="callback"></param>
        public void Execute<TRequest, TResponse>(byte reqCode, byte respCode, TRequest request, Action<Exception> onError, Action<TResponse> callback)
        {
            this.Execute(reqCode.ToString(), this.Serialize(reqCode, request), respCode, onError, response => callback(this.Deserialize<TResponse>(response)));
        }
        /// <summary>
        /// Execute
        /// </summary>
        /// <typeparam name="TRequest"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="request"></param>
        /// <param name="onError"></param>
        /// <param name="callback"></param>
        public void Execute<TRequest>(byte reqCode, byte respCode, TRequest request, Action<Exception> onError, Action callback)
        {
            this.Execute(reqCode.ToString(), this.Serialize(reqCode, request), respCode, onError, response => callback());
        }
        /// <summary>
        /// Execute
        /// </summary>
        /// <typeparam name="TResponse"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="onError"></param>
        /// <param name="callback"></param>
        public void Execute<TResponse>(byte reqCode, byte respCode, Action<Exception> onError, Action<TResponse> callback)
        {
            this.Execute(reqCode.ToString(), new byte[] { 0, 0, 0, 1, reqCode }, respCode, onError, response => callback(this.Deserialize<TResponse>(response)));
        }
        /// <summary>
        /// Execute
        /// </summary>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="onError"></param>
        /// <param name="callback"></param>
        public void Execute(byte reqCode, byte respCode, Action<Exception> onError, Action callback)
        {
            this.Execute(reqCode.ToString(), new byte[] { 0, 0, 0, 1, reqCode }, respCode, onError, response => callback());
        }
        #endregion
    }
}