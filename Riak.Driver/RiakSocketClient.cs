using System;
using System.IO;
using System.Text;
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
        protected override void OnServerPoolServerAvailable()
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
            var request = base.DequeueFromPendingQueue();
            if (request == null) { this._serverPool.Release(connection); return; };
            connection.BeginSend(request);
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// execute
        /// </summary>
        /// <typeparam name="TRequest"></typeparam>
        /// <typeparam name="TResponse"></typeparam>
        /// <param name="messageCode"></param>
        /// <param name="request"></param>
        /// <param name="onError"></param>
        /// <param name="onResponse"></param>
        /// <exception cref="ArgumentNullException">request is null</exception>
        /// <exception cref="ArgumentNullException">onError is null</exception>
        /// <exception cref="ArgumentNullException">onResponse is null</exception>
        public void Execute<TRequest, TResponse>(byte messageCode, TRequest request,
            Action<Exception> onError,
            Action<TResponse> onResponse)
            where TResponse : class
            where TRequest : class
        {
            if (request == null) throw new ArgumentNullException("request");
            if (onError == null) throw new ArgumentNullException("onError");
            if (onResponse == null) throw new ArgumentNullException("onResponse");

            byte[] bytes = null;
            using (var ms = new MemoryStream())
            {
                ms.Position = 5;
                ProtoBuf.Serializer.Serialize(ms, request);
                bytes = ms.ToArray();
            }
            bytes[4] = messageCode;
            Buffer.BlockCopy(NetworkBitConverter.GetBytes(bytes.Length - 4), 0, bytes, 0, 4);

            Request<RiakResponse> socketRequest = null;
            socketRequest = new Request<RiakResponse>(base.NextRequestSeqID(), messageCode.ToString(), bytes, ex =>
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
                if (response.MessageCode == Messages.Codes.RpbErrorResp)
                {
                    Messages.RpbErrorResp error = null;
                    using (var ms = new MemoryStream(response.Payload))
                        error = ProtoBuf.Serializer.Deserialize<Messages.RpbErrorResp>(ms);

                    onError(new RiakException(error.errcode, Encoding.UTF8.GetString(error.errmsg)));
                    return;
                }

                TResponse riakResponse = null;
                using (var ms = new MemoryStream(response.Payload))
                    riakResponse = ProtoBuf.Serializer.Deserialize<TResponse>(ms);
                onResponse(riakResponse);
            });

            this.Send(socketRequest);
        }
        #endregion
    }
}