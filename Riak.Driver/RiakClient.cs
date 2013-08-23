using System;
using System.Text;
using System.Threading.Tasks;
using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;

namespace Riak.Driver
{
    /// <summary>
    /// riak client
    /// </summary>
    public sealed class RiakClient : PooledSocketClient<RiakResponse>
    {
        #region Private Members
        private RiakServerPool _serverPool = null;
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        public RiakClient()
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
        public RiakClient(int socketBufferSize,
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

        #region Private Methods
        /// <summary>
        /// execute
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="cmdName"></param>
        /// <param name="payload"></param>
        /// <param name="funResult"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        private Task<TResult> Execute<TResult>(string cmdName, byte[] payload, Func<RiakResponse, TResult> funResult, object asyncState = null)
        {
            var source = new TaskCompletionSource<TResult>();

            Request<RiakResponse> request = null;
            request = new Request<RiakResponse>(base.NextRequestSeqID(), cmdName, payload, ex =>
            {
                var rex = ex as RequestException;
                if (rex != null && rex.Error == RequestException.Errors.ReceiveTimeout)
                {
                    var connection = request.Tag as IConnection;
                    if (connection != null) connection.BeginDisconnect();
                }
                source.TrySetException(ex);
            },
            response =>
            {
                TResult result;
                try { result = funResult(response); }
                catch (Exception ex) { source.TrySetException(ex); return; }
                source.TrySetResult(result);
            });

            this.Send(request);
            return source.Task;
        }
        #endregion

        /// <summary>
        /// put
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task Put(string bucket, string key, string value, object asyncState = null)
        {
            var req = new Messages.RpbPutReq
            {
                key = Encoding.UTF8.GetBytes(key),
                bucket = Encoding.UTF8.GetBytes(bucket),
                content = new Messages.RpbContent { value = Encoding.UTF8.GetBytes(value) }
            };

            byte[] bytes = null;
            using (var ms = new System.IO.MemoryStream())
            {
                ms.Position = 5;
                ProtoBuf.Serializer.Serialize(ms, req);
                bytes = ms.ToArray();
            }
            bytes[4] = Messages.Codes.RpbPutReq;
            Buffer.BlockCopy(Sodao.FastSocket.SocketBase.Utils.NetworkBitConverter.GetBytes(bytes.Length - 4), 0, bytes, 0, 4);

            return Execute<bool>("put", bytes, c => true, asyncState);
        }
    }
}