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
        /// OnStartSending
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="packet"></param>
        protected override void OnStartSending(IConnection connection, Packet packet)
        {
            connection.UserData = packet;
            base.OnStartSending(connection, packet);
        }
        /// <summary>
        /// OnResponse
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="response"></param>
        protected override void OnResponse(IConnection connection, RiakResponse response)
        {
            var request = base.DequeueFromPendingQueue();
            if (request == null) { this._serverPool.Release(connection); return; };
            connection.BeginSend(request);
        }
        #endregion

        public Task Put(string bucket, string key, string value)
        {
            var source = new TaskCompletionSource<bool>();

            var req = new Messages.RpbPutReq
            {
                key = Encoding.UTF8.GetBytes(key),
                bucket = Encoding.UTF8.GetBytes(bucket),
                content = new Messages.RpbContent
                {
                    value = Encoding.UTF8.GetBytes(value)
                }
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

            this.Send(new Request<RiakResponse>(base.NextRequestSeqID(), "put", bytes,
                ex => source.TrySetException(ex),
                (response) =>
                {
                    source.TrySetResult(true);
                }));
            return source.Task;
        }
    }
}