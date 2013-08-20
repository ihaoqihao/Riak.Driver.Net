using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;

namespace Riak.Driver
{
    /// <summary>
    /// riak client
    /// </summary>
    public sealed class RiakClient : PooledSocketClient<RiakResponse>
    {
        private RiakServerPool _serverPool = null;

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
        protected override IServerPool InitServerPool()
        {
            return this._serverPool = new RiakServerPool(this);
        }
        /// <summary>
        /// OnSendCallback
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="e"></param>
        protected override void OnSendCallback(IConnection connection, SendCallbackEventArgs e)
        {
            base.OnSendCallback(connection, e);

            if (e.Status == SendCallbackStatus.Success)
            {
                //try send next request.
                var request = base.DequeueFromPendingQueue();
                if (request == null) { this._serverPool.Release(connection); return; }
                this.Send(request);
                return;
            }
            this._serverPool.Release(connection);
        }
    }
}