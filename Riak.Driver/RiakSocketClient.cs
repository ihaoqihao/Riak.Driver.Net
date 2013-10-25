using Sodao.FastSocket.Client;
using Sodao.FastSocket.SocketBase;
using Sodao.FastSocket.SocketBase.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

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
            if (request != null) base.Send(request);
        }
        /// <summary>
        /// OnStartSending
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="packet"></param>
        protected override void OnStartSending(IConnection connection, Packet packet)
        {
            connection.UserData = packet.Tag;
            base.OnStartSending(connection, packet);
        }
        /// <summary>
        /// OnResponse
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="response"></param>
        protected override void OnResponse(IConnection connection, RiakResponse response)
        {
            connection.UserData = null;

            //try send next request from pending queue.
            ThreadPool.QueueUserWorkItem(_ =>
            {
                var request = base.DequeueFromPendingQueue();
                if (request == null) { this._serverPool.Release(connection); return; };
                connection.BeginSend(request);
            });
        }
        /// <summary>
        /// on request receive timeout
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="request"></param>
        protected override void OnReceiveTimeout(IConnection connection, Request<RiakResponse> request)
        {
            connection.BeginDisconnect(new Exception(string.Concat("request.", request.CmdName, " receive time out.")));
            base.OnReceiveTimeout(connection, request);
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
        /// execute
        /// </summary>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="bytes"></param>
        /// <param name="onError"></param>
        /// <param name="onReceive"></param>
        /// <param name="onCompleted"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        private void Execute(byte reqCode, byte respCode, byte[] bytes,
            Action<Exception> onError,
            Func<ArraySegment<byte>, bool> onReceive,
            Action onCompleted,
            int millisecondsReceiveTimeout)
        {
            var seqId = base.NextRequestSeqID();
            var request = new Request<RiakResponse>(seqId, reqCode.ToString(), bytes, onError, response =>
            {
                if (response.Exception == null) { onCompleted(); return; }
                onError(response.Exception);
            });
            request.MillisecondsReceiveTimeout = millisecondsReceiveTimeout;
            request.Tag = new RiakResponse(seqId, respCode, onReceive);

            this.Send(request);
        }
        /// <summary>
        /// execute
        /// </summary>
        /// <typeparam name="TReq"></typeparam>
        /// <typeparam name="TResp"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="request"></param>
        /// <param name="onError"></param>
        /// <param name="onCallback"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public void Execute<TReq, TResp>(byte reqCode, byte respCode, TReq request, Action<Exception> onError, Action<TResp> onCallback,
            int millisecondsReceiveTimeout)
        {
            TResp result = default(TResp);
            this.Execute(reqCode, respCode, this.Serialize(reqCode, request), onError, arrSeg =>
            {
                using (var stream = new MemoryStream(arrSeg.Array, arrSeg.Offset, arrSeg.Count))
                    result = ProtoBuf.Serializer.Deserialize<TResp>(stream);
                return true;
            },
            () => onCallback(result), millisecondsReceiveTimeout);
        }
        /// <summary>
        /// execute
        /// </summary>
        /// <typeparam name="TReq"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="request"></param>
        /// <param name="onError"></param>
        /// <param name="onCallback"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public void Execute<TReq>(byte reqCode, byte respCode, TReq request, Action<Exception> onError, Action onCallback,
            int millisecondsReceiveTimeout)
        {
            this.Execute(reqCode, respCode, this.Serialize(reqCode, request), onError, arrSeg => true,
                () => onCallback(), millisecondsReceiveTimeout);
        }
        /// <summary>
        /// execute
        /// </summary>
        /// <typeparam name="TResp"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="onError"></param>
        /// <param name="onCallback"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public void Execute<TResp>(byte reqCode, byte respCode, Action<Exception> onError, Action<TResp> onCallback, int millisecondsReceiveTimeout)
        {
            TResp result = default(TResp);
            this.Execute(reqCode, respCode, new byte[] { 0, 0, 0, 1, reqCode }, onError, arrSeg =>
            {
                using (var stream = new MemoryStream(arrSeg.Array, arrSeg.Offset, arrSeg.Count))
                    result = ProtoBuf.Serializer.Deserialize<TResp>(stream);
                return true;
            },
            () => onCallback(result), millisecondsReceiveTimeout);
        }
        /// <summary>
        /// execute
        /// </summary>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="onError"></param>
        /// <param name="onCallback"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public void Execute(byte reqCode, byte respCode, Action<Exception> onError, Action onCallback, int millisecondsReceiveTimeout)
        {
            this.Execute(reqCode, respCode, new byte[] { 0, 0, 0, 1, reqCode }, onError, arrSeg => true,
                () => onCallback(), millisecondsReceiveTimeout);
        }
        /// <summary>
        /// stream execute
        /// </summary>
        /// <typeparam name="TReq"></typeparam>
        /// <typeparam name="TResp"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="request"></param>
        /// <param name="onError"></param>
        /// <param name="isDone"></param>
        /// <param name="onCallback"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public void StreamExecute<TReq, TResp>(byte reqCode, byte respCode, TReq request, Action<Exception> onError,
            Func<TResp, bool> isDone,
            Action<IEnumerable<TResp>> onCallback,
            int millisecondsReceiveTimeout)
        {
            var list = new List<TResp>();
            this.Execute(reqCode, respCode, this.Serialize(reqCode, request), onError, arrSeg =>
            {
                TResp result;
                using (var stream = new MemoryStream(arrSeg.Array, arrSeg.Offset, arrSeg.Count))
                    result = ProtoBuf.Serializer.Deserialize<TResp>(stream);

                list.Add(result);
                return isDone(result);
            },
            () => onCallback(list), millisecondsReceiveTimeout);
        }
        /// <summary>
        /// stream execute
        /// </summary>
        /// <typeparam name="TResp"></typeparam>
        /// <param name="reqCode"></param>
        /// <param name="respCode"></param>
        /// <param name="onError"></param>
        /// <param name="isDone"></param>
        /// <param name="onCallback"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        public void StreamExecute<TResp>(byte reqCode, byte respCode, Action<Exception> onError,
            Func<TResp, bool> isDone,
            Action<IEnumerable<TResp>> onCallback,
            int millisecondsReceiveTimeout)
        {
            var list = new List<TResp>();
            this.Execute(reqCode, respCode, new byte[] { 0, 0, 0, 1, reqCode }, onError, arrSeg =>
            {
                TResp result;
                using (var stream = new MemoryStream(arrSeg.Array, arrSeg.Offset, arrSeg.Count))
                    result = ProtoBuf.Serializer.Deserialize<TResp>(stream);

                list.Add(result);
                return isDone(result);
            },
            () => onCallback(list), millisecondsReceiveTimeout);
        }
        #endregion
    }
}