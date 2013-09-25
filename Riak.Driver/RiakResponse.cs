using System;

namespace Riak.Driver
{
    /// <summary>
    /// riak response
    /// </summary>
    public sealed class RiakResponse : Sodao.FastSocket.Client.Response.IResponse
    {
        /// <summary>
        /// get seqId
        /// </summary>
        private readonly int _seqId;
        /// <summary>
        /// get messageCode
        /// </summary>
        public readonly byte MessageCode;
        /// <summary>
        /// get onreceive
        /// </summary>
        public readonly Func<ArraySegment<byte>, bool> OnReceive;
        /// <summary>
        /// get or set <see cref="RiakException"/>
        /// </summary>
        public RiakException Exception;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="seqId"></param>
        /// <param name="messageCode"></param>
        /// <param name="onReceive"></param>
        /// <exception cref="ArgumentNullException">onReceive is null</exception>
        public RiakResponse(int seqId, byte messageCode, Func<ArraySegment<byte>, bool> onReceive)
        {
            if (onReceive == null) throw new ArgumentNullException("onReceive");

            this._seqId = seqId;
            this.MessageCode = messageCode;
            this.OnReceive = onReceive;
        }

        /// <summary>
        /// get seqID
        /// </summary>
        public int SeqID
        {
            get { return this._seqId; }
        }
    }
}