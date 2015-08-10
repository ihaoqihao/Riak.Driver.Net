using System;

namespace Riak.Driver
{
    /// <summary>
    /// riak response
    /// </summary>
    public sealed class RiakResponse : Sodao.FastSocket.Client.Messaging.IMessage
    {
        /// <summary>
        /// get seqID
        /// </summary>
        private readonly int _seqID;
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
        /// <param name="seqID"></param>
        /// <param name="messageCode"></param>
        /// <param name="onReceive"></param>
        /// <exception cref="ArgumentNullException">onReceive is null</exception>
        public RiakResponse(int seqID, byte messageCode, Func<ArraySegment<byte>, bool> onReceive)
        {
            if (onReceive == null) throw new ArgumentNullException("onReceive");

            this._seqID = seqID;
            this.MessageCode = messageCode;
            this.OnReceive = onReceive;
        }

        /// <summary>
        /// get seqID
        /// </summary>
        public int SeqID
        {
            get { return this._seqID; }
        }
    }
}