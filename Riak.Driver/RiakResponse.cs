
namespace Riak.Driver
{
    /// <summary>
    /// riak response
    /// </summary>
    public sealed class RiakResponse : Sodao.FastSocket.Client.Response.IResponse
    {
        private int _seqID;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="seqId"></param>
        /// <param name="messageCode"></param>
        /// <param name="payload"></param>
        public RiakResponse(int seqId, byte messageCode, byte[] payload)
        {
            this._seqID = seqId;
            this.MessageCode = messageCode;
            this.Payload = payload;
        }

        /// <summary>
        /// get seqID
        /// </summary>
        public int SeqID
        {
            get { return this._seqID; }
        }
        /// <summary>
        /// get message code
        /// </summary>
        public byte MessageCode
        {
            get;
            private set;
        }
        /// <summary>
        /// get payload
        /// </summary>
        public byte[] Payload
        {
            get;
            private set;
        }
    }
}