using System;

namespace Riak.Driver
{
    /// <summary>
    /// riak exception
    /// </summary>
    public sealed class RiakException : ApplicationException
    {
        /// <summary>
        /// error code
        /// </summary>
        public readonly uint ErrorCode;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="message"></param>
        public RiakException(string message)
            : base(message)
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="message"></param>
        public RiakException(uint errorCode, string message)
            : base(string.Concat("riak returned an error. Code:", errorCode.ToString(), ". Message: ", message))
        {
            this.ErrorCode = errorCode;
        }
    }
}