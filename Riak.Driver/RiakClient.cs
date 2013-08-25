using System;
using System.Threading.Tasks;
using Riak.Driver.Messages;

namespace Riak.Driver
{
    /// <summary>
    /// riak client
    /// </summary>
    public sealed class RiakClient
    {
        #region Private Members
        private readonly RiakSocketClient _socket = null;
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        /// <param name="socket"></param>
        public RiakClient(RiakSocketClient socket)
        {
            if (socket == null) throw new ArgumentNullException("socket");
            this._socket = socket;
        }
        #endregion

        #region Put
        /// <summary>
        /// put
        /// </summary>
        /// <param name="req"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RpbPutResp> Put(RpbPutReq req, object asyncState = null)
        {
            var souce = new TaskCompletionSource<RpbPutResp>(asyncState);
            this._socket.Execute<RpbPutReq, RpbPutResp>(Codes.RpbPutReq, req, ex => souce.TrySetException(ex),
                res => souce.TrySetResult(res));
            return souce.Task;
        }
        #endregion

        #region Get
        /// <summary>
        /// get
        /// </summary>
        /// <param name="req"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RpbGetResp> Get(RpbGetReq req, object asyncState = null)
        {
            var souce = new TaskCompletionSource<RpbGetResp>(asyncState);
            this._socket.Execute<RpbGetReq, RpbGetResp>(Codes.RpbGetReq, req, ex => souce.TrySetException(ex),
                res => souce.TrySetResult(res));
            return souce.Task;
        }
        #endregion
    }
}