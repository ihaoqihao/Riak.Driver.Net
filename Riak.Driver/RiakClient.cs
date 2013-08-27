using System;
using System.Collections.Generic;
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
        private readonly HashSet<string> _setCounter = new HashSet<string>();
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

        #region Object/Key Operations
        /// <summary>
        /// new
        /// </summary>
        /// <param name="value"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Put(RiakObject value, object asyncState = null)
        {
            return this.Put(value, false, asyncState);
        }
        /// <summary>
        /// put
        /// </summary>
        /// <param name="value"></param>
        /// <param name="returnBody"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Put(RiakObject value, bool returnBody = false, object asyncState = null)
        {
            if (value == null) throw new ArgumentNullException("value");

            var request = value.ToRpbPutReq();
            request.return_body = returnBody;

            var source = new TaskCompletionSource<RiakObject>();
            this._socket.Execute<RpbPutReq, RpbPutResp>(Codes.PutReq, Codes.PutResp, request,
                ex => source.TrySetException(ex),
                response =>
                {
                    if (returnBody)
                    {
                        source.TrySetResult(new RiakObject(value.Bucket, value.Key, response.vclock, response.content));
                        return;
                    }
                    source.TrySetResult(value);
                });
            return source.Task;
        }
        #endregion

        #region Bucket Operations
        #endregion
    }
}