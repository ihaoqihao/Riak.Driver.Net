using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Riak.Driver.Messages;
using Riak.Driver.Utils;

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

        #region Bucket
        /// <summary>
        /// get bucket properties
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RpbBucketProps> GetBucketProperties(string bucket, object asyncState = null)
        {
            var source = new TaskCompletionSource<RpbBucketProps>(asyncState);
            this._socket.Execute<RpbGetBucketReq, RpbGetBucketResp>(Codes.GetBucketReq, Codes.GetBucketResp,
                new RpbGetBucketReq { bucket = bucket.GetBytes() },
                ex => source.TrySetException(ex),
                response => source.TrySetResult(response.props));
            return source.Task;
        }
        /// <summary>
        /// set bucket properties
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="properties"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task SetBucketProperties(string bucket, RpbBucketProps properties, object asyncState = null)
        {
            var source = new TaskCompletionSource<bool>(asyncState);
            this._socket.Execute<RpbSetBucketReq>(Codes.SetBucketReq, Codes.SetBucketResp,
                new RpbSetBucketReq { bucket = bucket.GetBytes(), props = properties },
                ex => source.TrySetException(ex),
                () => source.TrySetResult(true));
            return source.Task;
        }
        #endregion

        #region Object
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
        /// <summary>
        /// get 
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Get(string bucket, string key, object asyncState = null)
        {
            return this.Get(bucket, key.GetBytes(), asyncState);
        }
        /// <summary>
        /// get
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Get(string bucket, byte[] key, object asyncState = null)
        {
            var source = new TaskCompletionSource<RiakObject>();

            this._socket.Execute<RpbGetReq, RpbGetResp>(Codes.GetReq, Codes.GetResp,
                new RpbGetReq { bucket = bucket.GetBytes(), key = key },
                ex => source.TrySetException(ex),
                response =>
                {
                    if (response == null) source.TrySetResult(null);
                    else source.TrySetResult(new RiakObject(bucket, key, response.vclock, response.content));
                });
            return source.Task;
        }
        /// <summary>
        /// delete
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task Delete(string bucket, string key, object asyncState = null)
        {
            return this.Delete(bucket, key.GetBytes(), asyncState);
        }
        /// <summary>
        /// delete
        /// </summary>
        /// <param name="value"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">value is null</exception>
        public Task Delete(RiakObject value, object asyncState)
        {
            if (value == null) throw new ArgumentNullException("value");
            return this.Delete(value.Bucket, value.Key);
        }
        /// <summary>
        /// delete
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task Delete(string bucket, byte[] key, object asyncState = null)
        {
            var source = new TaskCompletionSource<bool>();
            this._socket.Execute<RpbDelReq>(Codes.DelReq, Codes.DelResp,
                new RpbDelReq { bucket = bucket.GetBytes(), key = key },
                ex => source.TrySetException(ex),
                () => source.TrySetResult(true));
            return source.Task;
        }
        #endregion

        #region Index
        /// <summary>
        /// index query
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="indexName"></param>
        /// <param name="minValue"></param>
        /// <param name="maxValue"></param>
        /// <param name="maxResults"></param>
        /// <param name="continuation"></param>
        /// <param name="returnTerms"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<IndexQueryResult> IndexQuery(string bucket, string indexName, long minValue, long maxValue,
            uint maxResults, byte[] continuation = null, bool returnTerms = false,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<IndexQueryResult>(asyncState);
            this._socket.Execute<RpbIndexReq, RpbIndexResp>(Codes.IndexReq, Codes.IndexResp, new RpbIndexReq
            {
                bucket = bucket.GetBytes(),
                continuation = continuation,
                max_results = maxResults,
                return_terms = returnTerms,
                index = string.Concat(indexName, "_int").GetBytes(),
                qtype = RpbIndexReq.IndexQueryType.range,
                range_min = minValue.ToString().GetBytes(),
                range_max = maxValue.ToString().GetBytes()
            },
            ex => source.TrySetException(ex),
            response => source.TrySetResult(new IndexQueryResult(response)));
            return source.Task;
        }
        /// <summary>
        /// index query
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="indexName"></param>
        /// <param name="value"></param>
        /// <param name="maxResults"></param>
        /// <param name="continuation"></param>
        /// <param name="returnTerms"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<IndexQueryResult> IndexQuery(string bucket, string indexName, string value,
            uint maxResults, byte[] continuation = null, bool returnTerms = false,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<IndexQueryResult>(asyncState);
            this._socket.Execute<RpbIndexReq, RpbIndexResp>(Codes.IndexReq, Codes.IndexResp, new RpbIndexReq
            {
                bucket = bucket.GetBytes(),
                continuation = continuation,
                return_terms = returnTerms,
                max_results = maxResults,
                index = string.Concat(indexName, "_bin").GetBytes(),
                qtype = RpbIndexReq.IndexQueryType.eq,
                key = value.GetBytes()
            },
            ex => source.TrySetException(ex),
            response => source.TrySetResult(new IndexQueryResult(response)));
            return source.Task;
        }
        #endregion

        #region Counter
        /// <summary>
        /// increment counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="amount"></param>
        /// <param name="returnValue"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> Increment(string bucket, string key, long amount, bool returnValue = false, object asyncState = null)
        {
            return this.Increment(bucket, key.GetBytes(), amount, returnValue, asyncState);
        }
        /// <summary>
        /// increment counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="amount"></param>
        /// <param name="returnValue"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> Increment(string bucket, byte[] key, long amount, bool returnValue = false, object asyncState = null)
        {
            var source = new TaskCompletionSource<long?>();
            this._socket.Execute<RpbCounterUpdateReq, RpbCounterUpdateResp>(Codes.CounterUpdateReq, Codes.CounterUpdateResp,
                new RpbCounterUpdateReq
                {
                    bucket = bucket.GetBytes(),
                    key = key,
                    amount = amount,
                    returnvalue = returnValue
                },
                ex => source.TrySetException(ex),
                response =>
                {
                    if (response == null) source.TrySetResult(null);
                    else source.TrySetResult(response.returnvalue);
                });
            return source.Task;
        }
        /// <summary>
        /// get counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> GetCounter(string bucket, string key, object asyncState = null)
        {
            return this.GetCounter(bucket, key.GetBytes(), asyncState);
        }
        /// <summary>
        /// get counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> GetCounter(string bucket, byte[] key, object asyncState = null)
        {
            var source = new TaskCompletionSource<long?>();
            this._socket.Execute<RpbCounterGetReq, RpbCounterGetResp>(Codes.CounterGetReq, Codes.CounterGetResp,
                new RpbCounterGetReq { bucket = bucket.GetBytes(), key = key },
                ex => source.TrySetException(ex),
                response =>
                {
                    if (response == null) source.TrySetResult(null);
                    else source.TrySetResult(response.returnvalue);
                });
            return source.Task;
        }
        #endregion
    }
}