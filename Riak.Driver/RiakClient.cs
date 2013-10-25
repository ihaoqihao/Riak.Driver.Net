using System;
using System.Collections.Generic;
using System.Linq;
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
        static private readonly byte[] APPLICATIONJSON = "application/json".GetBytes();

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

        #region Bucket
        /// <summary>
        /// get bucket properties
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RpbBucketProps> GetBucketProperties(string bucket,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<RpbBucketProps>(asyncState);
            this._socket.Execute<RpbGetBucketReq, RpbGetBucketResp>(Codes.GetBucketReq, Codes.GetBucketResp,
                new RpbGetBucketReq { bucket = bucket.GetBytes() },
                ex => source.TrySetException(ex),
                result => source.TrySetResult(result.props), millisecondsReceiveTimeout);
            return source.Task;
        }
        /// <summary>
        /// set bucket properties
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="properties"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task SetBucketProperties(string bucket,
            RpbBucketProps properties,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<bool>(asyncState);
            this._socket.Execute<RpbSetBucketReq>(Codes.SetBucketReq, Codes.SetBucketResp,
                new RpbSetBucketReq { bucket = bucket.GetBytes(), props = properties },
                ex => source.TrySetException(ex),
                () => source.TrySetResult(true),
                millisecondsReceiveTimeout);
            return source.Task;
        }
        /// <summary>
        /// list buckets
        /// </summary>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<string[]> ListBuckets(int millisecondsReceiveTimeout = 3000, object asyncState = null)
        {
            var source = new TaskCompletionSource<string[]>(asyncState);
            this._socket.Execute<RpbListBucketsResp>(Codes.ListBucketsReq, Codes.ListBucketsResp,
                ex => source.TrySetException(ex),
                result => source.TrySetResult(result.buckets.Select(c => c.GetString()).ToArray()),
                millisecondsReceiveTimeout);
            return source.Task;
        }
        #endregion

        #region Object
        /// <summary>
        /// put
        /// </summary>
        /// <param name="value"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Put(RiakObject value,
            Action<PutOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            if (value == null) throw new ArgumentNullException("value");

            var request = value.ToRpbPutReq();
            if (setOptions != null) setOptions(new PutOptions(request));

            var source = new TaskCompletionSource<RiakObject>(asyncState);
            this._socket.Execute<RpbPutReq, RpbPutResp>(Codes.PutReq, Codes.PutResp, request,
                ex => source.TrySetException(ex),
                result =>
                {
                    if (request.return_body) source.TrySetResult(new RiakObject(value.Bucket, value.Key, result.vclock, result.content));
                    else source.TrySetResult(value);
                }, millisecondsReceiveTimeout);
            return source.Task;
        }
        /// <summary>
        /// get
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Get(string bucket,
            string key,
            Action<GetOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            return this.Get(bucket, key.GetBytes(), setOptions, millisecondsReceiveTimeout, asyncState);
        }
        /// <summary>
        /// get
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="keys"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject[]> Get(string bucket,
            string[] keys,
            Action<GetOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var tasks = new Task<RiakObject>[keys.Length];
            for (int i = 0, l = keys.Length; i < l; i++)
                tasks[i] = this.Get(bucket, keys[i].GetBytes(), setOptions, millisecondsReceiveTimeout);

            var source = new TaskCompletionSource<RiakObject[]>(asyncState);
            Task.Factory.ContinueWhenAll(tasks, arr =>
            {
                var arrResult = new RiakObject[arr.Length];
                for (int i = 0, l = arr.Length; i < l; i++)
                {
                    var t = arr[i];
                    if (t.IsFaulted)
                    {
                        source.TrySetException(t.Exception.InnerException);
                        return;
                    }
                    arrResult[i] = t.Result;
                }
                source.TrySetResult(arrResult);
            });
            return source.Task;
        }
        /// <summary>
        /// get
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<RiakObject> Get(string bucket,
            byte[] key,
            Action<GetOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<RiakObject>(asyncState);

            var request = new RpbGetReq { bucket = bucket.GetBytes(), key = key };
            if (setOptions != null) setOptions(new GetOptions(request));

            this._socket.Execute<RpbGetReq, RpbGetResp>(Codes.GetReq, Codes.GetResp, request,
                ex => source.TrySetException(ex),
                result =>
                {
                    if (result == null) source.TrySetResult(null);
                    else source.TrySetResult(new RiakObject(bucket, key, result.vclock, result.content));
                }, millisecondsReceiveTimeout);
            return source.Task;
        }
        /// <summary>
        /// delete
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task Delete(string bucket,
            string key,
            Action<DeleteOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            return this.Delete(bucket, key.GetBytes(), setOptions, millisecondsReceiveTimeout, asyncState);
        }
        /// <summary>
        /// delete
        /// </summary>
        /// <param name="value"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">value is null</exception>
        public Task Delete(RiakObject value,
            Action<DeleteOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            if (value == null) throw new ArgumentNullException("value");
            return this.Delete(value.Bucket, value.Key, setOptions, millisecondsReceiveTimeout, asyncState);
        }
        /// <summary>
        /// delete
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task Delete(string bucket,
            byte[] key,
            Action<DeleteOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<bool>(asyncState);
            var req = new RpbDelReq { bucket = bucket.GetBytes(), key = key };
            if (setOptions != null) setOptions(new DeleteOptions(req));

            this._socket.Execute<RpbDelReq>(Codes.DelReq, Codes.DelResp, req,
                ex => source.TrySetException(ex),
                () => source.TrySetResult(true),
                millisecondsReceiveTimeout);
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
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<IndexQueryResult> IndexQuery(string bucket,
            string indexName,
            long minValue,
            long maxValue,
            uint maxResults,
            byte[] continuation = null,
            bool returnTerms = false,
            int millisecondsReceiveTimeout = 3000,
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
            result => source.TrySetResult(new IndexQueryResult(result)),
            millisecondsReceiveTimeout);
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
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<IndexQueryResult> IndexQuery(string bucket,
            string indexName,
            string value,
            uint maxResults,
            byte[] continuation = null,
            bool returnTerms = false,
            int millisecondsReceiveTimeout = 3000,
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
            result => source.TrySetResult(new IndexQueryResult(result)),
            millisecondsReceiveTimeout);
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
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> Increment(string bucket,
            string key,
            long amount,
            Action<CounterUpdateOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            return this.Increment(bucket, key.GetBytes(), amount, setOptions, millisecondsReceiveTimeout, asyncState);
        }
        /// <summary>
        /// increment counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="amount"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> Increment(string bucket,
            byte[] key,
            long amount,
            Action<CounterUpdateOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<long?>(asyncState);
            var req = new RpbCounterUpdateReq { bucket = bucket.GetBytes(), key = key, amount = amount, returnvalue = true };
            if (setOptions != null) setOptions(new CounterUpdateOptions(req));

            this._socket.Execute<RpbCounterUpdateReq, RpbCounterUpdateResp>(Codes.CounterUpdateReq, Codes.CounterUpdateResp, req,
                ex => source.TrySetException(ex),
                result =>
                {
                    if (result == null) source.TrySetResult(null);
                    else source.TrySetResult(result.returnvalue);
                }, millisecondsReceiveTimeout);
            return source.Task;
        }
        /// <summary>
        /// get counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> GetCounter(string bucket,
            string key,
            Action<CounterGetOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            return this.GetCounter(bucket, key.GetBytes(), setOptions, millisecondsReceiveTimeout, asyncState);
        }
        /// <summary>
        /// get counter
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="setOptions"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<long?> GetCounter(string bucket,
            byte[] key,
            Action<CounterGetOptions> setOptions = null,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<long?>(asyncState);
            var req = new RpbCounterGetReq { bucket = bucket.GetBytes(), key = key };
            if (setOptions != null) setOptions(new CounterGetOptions(req));

            this._socket.Execute<RpbCounterGetReq, RpbCounterGetResp>(Codes.CounterGetReq, Codes.CounterGetResp, req,
                ex => source.TrySetException(ex),
                response =>
                {
                    if (response == null) source.TrySetResult(null);
                    else source.TrySetResult(response.returnvalue);
                }, millisecondsReceiveTimeout);
            return source.Task;
        }
        #endregion

        #region MapReduce
        /// <summary>
        /// MapReduce
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="request"></param>
        /// <param name="selector"></param>
        /// <param name="millisecondsReceiveTimeout"></param>
        /// <param name="asyncState"></param>
        /// <returns></returns>
        public Task<T[]> MapReduce<T>(string request,
            Func<RpbMapRedResp, IEnumerable<T>> selector,
            int millisecondsReceiveTimeout = 3000,
            object asyncState = null)
        {
            var source = new TaskCompletionSource<T[]>(asyncState);
            this._socket.StreamExecute<RpbMapRedReq, RpbMapRedResp>(Codes.MapRedReq, Codes.MapRedResp, new RpbMapRedReq
            {
                request = request.GetBytes(),
                content_type = APPLICATIONJSON
            },
            ex => source.TrySetException(ex),
            response => response.done,
            responses =>
            {
                try { source.TrySetResult(responses.SelectMany(selector).ToArray()); }
                catch (Exception ex) { source.TrySetException(ex); }
            },
            millisecondsReceiveTimeout);
            return source.Task;
        }
        #endregion

        //#region Extension
        ///// <summary>
        ///// list values
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="bucket"></param>
        ///// <param name="keys"></param>
        ///// <param name="millisecondsReceiveTimeout"></param>
        ///// <param name="asyncState"></param>
        ///// <returns></returns>
        ///// <exception cref="ArgumentNullException">keys is null or empty.</exception>
        //public Task<T[]> ListValues<T>(string bucket, string[] keys, int millisecondsReceiveTimeout = 3000, object asyncState = null)
        //{
        //    if (keys == null || keys.Length == 0) throw new ArgumentNullException("keys");

        //    //{riak_pipe, [{worker_limit, 100000},{worker_queue_limit, 4096000}]},
        //    string str = string.Concat("{\"inputs\":{\"bucket\":\"", bucket,
        //        "\",\"key_filters\":[[\"set_member\",\"", string.Join("\",\"", keys),
        //        "\"]]},\"query\":[{\"map\":{ \"language\":\"javascript\",\"name\":\"Riak.mapValuesJson\"}}]}");

        //    return this.MapReduce<T>(str, resp =>
        //    {
        //        if (resp == null || resp.response == null) return new T[0];
        //        return Newtonsoft.Json.JsonConvert.DeserializeObject<T[]>(resp.response.GetString());
        //    }, millisecondsReceiveTimeout, asyncState);
        //}
        //#endregion
    }
}