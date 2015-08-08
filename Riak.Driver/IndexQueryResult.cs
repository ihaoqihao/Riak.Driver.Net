using Riak.Driver.Utils;
using System.Collections.Generic;
using System.Linq;

namespace Riak.Driver
{
    /// <summary>
    /// index query result
    /// </summary>
    public sealed class IndexQueryResult
    {
        #region Members
        /// <summary>
        /// get continuation
        /// </summary>
        public readonly byte[] Continuation;
        /// <summary>
        /// get results
        /// </summary>
        public readonly KeyValuePair<string, string>[] Results;
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        /// <param name="response"></param>
        public IndexQueryResult(Messages.RpbIndexResp response)
        {
            if (response == null) return;

            this.Continuation = response.continuation;
            this.Results = response.keys.Count > 0 ?
                response.keys.Select(c => new KeyValuePair<string, string>(c.GetString(), null)).ToArray() :
                response.results.Select(c => new KeyValuePair<string, string>(c.key.GetString(), c.value.GetString())).ToArray();
        }
        #endregion
    }

    /// <summary>
    /// index query result
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class IndexQueryResult<T>
    {
        #region Members
        /// <summary>
        /// get continuation
        /// </summary>
        public readonly string Continuation;
        /// <summary>
        /// result array
        /// </summary>
        public readonly List<T> Results;
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        public IndexQueryResult()
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="continuation"></param>
        /// <param name="results"></param>
        public IndexQueryResult(string continuation, List<T> results)
        {
            this.Continuation = continuation;
            this.Results = results;
        }
        #endregion
    }
}