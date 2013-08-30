using System.Linq;
using Riak.Driver.Utils;

namespace Riak.Driver
{
    /// <summary>
    /// index query result
    /// </summary>
    public sealed class IndexQueryResult
    {
        /// <summary>
        /// get continuation
        /// </summary>
        public readonly byte[] Continuation;
        /// <summary>
        /// get results
        /// </summary>
        public readonly IndexTerm[] Results;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="response"></param>
        public IndexQueryResult(Messages.RpbIndexResp response)
        {
            if (response == null)
            {
                this.Results = new IndexTerm[0]; return;
            }

            this.Continuation = response.continuation;
            if (response.keys.Count > 0) this.Results = response.keys.Select(c => new IndexTerm(c.GetString())).ToArray();
            else this.Results = response.results.Select(c => new IndexTerm(c.value.GetString(), c.key.GetString())).ToArray();
        }

        /// <summary>
        /// term
        /// </summary>
        public class IndexTerm
        {
            /// <summary>
            /// get key
            /// </summary>
            public readonly string Key;
            /// <summary>
            /// get value
            /// </summary>
            public readonly string Term;

            /// <summary>
            /// new
            /// </summary>
            /// <param name="key"></param>
            public IndexTerm(string key)
            {
                this.Key = key;
            }
            /// <summary>
            /// new
            /// </summary>
            /// <param name="key"></param>
            /// <param name="term"></param>
            public IndexTerm(string key, string term)
            {
                this.Key = key;
                this.Term = term;
            }
        }
    }
}