using System.Linq;
using Riak.Driver.Utils;

namespace Riak.Driver
{
    /// <summary>
    /// index result
    /// </summary>
    public sealed class IndexResult
    {
        /// <summary>
        /// get continuation
        /// </summary>
        public readonly byte[] Continuation;
        /// <summary>
        /// get results
        /// </summary>
        public readonly Term[] Results;

        /// <summary>
        /// new
        /// </summary>
        /// <param name="response"></param>
        public IndexResult(Messages.RpbIndexResp response)
        {
            if (response == null)
            {
                this.Results = new Term[0]; return;
            }

            this.Continuation = response.continuation;
            if (response.keys.Count > 0) this.Results = response.keys.Select(c => new Term(c.GetString())).ToArray();
            else this.Results = response.results.Select(c => new Term(c.value.GetString(), c.key.GetString())).ToArray();
        }

        /// <summary>
        /// term
        /// </summary>
        public class Term
        {
            /// <summary>
            /// get key
            /// </summary>
            public readonly string Key;
            /// <summary>
            /// get value
            /// </summary>
            public readonly string Value;

            /// <summary>
            /// new
            /// </summary>
            /// <param name="key"></param>
            public Term(string key)
            {
                this.Key = key;
            }
            /// <summary>
            /// new
            /// </summary>
            /// <param name="key"></param>
            /// <param name="value"></param>
            public Term(string key, string value)
            {
                this.Key = key;
                this.Value = value;
            }
        }
    }
}