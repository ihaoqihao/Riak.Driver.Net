using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Riak.Driver
{
    /// <summary>
    /// riak object
    /// </summary>
    public sealed class RiakObject
    {
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public RiakObject(string bucket, string key, string value)
            : this(bucket, key, Encoding.UTF8.GetBytes(value))
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public RiakObject(string bucket, string key, byte[] value)
        {

        }
    }
}