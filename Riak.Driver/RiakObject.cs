using Riak.Driver.Utils;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Riak.Driver
{
    /// <summary>
    /// riak object
    /// </summary>
    public sealed class RiakObject
    {
        #region Members
        /// <summary>
        /// get bucket
        /// </summary>
        public readonly string Bucket;
        /// <summary>
        /// get key
        /// </summary>
        public readonly byte[] Key;
        /// <summary>
        /// get vector clock
        /// </summary>
        public readonly byte[] VectorClock;
        /// <summary>
        /// siblings objects
        /// </summary>
        public readonly RiakObject[] Siblings;

        private readonly Messages.RpbContent _content = null;
        #endregion

        #region Constructors
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        public RiakObject(string bucket, string key)
            : this(bucket, key.GetBytes(), null)
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public RiakObject(string bucket, string key, string value)
            : this(bucket, key.GetBytes(), value.GetBytes())
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public RiakObject(string bucket, string key, byte[] value)
            : this(bucket, key.GetBytes(), value)
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public RiakObject(string bucket, byte[] key, byte[] value)
            : this(bucket, key, null, new Messages.RpbContent { value = value })
        {
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="vectorClock"></param>
        /// <param name="content"></param>
        /// <exception cref="ArgumentNullException">bucket is null or empty.</exception>
        internal RiakObject(string bucket, byte[] key, byte[] vectorClock, Messages.RpbContent content)
        {
            if (string.IsNullOrEmpty(bucket)) throw new ArgumentNullException("bucket");

            this.Bucket = bucket;
            this.Key = key;
            this.VectorClock = vectorClock;
            this._content = content;
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="vectorClock"></param>
        /// <param name="contents"></param>
        internal RiakObject(string bucket, byte[] key, byte[] vectorClock, List<Messages.RpbContent> contents)
            : this(bucket, key, vectorClock, contents[0])
        {
            if (contents.Count < 2) return;
            this.Siblings = contents.Select(c => new RiakObject(bucket, key, vectorClock, c)).ToArray();
        }
        #endregion

        #region Public Properties
        /// <summary>
        /// get or setvalue
        /// </summary>
        public byte[] Value
        {
            get { return this._content.value; }
            set { this._content.value = value; }
        }
        #endregion

        #region Index
        /// <summary>
        /// get index by key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        public byte[][] GetIndex(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            if (this._content.indexes.Count == 0) return new byte[0][];
            return this._content.indexes.Where(c => c.key.GetString().StartsWith(key)).Select(c => c.value).ToArray();
        }
        /// <summary>
        /// add index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <exception cref="ArgumentNullException">value is null or empty</exception>
        /// <returns></returns>
        public RiakObject AddIndex(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException("value");
            this._content.indexes.Add(new Messages.RpbPair { key = string.Concat(key, "_bin").GetBytes(), value = value.GetBytes() });
            return this;
        }
        /// <summary>
        /// add index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <returns></returns>
        public RiakObject AddIndex(string key, int value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            this._content.indexes.Add(new Messages.RpbPair { key = string.Concat(key, "_int").GetBytes(), value = value.ToString().GetBytes() });
            return this;
        }
        /// <summary>
        /// add index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <returns></returns>
        public RiakObject AddIndex(string key, long value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            this._content.indexes.Add(new Messages.RpbPair { key = string.Concat(key, "_int").GetBytes(), value = value.ToString().GetBytes() });
            return this;
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <exception cref="ArgumentNullException">value is null or empty</exception>
        /// <returns></returns>
        public RiakObject RemoveIndex(string key, string value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            if (string.IsNullOrEmpty(value)) throw new ArgumentNullException("value");
            if (this._content.indexes.Count == 0) return this;

            var hits = this._content.indexes.Where(c => c.key.GetString().StartsWith(key, StringComparison.OrdinalIgnoreCase) && c.value.GetString() == value).ToArray();
            if (hits.Length == 0) return this;

            for (int i = 0, l = hits.Length; i < l; i++)
                this._content.indexes.Remove(hits[i]);

            return this;
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <returns></returns>
        public RiakObject RemoveIndex(string key, int value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            if (this._content.indexes.Count == 0) return this;

            var hits = this._content.indexes.Where(c => c.key.GetString().StartsWith(key, StringComparison.OrdinalIgnoreCase) &&
                c.value.GetString() == value.ToString()).ToArray();
            if (hits.Length == 0) return this;

            for (int i = 0, l = hits.Length; i < l; i++)
                this._content.indexes.Remove(hits[i]);

            return this;
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <returns></returns>
        public RiakObject RemoveIndex(string key, long value)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            if (this._content.indexes.Count == 0) return this;

            var hits = this._content.indexes.Where(c => c.key.GetString().StartsWith(key, StringComparison.OrdinalIgnoreCase) &&
                c.value.GetString() == value.ToString()).ToArray();
            if (hits.Length == 0) return this;

            for (int i = 0, l = hits.Length; i < l; i++)
                this._content.indexes.Remove(hits[i]);

            return this;
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <exception cref="ArgumentNullException">key is null or empty</exception>
        /// <returns></returns>
        public RiakObject RemoveIndex(string key)
        {
            if (string.IsNullOrEmpty(key)) throw new ArgumentNullException("key");
            if (this._content.indexes.Count == 0) return this;

            var hits = this._content.indexes.Where(c => c.key.GetString().StartsWith(key, StringComparison.OrdinalIgnoreCase)).ToArray();
            if (hits.Length == 0) return this;

            for (int i = 0, l = hits.Length; i < l; i++)
                this._content.indexes.Remove(hits[i]);

            return this;
        }
        #endregion

        #region ToRpbPutReq
        /// <summary>
        /// ToRpbPutReq
        /// </summary>
        /// <returns></returns>
        internal Messages.RpbPutReq ToRpbPutReq()
        {
            return new Messages.RpbPutReq
            {
                bucket = this.Bucket.GetBytes(),
                key = this.Key,
                content = this._content,
                vclock = this.VectorClock
            };
        }
        #endregion
    }
}