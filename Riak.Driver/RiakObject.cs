using System.Collections.Generic;
using System.Linq;
using Riak.Driver.Utils;

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
        /// get or setvalue
        /// </summary>
        public byte[] Value;
        /// <summary>
        /// get vector clock
        /// </summary>
        public readonly byte[] VectorClock;
        /// <summary>
        /// siblings objects
        /// </summary>
        public readonly RiakObject[] Siblings;

        private readonly Messages.RpbContent Content = null;
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
        {
            this.Bucket = bucket;
            this.Key = key;
            this.Value = value;

            this.Content = new Messages.RpbContent
            {
                value = value
            };
        }
        /// <summary>
        /// new
        /// </summary>
        /// <param name="bucket"></param>
        /// <param name="key"></param>
        /// <param name="vectorClock"></param>
        /// <param name="content"></param>
        internal RiakObject(string bucket, byte[] key, byte[] vectorClock, Messages.RpbContent content)
        {
            this.Bucket = bucket;
            this.Key = key;
            this.Value = content.value;
            this.VectorClock = vectorClock;
            this.Content = content;
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

        #region Index
        /// <summary>
        /// get index by key.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public byte[][] GetIndex(string key)
        {
            if (this.Content.indexes.Count == 0) return new byte[0][];
            return this.Content.indexes.Where(c => c.key.GetString() == key).Select(c => c.value).ToArray();
        }
        /// <summary>
        /// add index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void AddIndex(string key, string value)
        {
            this.Content.indexes.Add(new Messages.RpbPair
            {
                key = key.GetBytes(),
                value = value.GetBytes()
            });
        }
        /// <summary>
        /// add index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void AddIndex(string key, int value)
        {
            this.Content.indexes.Add(new Messages.RpbPair
            {
                key = key.GetBytes(),
                value = value.ToString().GetBytes()
            });
        }
        /// <summary>
        /// add index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void AddIndex(string key, long value)
        {
            this.Content.indexes.Add(new Messages.RpbPair
            {
                key = key.GetBytes(),
                value = value.ToString().GetBytes()
            });
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void RemoveIndex(string key, string value)
        {
            if (this.Content.indexes.Count == 0) return;
            var hits = this.Content.indexes.Where(c => c.key.GetString() == key && c.value.GetString() == value).ToArray();
            if (hits.Length == 0) return;
            for (int i = 0, l = hits.Length; i < l; i++) this.Content.indexes.Remove(hits[i]);
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void RemoveIndex(string key, int value)
        {
            if (this.Content.indexes.Count == 0) return;
            var hits = this.Content.indexes.Where(c => c.key.GetString() == key && c.value.GetString() == value.ToString()).ToArray();
            if (hits.Length == 0) return;
            for (int i = 0, l = hits.Length; i < l; i++) this.Content.indexes.Remove(hits[i]);
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void RemoveIndex(string key, long value)
        {
            if (this.Content.indexes.Count == 0) return;
            var hits = this.Content.indexes.Where(c => c.key.GetString() == key && c.value.GetString() == value.ToString()).ToArray();
            if (hits.Length == 0) return;
            for (int i = 0, l = hits.Length; i < l; i++) this.Content.indexes.Remove(hits[i]);
        }
        /// <summary>
        /// remove index
        /// </summary>
        /// <param name="key"></param>
        public void RemoveINdex(string key)
        {
            if (this.Content.indexes.Count == 0) return;
            var hits = this.Content.indexes.Where(c => c.key.GetString() == key).ToArray();
            if (hits.Length == 0) return;
            for (int i = 0, l = hits.Length; i < l; i++) this.Content.indexes.Remove(hits[i]);
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
                content = this.Content,
                vclock = this.VectorClock
            };
        }
        #endregion
    }
}