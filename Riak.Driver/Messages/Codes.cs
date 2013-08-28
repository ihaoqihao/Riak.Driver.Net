
namespace Riak.Driver.Messages
{
    /// <summary>
    /// message codes
    /// </summary>
    static public class Codes
    {
        /// <summary>
        /// error response
        /// </summary>
        public const byte ErrorResp = 0;
        /// <summary>
        /// ping request
        /// </summary>
        public const byte PingReq = 1;
        /// <summary>
        /// ping response
        /// </summary>
        public const byte PingResp = 2;
        /// <summary>
        /// get clientId request
        /// </summary>
        public const byte GetClientIdReq = 3;
        /// <summary>
        /// get clientId response
        /// </summary>
        public const byte GetClientIdResp = 4;
        /// <summary>
        /// set clientId request
        /// </summary>
        public const byte SetClientIdReq = 5;
        /// <summary>
        /// set clientid response
        /// </summary>
        public const byte SetClientIdResp = 6;
        /// <summary>
        /// get server info request
        /// </summary>
        public const byte GetServerInfoReq = 7;
        /// <summary>
        /// get server info response
        /// </summary>
        public const byte GetServerInfoResp = 8;
        /// <summary>
        /// get request
        /// </summary>
        public const byte GetReq = 9;
        /// <summary>
        /// get response
        /// </summary>
        public const byte GetResp = 10;
        /// <summary>
        /// put request
        /// </summary>
        public const byte PutReq = 11;
        /// <summary>
        /// put response
        /// </summary>
        public const byte PutResp = 12;
        /// <summary>
        /// delete request
        /// </summary>
        public const byte DelReq = 13;
        /// <summary>
        /// delete response
        /// </summary>
        public const byte DelResp = 14;
        /// <summary>
        /// list buckets request
        /// </summary>
        public const byte ListBucketsReq = 15;
        /// <summary>
        /// list buckets response
        /// </summary>
        public const byte ListBucketsResp = 16;
        /// <summary>
        /// list keys request
        /// </summary>
        public const byte ListKeysReq = 17;
        /// <summary>
        /// list keys response
        /// </summary>
        public const byte ListKeysResp = 18;
        /// <summary>
        /// get bucket request
        /// </summary>
        public const byte GetBucketReq = 19;
        /// <summary>
        /// get bucket response
        /// </summary>
        public const byte GetBucketResp = 20;
        /// <summary>
        /// set bucket request
        /// </summary>
        public const byte SetBucketReq = 21;
        /// <summary>
        /// set bucket response
        /// </summary>
        public const byte SetBucketResp = 22;
        /// <summary>
        /// map-reduce request
        /// </summary>
        public const byte MapRedReq = 23;
        /// <summary>
        /// map-reduce response
        /// </summary>
        public const byte MapRedResp = 24;
        /// <summary>
        /// index request
        /// </summary>
        public const byte IndexReq = 25;
        /// <summary>
        /// index response
        /// </summary>
        public const byte IndexResp = 26;
        /// <summary>
        /// seach request
        /// </summary>
        public const byte SearchQueryReq = 27;
        /// <summary>
        /// search response
        /// </summary>
        public const byte SearchQueryResp = 28;
        /// <summary>
        /// counter update request
        /// </summary>
        public const byte CounterUpdateReq = 50;
        /// <summary>
        /// counter update response
        /// </summary>
        public const byte CounterUpdateResp = 51;
        /// <summary>
        /// counter get request
        /// </summary>
        public const byte CounterGetReq = 52;
        /// <summary>
        /// counter get response
        /// </summary>
        public const byte CounterGetResp = 53;
    }
}