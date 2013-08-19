
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
        public const byte RpbErrorResp = 0;
        /// <summary>
        /// ping request
        /// </summary>
        public const byte RpbPingReq = 1;
        /// <summary>
        /// ping response
        /// </summary>
        public const byte RpbPingResp = 2;
        /// <summary>
        /// get clientId request
        /// </summary>
        public const byte RpbGetClientIdReq = 3;
        /// <summary>
        /// get clientId response
        /// </summary>
        public const byte RpbGetClientIdResp = 4;
        /// <summary>
        /// set clientId request
        /// </summary>
        public const byte RpbSetClientIdReq = 5;
        /// <summary>
        /// set clientid response
        /// </summary>
        public const byte RpbSetClientIdResp = 6;
        /// <summary>
        /// get server info request
        /// </summary>
        public const byte RpbGetServerInfoReq = 7;
        /// <summary>
        /// get server info response
        /// </summary>
        public const byte RpbGetServerInfoResp = 8;
        /// <summary>
        /// get request
        /// </summary>
        public const byte RpbGetReq = 9;
        /// <summary>
        /// get response
        /// </summary>
        public const byte RpbGetResp = 10;
        /// <summary>
        /// put request
        /// </summary>
        public const byte RpbPutReq = 11;
        /// <summary>
        /// put response
        /// </summary>
        public const byte RpbPutResp = 12;
        /// <summary>
        /// delete request
        /// </summary>
        public const byte RpbDelReq = 13;
        /// <summary>
        /// delete response
        /// </summary>
        public const byte RpbDelResp = 14;
        /// <summary>
        /// list buckets request
        /// </summary>
        public const byte RpbListBucketsReq = 15;
        /// <summary>
        /// list buckets response
        /// </summary>
        public const byte RpbListBucketsResp = 16;
        /// <summary>
        /// list keys request
        /// </summary>
        public const byte RpbListKeysReq = 17;
        /// <summary>
        /// list keys response
        /// </summary>
        public const byte RpbListKeysResp = 18;
        /// <summary>
        /// get bucket request
        /// </summary>
        public const byte RpbGetBucketReq = 19;
        /// <summary>
        /// get bucket response
        /// </summary>
        public const byte RpbGetBucketResp = 20;
        /// <summary>
        /// set bucket request
        /// </summary>
        public const byte RpbSetBucketReq = 21;
        /// <summary>
        /// set bucket response
        /// </summary>
        public const byte RpbSetBucketResp = 22;
        /// <summary>
        /// map-reduce request
        /// </summary>
        public const byte RpbMapRedReq = 23;
        /// <summary>
        /// map-reduce response
        /// </summary>
        public const byte RpbMapRedResp = 24;
        /// <summary>
        /// index request
        /// </summary>
        public const byte RpbIndexReq = 25;
        /// <summary>
        /// index response
        /// </summary>
        public const byte RpbIndexResp = 26;
        /// <summary>
        /// seach request
        /// </summary>
        public const byte RpbSearchQueryReq = 27;
        /// <summary>
        /// search response
        /// </summary>
        public const byte RbpSearchQueryResp = 28;
    }
}