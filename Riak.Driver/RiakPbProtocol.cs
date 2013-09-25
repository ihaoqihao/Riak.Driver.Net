using System;
using System.IO;
using System.Text;
using Sodao.FastSocket.Client.Protocol;
using Sodao.FastSocket.SocketBase;
using Sodao.FastSocket.SocketBase.Utils;

namespace Riak.Driver
{
    /// <summary>
    /// riak protocolbuf protocol
    /// http://docs.basho.com/riak/latest/dev/references/protocol-buffers/
    /// 00 00 00 07 09 0A 01 62 12 01 6B
    /// |----Len---|MC|----Message-----|
    /// </summary>
    public sealed class RiakPbProtocol : IProtocol<RiakResponse>
    {
        /// <summary>
        /// find response
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="buffer"></param>
        /// <param name="readlength"></param>
        /// <returns></returns>
        public RiakResponse FindResponse(IConnection connection, ArraySegment<byte> buffer, out int readlength)
        {
            //riak协议长度至少5字节(len+mc)
            if (buffer.Count < 5) { readlength = 0; return null; }

            //riak协议编码为big-endian
            var len = NetworkBitConverter.ToInt32(buffer.Array, buffer.Offset);
            if (len < 1) throw new BadProtocolException();

            //riak协议长度=message code(1 byte)+Message(N bytes)
            readlength = len + 4;
            //判断本次接收数据是否完整
            if (readlength > buffer.Count) { readlength = 0; return null; }

            var response = connection.UserData as RiakResponse;
            if (response == null) throw new BadProtocolException("unknow response.");

            //第5个字节为MessageCode
            var messageCode = buffer.Array[buffer.Offset + 4];

            //riak error response
            if (messageCode == Messages.Codes.ErrorResp)
            {
                Messages.RpbErrorResp errResp = null;
                using (var stream = new MemoryStream(buffer.Array, buffer.Offset + 5, len - 1))
                {
                    errResp = ProtoBuf.Serializer.Deserialize<Messages.RpbErrorResp>(stream);
                }
                response.Exception = new RiakException(errResp.errcode, Encoding.UTF8.GetString(errResp.errmsg));
                return response;
            }
            //message code mismatching
            if (messageCode != response.MessageCode)
            {
                response.Exception = new RiakException(string.Concat(
                    "invalid response, expected is ", response.MessageCode.ToString(), ", but was is ", messageCode.ToString()));
                return response;
            }

            if (len == 1) return response;

            if (response.OnReceive(new ArraySegment<byte>(buffer.Array, buffer.Offset + 5, len - 1))) return response;
            else return null;
        }
    }
}