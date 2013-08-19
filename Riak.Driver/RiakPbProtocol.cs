using System;
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
    /// Len = 0x07
    /// Message Code (MC) = 0x09 = RpbGetReq
    /// RpbGetReq Message = 0x0A 0x01 0x62 0x12 0x01 0x6B
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

            //第5个字节为MessageCode
            var messageCode = buffer.Array[buffer.Offset + 4];
            var bytes = new byte[len - 1];
            Buffer.BlockCopy(buffer.Array, buffer.Offset + 5, bytes, 0, bytes.Length);

            return new RiakResponse(-1, messageCode, bytes);
        }
    }
}