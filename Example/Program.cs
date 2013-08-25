using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Example
{
    class Program
    {
        static void Main(string[] args)
        {
            var riakSocketClient = new Riak.Driver.RiakSocketClient();
            riakSocketClient.RegisterServerNode("1", new System.Net.IPEndPoint(System.Net.IPAddress.Parse("10.0.20.70"), 8087));
            riakSocketClient.RegisterServerNode("2", new System.Net.IPEndPoint(System.Net.IPAddress.Parse("10.0.20.71"), 8087));
            riakSocketClient.RegisterServerNode("3", new System.Net.IPEndPoint(System.Net.IPAddress.Parse("10.0.20.72"), 8087));

            var riakClient = new Riak.Driver.RiakClient(riakSocketClient);

            riakClient.Put(new Riak.Driver.Messages.RpbPutReq
            {
                bucket = Encoding.UTF8.GetBytes("bucket1"),
                key = Encoding.UTF8.GetBytes("key1"),
                content = new Riak.Driver.Messages.RpbContent
                {
                    value = BitConverter.GetBytes(DateTime.UtcNow.Ticks)
                }
            }).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine(Encoding.UTF8.GetString(c.Result.vclock));
            });

            riakClient.Get(new Riak.Driver.Messages.RpbGetReq
            {
                bucket = Encoding.UTF8.GetBytes("bucket1"),
                key = Encoding.UTF8.GetBytes("key1")
            }).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine(BitConverter.ToInt64(c.Result.content[0].value, 0));
            });

            Console.ReadLine();
        }
    }
}