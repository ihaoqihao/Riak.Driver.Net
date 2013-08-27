using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Riak.Driver.Utils;

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

            //put
            riakClient.Put(new Riak.Driver.RiakObject("bucket1", "key1", "value1"), true).ContinueWith(c =>
            {
                Console.WriteLine(c.Result.Value.GetString());
            });
            Console.ReadLine();
        }
    }
}