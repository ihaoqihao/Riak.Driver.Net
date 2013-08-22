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
            var riakClient = new Riak.Driver.RiakClient();
            riakClient.RegisterServerNode("1", new System.Net.IPEndPoint(System.Net.IPAddress.Parse("10.0.20.70"), 8087));
            riakClient.RegisterServerNode("2", new System.Net.IPEndPoint(System.Net.IPAddress.Parse("10.0.20.71"), 8087));
            riakClient.RegisterServerNode("3", new System.Net.IPEndPoint(System.Net.IPAddress.Parse("10.0.20.72"), 8087));

            int callback = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            for (int i = 0; i < 1000; i++)
                riakClient.Put("bucket1", "test1", "hello riak!").ContinueWith(c =>
                {
                    var count = System.Threading.Interlocked.Increment(ref callback);
                    // Console.WriteLine(count);
                    if (count == 1000) Console.WriteLine(sw.ElapsedMilliseconds);
                });

            Console.ReadLine();
        }
    }
}