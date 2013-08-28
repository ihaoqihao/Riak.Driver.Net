using System;
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
            Console.WriteLine("put");
            riakClient.Put(new Riak.Driver.RiakObject("bucket1", "key1", "value1")).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine(c.Result.Value.GetString());
            });
            Console.ReadLine();

            //get
            Riak.Driver.RiakObject obj = null;
            Console.WriteLine("get");
            riakClient.Get("bucket1", "key1").ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else if (c.Result == null) Console.WriteLine("key1 not exists!");
                else
                {
                    obj = c.Result;
                    Console.WriteLine(c.Result.Value.GetString());
                }
            });
            Console.ReadLine();

            //index
            Console.WriteLine("index");
            obj.AddIndex("age", 3250);
            riakClient.Put(obj, true).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine(c.Result.Value.GetString());
            });
            Console.ReadLine();

            //index query
            Console.WriteLine("index query");
            riakClient.IndexQuery("bucket1", "age", 0, 10000, 10, null, true).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else
                {
                    foreach (var child in c.Result.Results) Console.WriteLine(child.Key);
                }
            });
            Console.ReadLine();

            //delete
            Console.WriteLine("delete");
            riakClient.Delete("bucket1", "key1").ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine("key1 is deleted");
            });
            Console.ReadLine();

            //set bucket properties
            Console.WriteLine("set bucket properties");
            riakClient.SetBucketProperties("counter1", new Riak.Driver.Messages.RpbBucketProps
            {
                allow_mult = true
            }).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine("set bucket properties completed");
            });
            Console.ReadLine();

            //get bucket properties
            Console.WriteLine("get bucket properties");
            riakClient.GetBucketProperties("counter1").ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine(c.Result.allow_mult);
            });
            Console.ReadLine();

            //inc counter
            Console.WriteLine("inc counter");
            riakClient.Increment("counter1", "key1", 1, true).ContinueWith(c =>
            {
                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                else Console.WriteLine(c.Result.Value);
            });
            Console.ReadLine();

            Console.ReadLine();
        }
    }
}