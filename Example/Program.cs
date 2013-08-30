using System;
using Riak.Driver.Utils;
using System.Linq;

namespace Example
{
    class Program
    {
        static void Main(string[] args)
        {
            var riakClient = Riak.Driver.RiakClientPool.Get("riak.config", "riak1");

            long count = 0;
            var threads = 0;
            while (threads++ < 2)
            {
                new System.Threading.Thread(_ =>
                {
                    while (true)
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            string key = Guid.NewGuid().ToString();
                            riakClient.Put(new Riak.Driver.RiakObject("bucket1", key, key), true).ContinueWith(c =>
                            {
                                System.Threading.Interlocked.Increment(ref count);
                                if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
                                else
                                {
                                    System.Threading.Interlocked.Increment(ref count);
                                    riakClient.Get("bucket1", key).ContinueWith(t =>
                                    {
                                        if (t.IsFaulted) Console.WriteLine(t.Exception.ToString());
                                        else
                                        {
                                            if (t.Result.Value.GetString() != key) Console.WriteLine(key);
                                        }
                                    });
                                }
                            });
                        }
                        System.Threading.Thread.Sleep(1000);
                    }
                })
                {
                    IsBackground = true
                }.Start();
            }

            new System.Threading.Thread(_ =>
            {
                while (true)
                {
                    Console.Title = System.Threading.Thread.VolatileRead(ref count).ToString();
                    System.Threading.Thread.Sleep(100);
                }
            })
            {
                IsBackground = true
            }.Start();

            Console.ReadLine();
        }
    }
}