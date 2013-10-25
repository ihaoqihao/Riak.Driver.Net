using System;
using Riak.Driver.Utils;

namespace Example
{
    class Program
    {
        static void Main(string[] args)
        {
            System.Threading.ThreadPool.SetMinThreads(30, 30);
            Sodao.FastSocket.SocketBase.Log.Trace.EnableConsole();

            var riakClient = Riak.Driver.RiakClientPool.Get("riak.config", "riak1");

            int snetCount = 0;
            int receivedCount = 0;
            var sw = System.Diagnostics.Stopwatch.StartNew();

            for (int i = 0; i < 100000; i++)
            {
                if (System.Threading.Interlocked.Increment(ref snetCount) % 1000 == 0) System.Threading.Thread.Sleep(400);

                string key = i.ToString();
                string value = Guid.NewGuid().ToString();
                riakClient.Put(new Riak.Driver.RiakObject("bucket1", key, value)).ContinueWith(c =>
                {
                    if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());

                    riakClient.Get("bucket1", key).ContinueWith(c2 =>
                    {
                        if (c2.IsFaulted) Console.WriteLine(c.Exception.ToString());
                        else
                        {
                            if (c2.Result.Value.GetString() != value)
                                Console.WriteLine("value not match");
                        }

                        if (System.Threading.Interlocked.Increment(ref receivedCount) == 100000)
                            Console.WriteLine("end.." + sw.Elapsed.TotalSeconds.ToString());
                    });
                });
            }
            Console.ReadLine();
        }
    }
}