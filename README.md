riak.driver.net
===============

Riak.Driver.Net
```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>

  <configSections>
    <section name="riak"
             type="Riak.Driver.Config.RiakConfigSection, Riak.Driver"/>
  </configSections>

  <riak>
    <client>
      <endpoint name="riak1"
                socketBufferSize="8192"
                messageBufferSize="8192"
                millisecondsSendTimeout="3000"
                millisecondsReceiveTimeout="3000"
                maxConnectionPoolSize="30">
        <servers>
          <!--put you server here-->
          <server host="10.0.20.70" port="8087" />
          <server host="10.0.20.71" port="8087" />
          <server host="10.0.20.72" port="8087" />
        </servers>
      </endpoint>
    </client>
  </riak>

</configuration>
```

```csharp
static void Main(string[] args)
{
    System.Threading.ThreadPool.SetMinThreads(30, 30);
    Sodao.FastSocket.SocketBase.Log.Trace.EnableConsole();

    var riakClient = Riak.Driver.RiakClientPool.Get("riak.config", "riak1");

    riakClient.Put(new Riak.Driver.RiakObject("bucket1", "key1", "value1"), options => options.SetW(1).SetReturnBody(true)).ContinueWith(c =>
    {
        if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
        else Console.WriteLine(c.Result.Value.GetString());
    });

    riakClient.Get("bucket1", new string[] { "key1", "key2" }, options => options.SetR(1)).ContinueWith(c =>
    {
        if (c.IsFaulted) Console.WriteLine(c.Exception.ToString());
        else Console.WriteLine(c.Result.Length);
    });
    Console.ReadLine();
}
```
