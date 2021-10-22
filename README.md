# mesg

Simple message broker with GRPC protocol


Easy contract
```cs
//Full proto-file availabile at running application endpoint: http://localhost:35001/proto
service MesgProtocol {
  rpc Push (PushRequest) returns (PushResponse) {}
  rpc Pull (PullRequest) returns (stream PullResponse) {}
  rpc Commit (CommitRequest) returns (CommitResponse) {}
}
```

Genereate GRPC client for your language by single .proto file and use it


```csharp

string queueName =  "test_queue";

var stream = client.Pull(new PullRequest
{
    Queue = queueName
}).ResponseStream;

while (await stream.MoveNext(CancellationToken.None))
{
    try 
    {
        await ProcessAsync(stream.Current.Data);

        await client.Commit(queueName, stream.Current.MessageId);
    }
    catch(Exception ex){

    }    
 }
```

###TODO

- [ ]  Persistence with own storage engine
- [ ]  Sharding and replication

### License

Apache License 2.0