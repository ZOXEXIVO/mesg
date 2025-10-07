# mesg

Simple message broker with GRPC protocol


Easy contract
```cs
//Full proto-file availabile at running application endpoint: http://localhost:35001/proto
service MesgProtocol {
  rpc Push (PushRequest) returns (PushResponse) {}
  rpc Pull (PullRequest) returns (stream PullResponse) {}
  rpc Commit (CommitRequest) returns (CommitResponse) {}
  rpc Rollback (RollbackRequest) returns (RollbackResponse) {}
}
```

Genereate GRPC client for your language by single .proto file and use it


```csharp

string queueName =  "test_queue";

var stream = client.Pull(new PullRequest
{
    Queue = queueName,
    Application = Environment.ComputerName
}).ResponseStream;

while (await stream.MoveNext(CancellationToken.None))
{
    try 
    {
        await ProcessAsync(stream.Current.Data);

        await client.Commit(queueName, stream.Current.MessageId);
    }
    finally
    {
        await client.Rollback(queueName, stream.Current.MessageId);
    }    
 }
```

### TODO

- [x]  Broadcast push
- [ ]  Persistence
- [ ]  Sharding and replication

### License

Apache License 2.0
