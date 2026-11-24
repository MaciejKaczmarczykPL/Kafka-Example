using Confluent.Kafka;

try
{
    Console.Title = "Kafka Consumer";
}
catch
{
    // Ignore when console title cannot be set (e.g., redirected console).
}

const string topic = "reversed-messages-topic";

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");
if (string.IsNullOrWhiteSpace(bootstrapServers))
{
    bootstrapServers = "localhost:9092";
}

var groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID");
if (string.IsNullOrWhiteSpace(groupId))
{
    groupId = "consumer-group";
}

Console.WriteLine("Kafka Consumer started.");
Console.WriteLine($"Broker : {bootstrapServers}");
Console.WriteLine($"Topic  : {topic}");
Console.WriteLine($"GroupId: {groupId}");
Console.WriteLine("Listening for messages (press Ctrl+C to exit)...");

var config = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true
};

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

using var consumer = new ConsumerBuilder<Ignore, string>(config)
    .SetErrorHandler((_, error) =>
    {
        Console.Error.WriteLine($"Kafka error ({error.Code}): {error.Reason}");
    })
    .Build();

consumer.Subscribe(topic);

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        try
        {
            var consumeResult = consumer.Consume(cts.Token);
            if (consumeResult?.Message is not null)
            {
                Console.WriteLine($"Received: {consumeResult.Message.Value}");
            }
        }
        catch (ConsumeException ex)
        {
            Console.Error.WriteLine($"Consume error ({ex.Error.Code}): {ex.Error.Reason}");
            if (ex.Error.IsFatal)
            {
                break;
            }

            // Brief backoff to avoid tight-looping on transient errors (e.g., topic not yet created).
            cts.Token.WaitHandle.WaitOne(TimeSpan.FromMilliseconds(500));
        }
    }
}
catch (OperationCanceledException)
{
    // Expected on Ctrl+C shutdown.
}
finally
{
    consumer.Close();
    Console.WriteLine("Kafka Consumer stopped.");
}
