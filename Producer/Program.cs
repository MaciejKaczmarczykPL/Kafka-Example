using Confluent.Kafka;

const string TopicName = "messages-topic";

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");
if (string.IsNullOrWhiteSpace(bootstrapServers))
{
    bootstrapServers = "localhost:9092";
}

try
{
    Console.Title = "Kafka Producer";
}
catch
{
    // Ignore when running with redirected or headless console handles.
}

Console.WriteLine("=== Kafka Producer ===");
Console.WriteLine($"Broker : {bootstrapServers}");
Console.WriteLine($"Topic  : {TopicName}");
Console.WriteLine("Type a message and press Enter to send. Press Ctrl+C (or Ctrl+Z then Enter) to exit.");
Console.WriteLine();

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var config = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    MessageTimeoutMs = 10000
};

using var producer = new ProducerBuilder<Null, string>(config).Build();

try
{
    while (!cts.IsCancellationRequested)
    {
        if (!Console.IsInputRedirected)
        {
            Console.Write("> ");
        }

        string? line = Console.ReadLine();
        if (line is null)
        {
            break;
        }

        if (string.IsNullOrWhiteSpace(line))
        {
            continue;
        }

        try
        {
            var result = await producer.ProduceAsync(
                TopicName,
                new Message<Null, string> { Value = line },
                cts.Token);

            Console.WriteLine(
                $"Sent \"{line}\" -> {result.Topic} [partition {result.Partition.Value}, offset {result.Offset.Value}]");
        }
        catch (ProduceException<Null, string> ex)
        {
            Console.Error.WriteLine($"Failed to send message: {ex.Error.Reason}");
        }
        catch (OperationCanceledException)
        {
            break;
        }
    }
}
finally
{
    Console.WriteLine("Shutting down producer and flushing pending messages...");
    producer.Flush(TimeSpan.FromSeconds(5));
}

Console.WriteLine("Producer stopped.");
