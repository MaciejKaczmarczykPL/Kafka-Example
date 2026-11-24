using System.Globalization;
using System.Text;
using Confluent.Kafka;

const string InputTopic = "messages-topic";
const string OutputTopic = "reversed-messages-topic";

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");
if (string.IsNullOrWhiteSpace(bootstrapServers))
{
    bootstrapServers = "localhost:9092";
}

var groupId = Environment.GetEnvironmentVariable("KAFKA_GROUP_ID");
if (string.IsNullOrWhiteSpace(groupId))
{
    groupId = "streamer-group";
}

try
{
    Console.Title = "Kafka Streamer";
}
catch
{
    // Console title may not be supported when output is redirected.
}

try
{
    Console.OutputEncoding = Encoding.UTF8;
}
catch
{
    // Ignore if console output encoding cannot be changed.
}

Console.WriteLine("=== Kafka Streamer ===");
Console.WriteLine($"Bootstrap Servers : {bootstrapServers}");
Console.WriteLine($"Consumer Group    : {groupId}");
Console.WriteLine($"Input Topic       : {InputTopic}");
Console.WriteLine($"Output Topic      : {OutputTopic}");
Console.WriteLine("Press Ctrl+C to exit.");
Console.WriteLine();

using var cts = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine();
    Console.WriteLine("Shutdown requested. Closing consumer and flushing producer...");
    try
    {
        cts.Cancel();
    }
    catch (ObjectDisposedException)
    {
        // Shutdown already completed.
    }
};
Console.CancelKeyPress += cancelHandler;

var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = groupId,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrapServers,
    Acks = Acks.All
};

using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig)
    .SetErrorHandler((_, error) => Console.Error.WriteLine($"[Kafka Consumer Error] {error.Reason}"))
    .Build();

using var producer = new ProducerBuilder<Null, string>(producerConfig)
    .SetErrorHandler((_, error) => Console.Error.WriteLine($"[Kafka Producer Error] {error.Reason}"))
    .Build();

consumer.Subscribe(InputTopic);
Console.WriteLine($"Subscribed to '{InputTopic}'. Waiting for messages...");

try
{
    while (!cts.Token.IsCancellationRequested)
    {
        ConsumeResult<Ignore, string>? consumeResult;
        try
        {
            consumeResult = consumer.Consume(cts.Token);
        }
        catch (ConsumeException ex)
        {
            Console.Error.WriteLine($"[Consume Error] {ex.Error.Reason}");
            await DelaySafelyAsync(TimeSpan.FromMilliseconds(500), cts.Token);
            continue;
        }

        if (consumeResult?.Message is null)
        {
            continue;
        }

        var originalMessage = consumeResult.Message.Value;
        if (originalMessage is null)
        {
            Console.WriteLine(
                $"[{DateTime.Now:HH:mm:ss}] Skipping null message at partition {consumeResult.Partition.Value}, offset {consumeResult.Offset.Value}.");
            TryCommit(consumer, consumeResult);
            continue;
        }

        var reversedMessage = ReverseTextElements(originalMessage);

        try
        {
            var deliveryResult = await producer.ProduceAsync(
                OutputTopic,
                new Message<Null, string> { Value = reversedMessage },
                cts.Token);

            TryCommit(consumer, consumeResult);

            Console.WriteLine(
                $"[{DateTime.Now:HH:mm:ss}] Received: \"{originalMessage}\" -> Reversed: \"{reversedMessage}\" " +
                $"(in: p{consumeResult.Partition.Value}@{consumeResult.Offset.Value}, " +
                $"out: p{deliveryResult.Partition.Value}@{deliveryResult.Offset.Value})");
        }
        catch (ProduceException<Null, string> ex)
        {
            Console.Error.WriteLine(
                $"[Produce Error] Failed to publish reversed message for offset {consumeResult.Offset.Value}: {ex.Error.Reason}");

            try
            {
                consumer.Seek(consumeResult.TopicPartitionOffset);
            }
            catch (KafkaException seekEx)
            {
                Console.Error.WriteLine($"[Seek Error] {seekEx.Error.Reason}");
            }

            await DelaySafelyAsync(TimeSpan.FromSeconds(1), cts.Token);
        }
    }
}
catch (OperationCanceledException)
{
    // Expected when Ctrl+C triggers cancellation.
}
finally
{
    Console.CancelKeyPress -= cancelHandler;

    try
    {
        consumer.Close();
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Shutdown] Error closing consumer: {ex.Message}");
    }

    try
    {
        producer.Flush(TimeSpan.FromSeconds(5));
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"[Shutdown] Error flushing producer: {ex.Message}");
    }

    Console.WriteLine("Kafka Streamer stopped.");
}

static string ReverseTextElements(string input)
{
    if (string.IsNullOrEmpty(input))
    {
        return input;
    }

    var enumerator = StringInfo.GetTextElementEnumerator(input);
    var elements = new List<string>(input.Length);
    while (enumerator.MoveNext())
    {
        elements.Add(enumerator.GetTextElement());
    }

    elements.Reverse();
    return string.Concat(elements);
}

static void TryCommit(IConsumer<Ignore, string> consumer, ConsumeResult<Ignore, string> consumeResult)
{
    try
    {
        consumer.Commit(consumeResult);
    }
    catch (KafkaException ex)
    {
        Console.Error.WriteLine(
            $"[Commit Error] Failed to commit offset {consumeResult.Offset.Value}: {ex.Error.Reason}");
    }
}

static async Task DelaySafelyAsync(TimeSpan delay, CancellationToken cancellationToken)
{
    try
    {
        await Task.Delay(delay, cancellationToken);
    }
    catch (OperationCanceledException)
    {
        // Shutdown requested during backoff delay.
    }
}
