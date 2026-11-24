using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Kafka_Example_Startup;

internal static class Program
{
    private const string KafkaImage = "apache/kafka:4.4.0";
    private const string ManagedContainerName = "kafka-example-broker";
    private const string SolutionFileName = "Kafka Example.slnx";
    private const string MessagesTopic = "messages-topic";
    private const string ReversedMessagesTopic = "reversed-messages-topic";
    private static readonly TimeSpan BrokerReadyTimeout = TimeSpan.FromSeconds(60);

    private static readonly string[] AppsToLaunch = ["Consumer", "Streamer", "Producer"];

    private static async Task<int> Main(string[] args)
    {
        TrySetConsoleTitle("Kafka Example Startup");

        if (args.Any(a => a.Equals("--help", StringComparison.OrdinalIgnoreCase) || a.Equals("-h", StringComparison.OrdinalIgnoreCase)))
        {
            PrintUsage();
            return 0;
        }

        bool launchApps = !args.Any(a => a.Equals("--no-launch-apps", StringComparison.OrdinalIgnoreCase));
        string bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:9092";
        (string targetHost, int targetPort) = ParseBootstrapHostAndPort(bootstrapServers);

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            Console.WriteLine("=== Kafka Example Startup ===");
            Console.WriteLine($"Bootstrap servers: {bootstrapServers}");
            Console.WriteLine();

            Console.WriteLine("[1/4] Checking Kafka Docker container...");
            if (IsLocalHost(targetHost))
            {
                await EnsureKafkaContainerRunningAsync(targetHost, targetPort, cts.Token);
            }
            else
            {
                Console.WriteLine($"Non-local Kafka host '{targetHost}' configured; skipping local Docker container check.");
            }

            Console.WriteLine();
            Console.WriteLine($"[2/4] Waiting for Kafka broker at {bootstrapServers} to accept connections...");
            await WaitForKafkaReadyAsync(bootstrapServers, BrokerReadyTimeout, cts.Token);

            Console.WriteLine();
            Console.WriteLine("[3/4] Ensuring Kafka topics exist...");
            await EnsureTopicsExistAsync(bootstrapServers, [MessagesTopic, ReversedMessagesTopic], cts.Token);

            Console.WriteLine();
            if (!launchApps)
            {
                Console.WriteLine("[4/4] Skipping console app launch (--no-launch-apps specified).");
            }
            else
            {
                Console.WriteLine("[4/4] Launching Consumer, Streamer, and Producer console applications...");
                await LaunchConsoleAppsAsync(cts.Token);
            }

            Console.WriteLine();
            Console.WriteLine("Startup completed successfully.");
            return 0;
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Startup cancelled by user.");
            return 130;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Startup failed: {ex.Message}");
            return 1;
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Kafka Example Startup");
        Console.WriteLine("Ensures Apache Kafka (apache/kafka:4.4.0) is running in Docker, creates required topics,");
        Console.WriteLine("and launches Consumer, Streamer, and Producer in separate console windows.");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --no-launch-apps   Prepare Docker broker and topics without launching the 3 console apps.");
        Console.WriteLine("  -h, --help         Show this help message.");
        Console.WriteLine();
        Console.WriteLine("Environment variables:");
        Console.WriteLine("  KAFKA_BOOTSTRAP_SERVERS   Kafka bootstrap server (default: localhost:9092).");
    }

    private static (string Host, int Port) ParseBootstrapHostAndPort(string bootstrapServers)
    {
        string firstEndpoint = bootstrapServers
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? "localhost:9092";

        if (firstEndpoint.Contains("://", StringComparison.Ordinal))
        {
            firstEndpoint = firstEndpoint[(firstEndpoint.IndexOf("://", StringComparison.Ordinal) + 3)..];
        }

        int lastColon = firstEndpoint.LastIndexOf(':');
        if (lastColon > 0 && int.TryParse(firstEndpoint[(lastColon + 1)..], out int parsedPort))
        {
            string host = firstEndpoint[..lastColon].Trim('[', ']');
            return (string.IsNullOrWhiteSpace(host) ? "localhost" : host, parsedPort);
        }

        return ("localhost", 9092);
    }

    private static async Task EnsureKafkaContainerRunningAsync(string targetHost, int targetPort, CancellationToken cancellationToken)
    {
        DockerCommandResult dockerInfo = await RunDockerAsync("info --format \"{{.ServerVersion}}\"", cancellationToken);
        if (dockerInfo.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Docker is not available or Docker Desktop is not running. Details: {dockerInfo.CombinedOutput}");
        }

        // 1. Check if any running Kafka container already publishes targetPort to the host.
        DockerCommandResult runningResult = await RunDockerAsync(
            "ps --filter \"status=running\" --format \"{{.Names}}\t{{.Image}}\t{{.Ports}}\"",
            cancellationToken);

        if (runningResult.ExitCode != 0)
        {
            throw new InvalidOperationException($"Failed to query running Docker containers: {runningResult.CombinedOutput}");
        }

        foreach (string line in SplitLines(runningResult.StdOut))
        {
            string[] parts = line.Split('\t');
            if (parts.Length < 3)
            {
                continue;
            }

            string name = parts[0].Trim();
            string image = parts[1].Trim();
            string ports = parts[2].Trim();

            if (image.Contains("kafka", StringComparison.OrdinalIgnoreCase) && PublishesHostPort(ports, targetPort))
            {
                Console.WriteLine($"Reusing running Kafka container '{name}' ({image}) publishing host port {targetPort}.");
                return;
            }
        }

        // 2. Check if our managed container exists in a stopped/exited state (or exists at all).
        DockerCommandResult allResult = await RunDockerAsync(
            $"ps -a --filter \"name={ManagedContainerName}\" --format \"{{{{.Names}}}}\t{{{{.State}}}}\t{{{{.Image}}}}\t{{{{.Ports}}}}\"",
            cancellationToken);

        if (allResult.ExitCode != 0)
        {
            throw new InvalidOperationException($"Failed to inspect existing Docker containers: {allResult.CombinedOutput}");
        }

        foreach (string line in SplitLines(allResult.StdOut))
        {
            string[] parts = line.Split('\t');
            if (parts.Length < 2)
            {
                continue;
            }

            string name = parts[0].Trim();
            string state = parts[1].Trim();
            if (!name.Equals(ManagedContainerName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (state.Equals("running", StringComparison.OrdinalIgnoreCase))
            {
                // Managed container is running but did not publish targetPort.
                throw new InvalidOperationException(
                    $"Container '{ManagedContainerName}' is running without publishing host port {targetPort}. " +
                    $"Stop and remove it (`docker rm -f {ManagedContainerName}`) and re-run Startup.");
            }

            Console.WriteLine($"Found existing stopped container '{ManagedContainerName}' (state: {state}). Starting it...");
            DockerCommandResult startResult = await RunDockerAsync($"start {ManagedContainerName}", cancellationToken);
            if (startResult.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Failed to start container '{ManagedContainerName}': {startResult.CombinedOutput}");
            }

            Console.WriteLine($"Started Kafka container '{ManagedContainerName}'.");
            return;
        }

        // 3. Create and start a new managed Kafka container publishing targetPort -> 9092.
        Console.WriteLine($"No running Kafka container publishes host port {targetPort}.");
        Console.WriteLine($"Starting new container '{ManagedContainerName}' from image '{KafkaImage}' on port {targetPort}...");

        string runArgs = targetPort == 9092
            ? $"run -d --name {ManagedContainerName} -p 9092:9092 {KafkaImage}"
            : $"run -d --name {ManagedContainerName} -p {targetPort}:9092 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{targetHost}:{targetPort} {KafkaImage}";

        DockerCommandResult runResult = await RunDockerAsync(runArgs, cancellationToken);
        if (runResult.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Failed to start Kafka container '{ManagedContainerName}': {runResult.CombinedOutput}");
        }

        string containerId = runResult.StdOut.Trim();
        string shortId = containerId.Length > 12 ? containerId[..12] : containerId;
        Console.WriteLine($"Started new Kafka container '{ManagedContainerName}' (ID: {shortId}).");
    }

    private static bool PublishesHostPort(string portsColumn, int hostPort)
    {
        if (string.IsNullOrWhiteSpace(portsColumn))
        {
            return false;
        }

        // Matches published mappings like "0.0.0.0:9092->9092/tcp", "[::]:9092->9092/tcp", "127.0.0.1:9092->9092/tcp",
        // while ignoring unpublished exposed ports like "9092/tcp".
        return Regex.IsMatch(portsColumn, $@"(?:^|,\s*)(?:[\d\.\*\[\]:]+)?:{hostPort}->\d+/tcp");
    }

    private static async Task WaitForKafkaReadyAsync(string bootstrapServers, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers,
            SocketTimeoutMs = 3000,
            ReconnectBackoffMs = 500,
            ReconnectBackoffMaxMs = 1000
        };

        using IAdminClient adminClient = new AdminClientBuilder(adminConfig)
            .SetLogHandler((_, _) => { })
            .SetErrorHandler((_, _) => { })
            .Build();

        Stopwatch stopwatch = Stopwatch.StartNew();
        DateTime lastProgressLog = DateTime.MinValue;
        string? lastError = null;

        while (stopwatch.Elapsed < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                Metadata metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(3));
                if (metadata.Brokers.Count > 0)
                {
                    Console.WriteLine(
                        $"Kafka broker is ready at {bootstrapServers} ({metadata.Brokers.Count} broker(s) available, elapsed {stopwatch.Elapsed.TotalSeconds:F1}s).");
                    return;
                }
            }
            catch (KafkaException ex)
            {
                lastError = ex.Error.Reason;
            }
            catch (Exception ex)
            {
                lastError = ex.Message;
            }

            if ((DateTime.UtcNow - lastProgressLog).TotalSeconds >= 4)
            {
                Console.WriteLine($"Waiting for Kafka broker... ({stopwatch.Elapsed.TotalSeconds:F0}s / {timeout.TotalSeconds:F0}s)");
                lastProgressLog = DateTime.UtcNow;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
        }

        throw new TimeoutException(
            $"Timed out after {timeout.TotalSeconds:F0}s waiting for Kafka broker at {bootstrapServers} to become ready. " +
            $"Last error: {lastError ?? "No brokers returned in metadata."}");
    }

    private static async Task EnsureTopicsExistAsync(
        string bootstrapServers,
        IEnumerable<string> topicNames,
        CancellationToken cancellationToken)
    {
        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        };

        using IAdminClient adminClient = new AdminClientBuilder(adminConfig).Build();

        foreach (string topicName in topicNames)
        {
            await EnsureSingleTopicExistsAsync(adminClient, topicName, cancellationToken);
        }
    }

    private static async Task EnsureSingleTopicExistsAsync(
        IAdminClient adminClient,
        string topicName,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 5;

        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var specification = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = 1,
                ReplicationFactor = 1
            };

            try
            {
                await adminClient.CreateTopicsAsync(
                    [specification],
                    new CreateTopicsOptions
                    {
                        RequestTimeout = TimeSpan.FromSeconds(15),
                        OperationTimeout = TimeSpan.FromSeconds(15)
                    });

                Console.WriteLine($"Topic '{topicName}' created (partitions: 1, replication factor: 1).");
                return;
            }
            catch (CreateTopicsException ex)
            {
                CreateTopicReport? report = ex.Results.FirstOrDefault(r =>
                    r.Topic.Equals(topicName, StringComparison.Ordinal));

                if (report?.Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    Console.WriteLine($"Topic '{topicName}' already exists. Skipping creation.");
                    return;
                }

                bool isTransient = report is not null && IsTransientTopicCreationError(report.Error.Code);
                if (isTransient && attempt < maxAttempts)
                {
                    Console.WriteLine(
                        $"Transient error creating topic '{topicName}' ({report!.Error.Reason}). Retrying ({attempt}/{maxAttempts})...");
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                    continue;
                }

                string reason = report?.Error.Reason ?? ex.Message;
                throw new InvalidOperationException($"Failed to create Kafka topic '{topicName}': {reason}", ex);
            }
        }
    }

    private static bool IsTransientTopicCreationError(ErrorCode code) => code is
        ErrorCode.BrokerNotAvailable or
        ErrorCode.NotController or
        ErrorCode.LeaderNotAvailable or
        ErrorCode.NetworkException or
        ErrorCode.RequestTimedOut or
        ErrorCode.Local_Transport or
        ErrorCode.Local_TimedOut;

    private static async Task LaunchConsoleAppsAsync(CancellationToken cancellationToken)
    {
        string solutionDirectory = FindSolutionDirectory();
        Console.WriteLine($"Solution directory: {solutionDirectory}");

        for (int i = 0; i < AppsToLaunch.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            string appName = AppsToLaunch[i];
            LaunchConsoleApp(solutionDirectory, appName);

            if (i < AppsToLaunch.Length - 1)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(1000), cancellationToken);
            }
        }
    }

    private static string FindSolutionDirectory()
    {
        string? found = WalkUpToSolution(AppContext.BaseDirectory)
            ?? WalkUpToSolution(Directory.GetCurrentDirectory());

        if (found is null)
        {
            throw new DirectoryNotFoundException(
                $"Could not locate '{SolutionFileName}' starting from '{AppContext.BaseDirectory}' or '{Directory.GetCurrentDirectory()}'.");
        }

        return found;
    }

    private static string? WalkUpToSolution(string startPath)
    {
        var directory = new DirectoryInfo(startPath);
        while (directory is not null)
        {
            string candidate = Path.Combine(directory.FullName, SolutionFileName);
            if (File.Exists(candidate))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }

    private static void LaunchConsoleApp(string solutionDirectory, string projectName)
    {
        string projectDirectory = Path.Combine(solutionDirectory, projectName);
        string csprojPath = Path.Combine(projectDirectory, $"{projectName}.csproj");

        if (!File.Exists(csprojPath))
        {
            throw new FileNotFoundException($"Could not find project file for '{projectName}'.", csprojPath);
        }

        string? executablePath = FindUpToDateExecutable(projectDirectory, projectName, csprojPath);

        ProcessStartInfo startInfo;
        string launchDescription;

        if (executablePath is not null)
        {
            startInfo = new ProcessStartInfo
            {
                FileName = executablePath,
                WorkingDirectory = projectDirectory,
                UseShellExecute = true
            };
            launchDescription = executablePath;
        }
        else
        {
            startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = $"run --project \"{csprojPath}\"",
                WorkingDirectory = projectDirectory,
                UseShellExecute = true
            };
            launchDescription = $"dotnet run --project \"{csprojPath}\"";
        }

        using Process? process = Process.Start(startInfo);
        if (process is null)
        {
            throw new InvalidOperationException($"Failed to launch '{projectName}' ({launchDescription}).");
        }

        Console.WriteLine($"Launched '{projectName}' in a new console window (PID: {process.Id}, via {launchDescription}).");
    }

    private static bool IsLocalHost(string host) =>
        host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
        host.Equals("127.0.0.1", StringComparison.Ordinal) ||
        host.Equals("::1", StringComparison.Ordinal) ||
        host.Equals("0.0.0.0", StringComparison.Ordinal);

    private static string? FindUpToDateExecutable(string projectDirectory, string projectName, string csprojPath)
    {
        string preferredConfig =
#if DEBUG
            "Debug";
#else
            "Release";
#endif
        string fallbackConfig = preferredConfig == "Debug" ? "Release" : "Debug";
        string exeFileName = OperatingSystem.IsWindows() ? $"{projectName}.exe" : projectName;

        string[] candidateConfigs = [preferredConfig, fallbackConfig];
        DateTime latestSourceUtc = GetLatestSourceWriteTimeUtc(projectDirectory, csprojPath);

        foreach (string config in candidateConfigs)
        {
            string candidatePath = Path.Combine(projectDirectory, "bin", config, "net10.0", exeFileName);
            if (File.Exists(candidatePath) && File.GetLastWriteTimeUtc(candidatePath) >= latestSourceUtc)
            {
                return candidatePath;
            }
        }

        return null;
    }

    private static DateTime GetLatestSourceWriteTimeUtc(string projectDirectory, string csprojPath)
    {
        DateTime latest = File.GetLastWriteTimeUtc(csprojPath);

        foreach (string file in Directory.EnumerateFiles(projectDirectory, "*.cs", SearchOption.AllDirectories))
        {
            string relative = Path.GetRelativePath(projectDirectory, file);
            if (relative.StartsWith($"bin{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase) ||
                relative.StartsWith($"obj{Path.DirectorySeparatorChar}", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            DateTime writeTime = File.GetLastWriteTimeUtc(file);
            if (writeTime > latest)
            {
                latest = writeTime;
            }
        }

        return latest;
    }

    private static async Task<DockerCommandResult> RunDockerAsync(string arguments, CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = "docker",
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };

        using var process = new Process { StartInfo = startInfo };
        try
        {
            process.Start();
        }
        catch (Exception ex)
        {
            return new DockerCommandResult(-1, string.Empty, $"Failed to start 'docker' CLI: {ex.Message}");
        }

        Task<string> stdOutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        Task<string> stdErrTask = process.StandardError.ReadToEndAsync(cancellationToken);

        await process.WaitForExitAsync(cancellationToken);

        string stdOut = await stdOutTask;
        string stdErr = await stdErrTask;

        return new DockerCommandResult(process.ExitCode, stdOut, stdErr);
    }

    private static IEnumerable<string> SplitLines(string text) =>
        text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private static void TrySetConsoleTitle(string title)
    {
        try
        {
            Console.Title = title;
        }
        catch
        {
            // Ignore when stdout is redirected or console does not support setting title.
        }
    }

    private sealed record DockerCommandResult(int ExitCode, string StdOut, string StdErr)
    {
        public string CombinedOutput => string.IsNullOrWhiteSpace(StdErr) ? StdOut.Trim() : StdErr.Trim();
    }
}
