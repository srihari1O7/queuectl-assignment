package queuectl;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Command(name = "queuectl",
    mixinStandardHelpOptions = true,
    version = "queuectl 1.0",
    description = "A CLI-based background job queue system.",
    subcommands = {
        QueueCtl.EnqueueCommand.class,
        QueueCtl.ListCommand.class,
        QueueCtl.WorkerCommand.class,
        QueueCtl.DLQCommand.class,
        QueueCtl.ConfigCommand.class,
        QueueCtl.StatusCommand.class
    })
public class QueueCtl implements Callable<Integer> {

    @Override
    public Integer call() throws Exception {
        System.out.println("QueueCtl is running. Use --help for commands.");
        return 0;
    }

    @Command(name = "enqueue",
        description = "Add a new job to the queue.")
    static class EnqueueCommand implements Callable<Integer> {

        @Parameters(index = "0", description = "The command for the job to execute.")
        private String command;

        @Override
        public Integer call() {
            String jobId = Database.enqueueJob(command);
            if (jobId != null) {
                System.out.println("Job enqueued with ID: " + jobId);
                return 0;
            } else {
                System.err.println("Failed to enqueue job.");
                return 1;
            }
        }
    }

    @Command(name = "list",
        description = "List jobs by state.")
    static class ListCommand implements Callable<Integer> {

        @Option(names = "--state", required = true, description = "Filter by state (e.g., pending, completed, dead)")
        private String state;

        @Override
        public Integer call() {
            List<Job> jobs = Database.getJobsByState(state);
            if (jobs.isEmpty()) {
                System.out.println("No jobs found with state: " + state);
            } else {
                System.out.println("Found " + jobs.size() + " jobs:");
                for (Job job : jobs) {
                    System.out.println("- " + job.toString());
                }
            }
            return 0;
        }
    }

    @Command(name = "worker",
        description = "Start worker processes.")
    static class WorkerCommand implements Callable<Integer> {

        @Option(names = "--count", defaultValue = "1", description = "Number of workers to start.")
        private int count;

        @Override
        public Integer call() {
            System.out.println("Starting " + count + " workers... Press Ctrl+C to stop.");
            ExecutorService executor = Executors.newFixedThreadPool(count);
            List<JobWorker> workers = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                JobWorker worker = new JobWorker(i + 1);
                workers.add(worker);
                executor.submit(worker);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down workers gracefully...");
                for (JobWorker worker : workers) {
                    worker.stop();
                }
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                }
                System.out.println("Workers stopped.");
            }));

            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                System.err.println("Worker pool interrupted.");
            }
            
            return 0;
        }
    }

    @Command(name = "dlq",
        description = "Manage the Dead Letter Queue (DLQ).",
        subcommands = {
            DLQCommand.ListDLQCommand.class,
            DLQCommand.RetryDLQCommand.class
        })
    static class DLQCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("Use 'dlq list' or 'dlq retry'.");
            return 0;
        }

        @Command(name = "list", description = "List all jobs in the DLQ.")
        static class ListDLQCommand implements Callable<Integer> {
            @Override
            public Integer call() {
                List<Job> jobs = Database.getJobsByState("dead");
                if (jobs.isEmpty()) {
                    System.out.println("DLQ is empty.");
                } else {
                    System.out.println("Found " + jobs.size() + " jobs in DLQ:");
                    for (Job job : jobs) {
                        System.out.println("- " + job.toString());
                        if (job.errorMessage != null) {
                            System.out.println("    Error: " + job.errorMessage.replace("\n", "\n    "));
                        }
                    }
                }
                return 0;
            }
        }

        @Command(name = "retry", description = "Retry a specific job from the DLQ.")
        static class RetryDLQCommand implements Callable<Integer> {
            @Parameters(index = "0", description = "The ID of the job to retry.")
            private String jobId;

            @Override
            public Integer call() {
                if (Database.retryJob(jobId)) {
                    System.out.println("Job " + jobId + " moved back to 'pending' queue.");
                    return 0;
                } else {
                    System.err.println("Failed to retry job " + jobId + ". (Is it in the DLQ?)");
                    return 1;
                }
            }
        }
    }
    
    @Command(name = "config",
        description = "Manage configuration settings.",
        subcommands = {
            ConfigCommand.SetCommand.class,
            ConfigCommand.GetCommand.class
        })
    static class ConfigCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("Use 'config set <key> <value>' or 'config get <key>'.");
            return 0;
        }

        @Command(name = "set", description = "Set a config value.")
        static class SetCommand implements Callable<Integer> {
            @Parameters(index = "0", description = "The config key.")
            private String key;
            @Parameters(index = "1", description = "The config value.")
            private String value;

            @Override
            public Integer call() {
                Config.set(key, value);
                System.out.println(key + " = " + value);
                return 0;
            }
        }

        @Command(name = "get", description = "Get a config value.")
        static class GetCommand implements Callable<Integer> {
            @Parameters(index = "0", description = "The config key.")
            private String key;

            @Override
            public Integer call() {
                System.out.println(key + " = " + Config.get(key));
                return 0;
            }
        }
    }
    
    @Command(name = "status",
        description = "Show summary of all job states.")
    static class StatusCommand implements Callable<Integer> {
        @Override
        public Integer call() {
            System.out.println("Job Status Summary");
            Map<String, Integer> counts = Database.getJobCounts();
            
            int pending = counts.getOrDefault("pending", 0);
            int processing = counts.getOrDefault("processing", 0);
            int completed = counts.getOrDefault("completed", 0);
            int dead = counts.getOrDefault("dead", 0);
            
            System.out.println("Pending:    " + pending);
            System.out.println("Processing: " + processing);
            System.out.println("Completed:  " + completed);
            System.out.println("Dead (DLQ): " + dead);
            
            
            return 0;
        }
    }

    public static void main(String[] args) {
        Database.init();
        Database.recoverStaleProcessing(Config.getInt("lock_timeout_seconds", 60));
        
        int exitCode = new CommandLine(new QueueCtl()).execute(args);
        System.exit(exitCode);
    }
}