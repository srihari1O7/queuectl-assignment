package queuectl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class JobWorker implements Runnable {

    private volatile boolean running = true;
    private final String id;
    private final long staleRecoverySeconds = Config.getInt("lock_timeout_seconds", 60);

    public JobWorker(int id) {
        this.id = "worker-" + id;
    }

    @Override
    public void run() {
        System.out.println("Worker " + id + " starting.");
        Database.recoverStaleProcessing(staleRecoverySeconds);

        while (running) {
            Job job = Database.findAndLockJob(id);

            if (job != null) {
                System.out.println("Worker " + id + " processing job: " + job.id + " (attempt " + job.attempts + ")");
                executeJob(job);
            } else {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    running = false;
                }
            }
        }
        System.out.println("Worker " + id + " shutting down.");
    }

    private void executeJob(Job job) {
        try {
            String[] cmd;
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("win")) {
                cmd = new String[]{"cmd", "/c", job.command};
            } else {
                cmd = new String[]{"sh", "-c", job.command};
            }

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process process = pb.start();

            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            int timeoutSec = Config.getInt("job_timeout_seconds", 300);
            boolean finished = process.waitFor(timeoutSec, TimeUnit.SECONDS);
            int exitCode;
            if (!finished) {
                process.destroyForcibly();
                exitCode = -1;
            } else {
                exitCode = process.exitValue();
            }

            String log = output.toString().trim();

            if (exitCode == 0) {
                Database.markJobCompleted(job.id);
                System.out.println("Worker " + id + " completed job: " + job.id);
            } else {
                handleFailedJob(job, "Exit code: " + exitCode + "\nOutput: " + log);
            }

        } catch (Exception e) {
            handleFailedJob(job, "Worker exception: " + e.getMessage());
        }
    }

    private void handleFailedJob(Job job, String error) {
        int newAttempts = job.attempts;
        
        if (newAttempts >= job.maxRetries) {
            Database.markJobDead(job.id, error);
            System.err.println("Worker " + id + " moved job to DLQ: " + job.id);
        } else {
            int base = Config.getInt("backoff_base", 2);
            long backoffSeconds = (long) Math.pow(base, newAttempts);
            Instant newRunAt = Instant.now().plusSeconds(backoffSeconds);
            Database.markJobFailed(job.id, error, newAttempts, newRunAt);
            System.err.println("Worker " + id + " failed job: " + job.id + ". Retrying in " + backoffSeconds + "s.");
        }
    }

    public void stop() {
        running = false;
    }
}