package queuectl;

import java.time.Instant;

public class Job {

    String id;
    String command;
    String state;
    int attempts;
    int maxRetries;
    Instant runAt;
    Instant createdAt;
    Instant updatedAt;
    String errorMessage;
    String workerId;
    Instant lockedAt;

    @Override
    public String toString() {
        return String.format(
            "Job[ID=%s, State=%s, Attempts=%d, Command=%s]",
            id, state, attempts, command
        );
    }
}