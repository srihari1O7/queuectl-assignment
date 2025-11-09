package queuectl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Database {

    private static final String DB_URL = "jdbc:sqlite:queue.db";

    public static Connection getConnection() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL);
        try (Statement s = conn.createStatement()) {
            s.execute("PRAGMA foreign_keys = ON");
        } catch (SQLException ignored) {}
        return conn;
    }

    public static void init() {
        String sql = """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                run_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                error_message TEXT,
                worker_id TEXT,
                locked_at DATETIME
            );
            """;

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute("PRAGMA journal_mode = WAL");
            stmt.execute(sql);

            try { stmt.execute("ALTER TABLE jobs ADD COLUMN worker_id TEXT"); } catch (SQLException ignored) {}
            try { stmt.execute("ALTER TABLE jobs ADD COLUMN locked_at DATETIME"); } catch (SQLException ignored) {}

        } catch (SQLException e) {
            System.err.println("Database init error: " + e.getMessage());
        }
    }

    public static String enqueueJob(String command) {
        String jobId = UUID.randomUUID().toString();
        Instant now = Instant.now();
        int maxRetries = Config.getInt("max_retries", 3);

        String sql = """
            INSERT INTO jobs (id, command, state, max_retries, run_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """;

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, jobId);
            pstmt.setString(2, command);
            pstmt.setString(3, "pending");
            pstmt.setInt(4, maxRetries);
            pstmt.setString(5, now.toString());
            pstmt.setString(6, now.toString());
            pstmt.setString(7, now.toString());

            pstmt.executeUpdate();
            return jobId;

        } catch (SQLException e) {
            System.err.println("Error enqueuing job: " + e.getMessage());
            return null;
        }
    }

    public static Job findAndLockJob(String workerId) {
        String selectSql = "SELECT id FROM jobs WHERE state = 'pending' AND run_at <= ? ORDER BY created_at LIMIT 1";
        String updateSql = "UPDATE jobs SET state = 'processing', attempts = attempts + 1, worker_id = ?, locked_at = ?, updated_at = ? WHERE id = ? AND state = 'pending'";
        String fetchSql = "SELECT * FROM jobs WHERE id = ?";

        Instant now = Instant.now();
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement sel = conn.prepareStatement(selectSql)) {
                sel.setString(1, now.toString());
                ResultSet rs = sel.executeQuery();
                if (!rs.next()) {
                    conn.commit();
                    return null;
                }
                String id = rs.getString("id");

                try (PreparedStatement upd = conn.prepareStatement(updateSql)) {
                    upd.setString(1, workerId);
                    upd.setString(2, now.toString());
                    upd.setString(3, now.toString());
                    upd.setString(4, id);
                    int updated = upd.executeUpdate();
                    if (updated != 1) {
                        conn.rollback();
                        return null;
                    }
                }

                Job job;
                try (PreparedStatement fetch = conn.prepareStatement(fetchSql)) {
                    fetch.setString(1, id);
                    ResultSet r2 = fetch.executeQuery();
                    if (!r2.next()) {
                        conn.rollback();
                        return null;
                    }
                    job = mapRowToJob(r2);
                }

                conn.commit();
                return job;
            } catch (SQLException ex) {
                conn.rollback();
                throw ex;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            System.err.println("Error finding/locking job: " + e.getMessage());
            return null;
        }
    }

    public static void markJobCompleted(String jobId) {
        String sql = "UPDATE jobs SET state = 'completed', updated_at = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, Instant.now().toString());
            pstmt.setString(2, jobId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error marking job completed: " + e.getMessage());
        }
    }

    public static void markJobDead(String jobId, String error) {
        String sql = "UPDATE jobs SET state = 'dead', updated_at = ?, error_message = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, Instant.now().toString());
            pstmt.setString(2, error);
            pstmt.setString(3, jobId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error marking job dead: " + e.getMessage());
        }
    }

    public static void markJobFailed(String jobId, String error, int attempts, Instant newRunAt) {
        String sql = "UPDATE jobs SET state = 'pending', updated_at = ?, error_message = ?, attempts = ?, run_at = ? WHERE id = ?";
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, Instant.now().toString());
            pstmt.setString(2, error);
            pstmt.setInt(3, attempts);
            pstmt.setString(4, newRunAt.toString());
            pstmt.setString(5, jobId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error marking job failed: " + e.getMessage());
        }
    }

    public static boolean retryJob(String jobId) {
        String sql = "UPDATE jobs SET state = 'pending', attempts = 0, run_at = ?, updated_at = ? WHERE id = ? AND state = 'dead'";
        Instant now = Instant.now();

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, now.toString());
            pstmt.setString(2, now.toString());
            pstmt.setString(3, jobId);

            int rows = pstmt.executeUpdate();
            return rows > 0;
        } catch (SQLException e) {
            System.err.println("Error retrying job: " + e.getMessage());
            return false;
        }
    }

    public static Map<String, Integer> getJobCounts() {
        Map<String, Integer> counts = new HashMap<>();
        String sql = "SELECT state, COUNT(*) as count FROM jobs GROUP BY state";

        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                counts.put(rs.getString("state"), rs.getInt("count"));
            }
        } catch (SQLException e) {
            System.err.println("Error getting job counts: " + e.getMessage());
        }
        return counts;
    }

    public static List<Job> getJobsByState(String state) {
        List<Job> jobs = new ArrayList<>();
        String sql = "SELECT * FROM jobs WHERE state = ?";

        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, state);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                jobs.add(mapRowToJob(rs));
            }

        } catch (SQLException e) {
            System.err.println("Error getting jobs by state: " + e.getMessage());
        }
        return jobs;
    }

    public static void recoverStaleProcessing(long staleSeconds) {
        String sql = "UPDATE jobs SET state = 'pending', worker_id = NULL, locked_at = NULL, updated_at = ? WHERE state = 'processing' AND locked_at <= ?";
        Instant cutoff = Instant.now().minusSeconds(staleSeconds);
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, Instant.now().toString());
            pstmt.setString(2, cutoff.toString());
            int rows = pstmt.executeUpdate();
            if (rows > 0) {
                System.out.println("Recovered " + rows + " stale processing jobs.");
            }
        } catch (SQLException e) {
            System.err.println("Error recovering stale jobs: " + e.getMessage());
        }
    }

    private static Job mapRowToJob(ResultSet rs) throws SQLException {
        Job job = new Job();
        job.id = rs.getString("id");
        job.command = rs.getString("command");
        job.state = rs.getString("state");
        job.attempts = rs.getInt("attempts");
        job.maxRetries = rs.getInt("max_retries");
        job.runAt = Instant.parse(rs.getString("run_at"));
        job.createdAt = Instant.parse(rs.getString("created_at"));
        job.updatedAt = Instant.parse(rs.getString("updated_at"));
        job.errorMessage = rs.getString("error_message");
        job.workerId = rs.getString("worker_id");
        job.lockedAt = rs.getString("locked_at") != null ? Instant.parse(rs.getString("locked_at")) : null;
        return job;
    }
}