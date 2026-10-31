import time
from contextlib import closing

from alphavantage_trackers.paths import API_TRACKER_PATH
from alphavantage_trackers.api.quota_storage import QuotaStorage

class RateLimiter:

    def __init__(
        self,
        storage: QuotaStorage,
        max_requests: int = 25,
        window_seconds: int = 86400,
        min_interval: float = 1.5
    ):
        self.storage = storage
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.min_interval = min_interval

    def acquire(self):

        while True:

            with closing(self.storage.connect()) as conn:

                conn.execute("BEGIN IMMEDIATE")

                now = time.time()
                cutoff = now - self.window_seconds

                conn.execute(
                    """
                    DELETE FROM api_requests
                    WHERE requested_at <= ?
                    """,
                    (cutoff,)
                )

                requests = conn.execute(
                    """
                    SELECT requested_at
                    FROM api_requests
                    WHERE requested_at > ?
                    ORDER BY requested_at ASC
                    """,
                    (cutoff,)
                ).fetchall()

                wait_until = now

                # Daily quota check
                if len(requests) >= self.max_requests:
                    wait_until = max(
                        wait_until,
                        requests[0][0] + self.window_seconds
                    )

                # Minimum interval check
                if requests:
                    wait_until = max(
                        wait_until,
                        requests[-1][0] + self.min_interval
                    )

                if wait_until <= now:

                    conn.execute(
                        """
                        INSERT INTO api_requests (requested_at)
                        VALUES (?)
                        """,
                        (now,)
                    )

                    conn.commit()
                    return

                conn.commit()

            sleep_duration = wait_until - time.time()

            if sleep_duration > 0:
                print(
                    f"[RATE LIMITER] Waiting "
                    f"{sleep_duration:.2f} seconds..."
                )

                time.sleep(sleep_duration)


quota_storage = QuotaStorage(
    API_TRACKER_PATH / "api_usage.sqlite3"
)

av_rate_limiter = RateLimiter(
    storage=quota_storage,
    max_requests=25,
    window_seconds=86400,
    min_interval=1.5
)