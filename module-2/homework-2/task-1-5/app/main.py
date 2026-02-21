import logging
import threading
import time

from app.config import settings
from app.database import DatabaseManager
from app.strategies import Strategies

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_benchmark(name: str, strategy_func):
    """
    Executes the threaded benchmark.
    """
    logger.info(f"--- Starting: {name} ---")
    DatabaseManager.reset_counter()

    threads = []
    start_time = time.time()

    for _ in range(settings.threads):
        t = threading.Thread(target=strategy_func)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = time.time() - start_time

    with DatabaseManager.get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = %s", (settings.target_user_id,))
            final_count = cursor.fetchone()[0]

    expected = settings.threads * settings.iterations
    loss_pct = ((expected - final_count) / expected) * 100

    logger.info(f"Finished:    {name}")
    logger.info(f"Duration:    {duration:.4f}s")
    logger.info(f"Final Count: {final_count} / {expected}")
    logger.info(f"Data Loss:   {loss_pct:.2f}%")
    logger.info("-" * 40)


if __name__ == "__main__":
    time.sleep(3)

    try:
        DatabaseManager.init_schema()

        test_cases = [
            ("1. Lost update", Strategies.lost_update),
            ("2. Serializable update", Strategies.serializable_update),
            ("3. In-place update", Strategies.in_place_update),
            ("4. Row-level locking", Strategies.row_level_locking),
            ("5. Optimistic concurrency control", Strategies.optimistic_concurrency_control)
        ]

        for name, func in test_cases:
            run_benchmark(name, func)

    except Exception as error:
        logger.error(f"Fatal Application Error: {error}")
    finally:
        DatabaseManager.close_pool()
