import { errorDetails, type AppLogger } from "./logger";

export type ShutdownHandler = () => Promise<void>;

export function installSignalHandlers(
  shutdown: ShutdownHandler,
  logger: AppLogger,
  timeoutMs: number,
): void {
  let shuttingDown = false;

  const run = async (signal: NodeJS.Signals): Promise<void> => {
    if (shuttingDown) {
      logger.warn("Shutdown already in progress", { signal });
      return;
    }

    shuttingDown = true;
    logger.info("Received signal; starting graceful shutdown", { signal, timeoutMs });

    const forceExit = setTimeout(() => {
      logger.error("Graceful shutdown timed out; forcing exit", { signal, timeoutMs });
      process.exit(1);
    }, timeoutMs);
    forceExit.unref();

    try {
      await shutdown();
      clearTimeout(forceExit);
      logger.info("Graceful shutdown complete", { signal });
      process.exit(0);
    } catch (error) {
      clearTimeout(forceExit);
      logger.error("Graceful shutdown failed", errorDetails(error));
      process.exit(1);
    }
  };

  process.on("SIGTERM", () => void run("SIGTERM"));
  process.on("SIGINT", () => void run("SIGINT"));
}
