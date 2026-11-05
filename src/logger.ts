type Level = "debug" | "info" | "warn" | "error";

export interface LogContext {
  [key: string]: unknown;
}

export interface AppLogger {
  debug(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  error(message: string, context?: LogContext): void;
  child(context: LogContext): AppLogger;
}

const levels: Record<Level, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function configuredLevel(): Level {
  const value = process.env.LOG_LEVEL?.trim().toLowerCase();
  if (value === "debug" || value === "info" || value === "warn" || value === "error") {
    return value;
  }
  return "info";
}

const minimumLevel = configuredLevel();

function emit(level: Level, message: string, base: LogContext, context?: LogContext): void {
  if (levels[level] < levels[minimumLevel]) return;

  const line = JSON.stringify({
    timestamp: new Date().toISOString(),
    severity: level.toUpperCase(),
    message,
    service: process.env.K_SERVICE ?? "local",
    revision: process.env.K_REVISION ?? "local",
    ...base,
    ...context,
  });

  // Cloud Run captures stdout/stderr and forwards them to Cloud Logging.
  if (level === "error") {
    console.error(line);
  } else if (level === "warn") {
    console.warn(line);
  } else {
    console.log(line);
  }
}

export function createLogger(base: LogContext = {}): AppLogger {
  return {
    debug: (message, context) => emit("debug", message, base, context),
    info: (message, context) => emit("info", message, base, context),
    warn: (message, context) => emit("warn", message, base, context),
    error: (message, context) => emit("error", message, base, context),
    child: (context) => createLogger({ ...base, ...context }),
  };
}

export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === "string") return error;
  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
}

export function errorDetails(error: unknown): LogContext {
  if (error instanceof Error) {
    return {
      error: error.message,
      name: error.name,
      stack: error.stack,
      ...(Object.getOwnPropertyNames(error).reduce<LogContext>((acc, key) => {
        if (key !== "message" && key !== "name" && key !== "stack") {
          acc[key] = (error as unknown as Record<string, unknown>)[key];
        }
        return acc;
      }, {})),
    };
  }
  return { error: errorMessage(error) };
}
