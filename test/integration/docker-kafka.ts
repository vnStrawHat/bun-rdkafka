/**
 * test/integration/docker-kafka.ts — a real Kafka broker for integration tests.
 *
 * Runs a single `apache/kafka` node on KRaft (no ZooKeeper), with a listener
 * `PLAINTEXT://localhost:9092`, `auto.create.topics.enable=true`.
 *
 * Designed for small machines (CI runners with 2–4 CPUs / 3 GB RAM):
 *  - `KAFKA_HEAP_OPTS=-Xmx512m -Xms256m`
 *  - a fixed container name ⇒ **idempotent**: running → reused, stopped →
 *    restarted, absent → `docker run`.
 *
 * Shared by every integration test:
 *
 * ```ts
 * const kafka = await startKafka();      // { brokers: "localhost:9092" }
 * afterAll(() => stopKafka());           // keeps the container if KEEP_KAFKA=1
 * ```
 *
 * Environment variables:
 *  | Variable          | Meaning                                              |
 *  |-------------------|------------------------------------------------------|
 *  | `KEEP_KAFKA=1`    | `stopKafka()` keeps the container (debugging)        |
 *  | `KAFKA_IMAGE`     | force a specific image                                |
 *  | `KAFKA_PORT`      | host port (default 9092)                              |
 *  | `KAFKA_BROKERS`   | use an existing broker, do NOT touch docker           |
 */

import { existsSync } from "node:fs";
import { suffix } from "bun:ffi";

/* ========================================================================== */
/* Constants                                                                   */
/* ========================================================================== */

export const CONTAINER_NAME = "bun-rdkafka-test-kafka";

/** The default image + common alternative names on internal registry mirrors. */
const DEFAULT_IMAGES = ["apache/kafka:3.9.0", "apache/kafka:latest"] as const;

const HOST_PORT = Number(process.env["KAFKA_PORT"] ?? 9092);

/** An existing broker (bypassing docker entirely). */
const EXTERNAL_BROKERS = process.env["KAFKA_BROKERS"];

/** The KRaft server log line marking readiness to accept requests. */
const READY_LOG = /Kafka Server started|KafkaRaftServer.*started|started \(kafka\.server/i;

export interface KafkaHandle {
  /** `bootstrap.servers` to drop straight into the librdkafka config. */
  brokers: string;
  /** `true` when the container pre-existed (not created by this call). */
  reused: boolean;
  /** `false` when using `KAFKA_BROKERS` (no container managed). */
  managed: boolean;
  image: string | undefined;
}

/* ========================================================================== */
/* Shell utilities                                                             */
/* ========================================================================== */

interface RunResult {
  ok: boolean;
  code: number;
  stdout: string;
  stderr: string;
}

async function run(args: string[], timeoutMs = 60_000): Promise<RunResult> {
  const proc = Bun.spawn(args, { stdout: "pipe", stderr: "pipe" });
  const timer = setTimeout(() => proc.kill(9), timeoutMs);
  try {
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ]);
    return { ok: code === 0, code, stdout, stderr };
  } finally {
    clearTimeout(timer);
  }
}

async function docker(args: string[], timeoutMs?: number): Promise<RunResult> {
  return run(["docker", ...args], timeoutMs);
}

/* ========================================================================== */
/* Runability conditions                                                       */
/* ========================================================================== */

let dockerAvailableCache: boolean | undefined;

/** Is the Docker CLI present and the daemon answering? (process-cached) */
export async function isDockerAvailable(): Promise<boolean> {
  if (dockerAvailableCache !== undefined) return dockerAvailableCache;
  if (EXTERNAL_BROKERS) return (dockerAvailableCache = true);
  const res = await docker(["info", "--format", "{{.ServerVersion}}"], 15_000);
  dockerAvailableCache = res.ok;
  return dockerAvailableCache;
}

/** The native library path the loader would use (file not opened). */
export function nativeLibPath(): string {
  return (
    process.env["BUN_RDKAFKA_LIB_PATH"] ??
    new URL(`../../native/build/libbunrdkafka.${suffix}`, import.meta.url).pathname
  );
}

/** Has the shim been built? Integration tests self-skip otherwise. */
export function hasNativeLib(): boolean {
  return existsSync(nativeLibPath());
}

/**
 * Sufficient conditions for integration tests: a shim + docker (or
 * `KAFKA_BROKERS` pointing at an existing broker).
 */
export async function integrationAvailable(): Promise<boolean> {
  return hasNativeLib() && (await isDockerAvailable());
}

/* ========================================================================== */
/* Image selection                                                             */
/* ========================================================================== */

async function localImages(): Promise<string[]> {
  const res = await docker(["images", "--format", "{{.Repository}}:{{.Tag}}"], 20_000);
  if (!res.ok) return [];
  return res.stdout.split("\n").map((s) => s.trim()).filter(Boolean);
}

/**
 * The image to use: `KAFKA_IMAGE` > a locally present apache/kafka image
 * (including internal registry mirrors) > pulling the default image.
 */
async function resolveImage(): Promise<string> {
  const forced = process.env["KAFKA_IMAGE"];
  if (forced) {
    await ensurePulled(forced);
    return forced;
  }

  const local = await localImages();
  for (const candidate of DEFAULT_IMAGES) {
    if (local.includes(candidate)) return candidate;
  }
  // internal mirror: <registry>/apache/kafka:<tag>
  const mirrored = local.find((img) => /(^|\/)apache\/kafka:/.test(img));
  if (mirrored) return mirrored;

  const primary = DEFAULT_IMAGES[0];
  await ensurePulled(primary);
  return primary;
}

async function ensurePulled(image: string): Promise<void> {
  const inspect = await docker(["image", "inspect", image], 20_000);
  if (inspect.ok) return;
  const pull = await docker(["pull", image], 15 * 60_000);
  if (!pull.ok) {
    throw new Error(
      `docker-kafka: could not pull image "${image}".\n${pull.stderr.trim()}\n` +
        `Hint: set KAFKA_IMAGE=<available image> or KAFKA_BROKERS=host:port to use an existing broker.`,
    );
  }
}

/* ========================================================================== */
/* Container lifecycle                                                         */
/* ========================================================================== */

type ContainerState = "missing" | "running" | "stopped";

async function containerState(): Promise<ContainerState> {
  const res = await docker(
    ["inspect", "-f", "{{.State.Running}}", CONTAINER_NAME],
    20_000,
  );
  if (!res.ok) return "missing";
  return res.stdout.trim() === "true" ? "running" : "stopped";
}

function containerEnv(): string[] {
  const env: Record<string, string> = {
    KAFKA_NODE_ID: "1",
    KAFKA_PROCESS_ROLES: "broker,controller",
    KAFKA_LISTENERS: `PLAINTEXT://:${HOST_PORT},CONTROLLER://:9093`,
    KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://localhost:${HOST_PORT}`,
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093",
    KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT",
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
    KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: "1",
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true",
    KAFKA_NUM_PARTITIONS: "1",
    KAFKA_DEFAULT_REPLICATION_FACTOR: "1",
    // Retention off: tests produce messages with fake (past) timestamps, and a
    // finite retention would delete the segment before the consumer reads it.
    KAFKA_LOG_RETENTION_MS: "-1",
    KAFKA_LOG_RETENTION_BYTES: "-1",
    // The test machine has only 3 GB RAM — pin the broker JVM's heap.
    KAFKA_HEAP_OPTS: "-Xmx512m -Xms256m",
  };
  return Object.entries(env).flatMap(([k, v]) => ["-e", `${k}=${v}`]);
}

/**
 * The container config's fingerprint. An old container built with a different
 * config gets rebuilt instead of wrongly reused.
 */
function configFingerprint(image: string): string {
  return Bun.hash(`${image}\x00${containerEnv().join("\x00")}`).toString(16);
}

async function containerLabel(): Promise<string> {
  const res = await docker(
    ["inspect", "-f", '{{index .Config.Labels "bun-rdkafka.config"}}', CONTAINER_NAME],
    20_000,
  );
  return res.ok ? res.stdout.trim() : "";
}

/**
 * Ensures a broker is running at `localhost:${KAFKA_PORT}`. Idempotent:
 * safe to call repeatedly in one process (or across test processes).
 */
export async function startKafka(): Promise<KafkaHandle> {
  if (EXTERNAL_BROKERS) {
    return { brokers: EXTERNAL_BROKERS, reused: true, managed: false, image: undefined };
  }

  let state = await containerState();

  // An old container with a changed config ⇒ rebuild to be safe.
  if (state !== "missing") {
    const currentImage = (
      await docker(["inspect", "-f", "{{.Config.Image}}", CONTAINER_NAME])
    ).stdout.trim();
    if ((await containerLabel()) !== configFingerprint(currentImage)) {
      await docker(["rm", "-f", CONTAINER_NAME], 120_000);
      state = "missing";
    }
  }

  let reused = state !== "missing";
  let image: string;

  if (state === "running") {
    image = (await docker(["inspect", "-f", "{{.Config.Image}}", CONTAINER_NAME])).stdout.trim();
  } else if (state === "stopped") {
    image = (await docker(["inspect", "-f", "{{.Config.Image}}", CONTAINER_NAME])).stdout.trim();
    const started = await docker(["start", CONTAINER_NAME], 60_000);
    if (!started.ok) {
      // the old container is broken (e.g. the port got taken) → remove and rebuild
      await docker(["rm", "-f", CONTAINER_NAME], 60_000);
      image = await resolveImage();
      await createContainer(image);
      reused = false;
    }
  } else {
    image = await resolveImage();
    await createContainer(image);
    reused = false;
  }

  await waitForReady();
  return { brokers: `localhost:${HOST_PORT}`, reused, managed: true, image };
}

async function createContainer(image: string): Promise<void> {
  const res = await docker(
    [
      "run",
      "-d",
      "--name",
      CONTAINER_NAME,
      "--label",
      `bun-rdkafka.config=${configFingerprint(image)}`,
      "-p",
      `${HOST_PORT}:${HOST_PORT}`,
      ...containerEnv(),
      image,
    ],
    120_000,
  );
  if (!res.ok) {
    throw new Error(`docker-kafka: docker run failed:\n${res.stderr.trim()}`);
  }
}

/**
 * The container's logs (for diagnosing test failures).
 * @param since RFC3339 — only logs from this point (`docker` keeps logs from
 *   earlier boots, so readiness probing must cut at `State.StartedAt`).
 */
export async function kafkaLogs(tail = 50, since?: string): Promise<string> {
  const args = ["logs", "--tail", String(tail)];
  if (since) args.push("--since", since);
  args.push(CONTAINER_NAME);
  const res = await docker(args, 30_000);
  return `${res.stdout}\n${res.stderr}`;
}

/** Is port 9092 accepting TCP yet? */
async function canConnect(): Promise<boolean> {
  try {
    const socket = await Bun.connect({
      hostname: "127.0.0.1",
      port: HOST_PORT,
      socket: { data() {}, error() {}, close() {} },
    });
    socket.end();
    return true;
  } catch {
    return false;
  }
}

/** The current start's `State.StartedAt` (RFC3339). */
async function containerStartedAt(): Promise<string> {
  const res = await docker(["inspect", "-f", "{{.State.StartedAt}}", CONTAINER_NAME], 20_000);
  return res.ok ? res.stdout.trim() : "";
}

/**
 * Waits for broker readiness. Far cheaper than spawning `kafka-topics.sh` (an
 * JVM 512 MB).
 *
 * Condition: the port accepts TCP **and** the broker has truly booted. The
 * second half cannot rely on TCP alone — `docker run -p` binds the host port
 * the moment the container starts, before the JVM listens — so the "Kafka
 * Server started" line **of the current start** must be seen (`docker logs`
 * keeps lines from earlier boots; after `docker start` the old line remains
 * and would report readiness too early). For long-running containers that line
 * may scroll past `--tail`; then an uptime > 60 s is the substitute evidence.
 */
export async function waitForReady(timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const startedAt = await containerStartedAt();
  const startedMs = Date.parse(startedAt);
  let booted = false;
  let lastLog = "";
  while (Date.now() < deadline) {
    if (!booted) {
      lastLog = await kafkaLogs(5_000, startedAt || undefined);
      booted =
        READY_LOG.test(lastLog) ||
        (Number.isFinite(startedMs) && Date.now() - startedMs > 60_000);
    }
    if (booted && (await canConnect())) return;
    await Bun.sleep(500);
  }
  throw new Error(
    `docker-kafka: the broker was not ready after ${timeoutMs}ms.\n--- logs ---\n${lastLog.slice(-4000)}`,
  );
}

/**
 * Creates a topic with the given partition count (running `kafka-topics.sh`
 * inside the container). Optional — the broker has auto-create on — but needed
 * when a test wants a non-default partition count. "already exists" errors are
 * ignored.
 */
export async function createTopic(topic: string, partitions = 1): Promise<void> {
  if (!EXTERNAL_BROKERS) {
    const res = await docker(
      [
        "exec",
        CONTAINER_NAME,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        `localhost:${HOST_PORT}`,
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        String(partitions),
        "--replication-factor",
        "1",
      ],
      120_000,
    );
    if (!res.ok && !/already exists/i.test(res.stdout + res.stderr)) {
      throw new Error(`docker-kafka: creating topic "${topic}" failed:\n${res.stderr.trim()}`);
    }
  }
}

/**
 * Cleans up the broker after tests. The container is kept when `KEEP_KAFKA=1`
 * (for debugging) or when the broker is externally provided (`KAFKA_BROKERS`).
 */
export async function stopKafka(): Promise<void> {
  if (EXTERNAL_BROKERS) return;
  if (process.env["KEEP_KAFKA"] === "1") return;
  await docker(["rm", "-f", CONTAINER_NAME], 120_000);
}

/* ========================================================================== */
/* Broker SASL (M4 — security integration)                                     */
/* ==========================================================================
 * A SEPARATE container, not shared with the PLAINTEXT broker above: a listener
 * SASL_PLAINTEXT (PLAIN + SCRAM-SHA-256) publish ra host; listener INTERNAL
 * (PLAINTEXT, unpublished) for inter-broker traffic + `kafka-configs.sh`
 * creating the SCRAM user via `docker exec`. Heap 384m — this container is
 * short-lived (stopped in afterAll).
 *
 * The apache/kafka image's env→property convention (verified by reading the
 * generated server.properties): `_`→`.`, `__`→`_`, `___`→`-` — i.e. properties
 * with hyphens (scram-sha-256) need THREE underscores in the env name.
 */

export const SASL_CONTAINER_NAME = "bun-rdkafka-test-kafka-sasl";

const SASL_HOST_PORT = Number(process.env["KAFKA_SASL_PORT"] ?? 9094);

export const SASL_CREDENTIALS = {
  username: "admin",
  password: "admin-secret",
} as const;

const PLAIN_JAAS =
  'org.apache.kafka.common.security.plain.PlainLoginModule required ' +
  `username="${SASL_CREDENTIALS.username}" password="${SASL_CREDENTIALS.password}" ` +
  `user_${SASL_CREDENTIALS.username}="${SASL_CREDENTIALS.password}";`;

function saslContainerEnv(): string[] {
  const env: Record<string, string> = {
    KAFKA_NODE_ID: "1",
    KAFKA_PROCESS_ROLES: "broker,controller",
    KAFKA_LISTENERS: `SASL://:${SASL_HOST_PORT},CONTROLLER://:9095,INTERNAL://:19092`,
    KAFKA_ADVERTISED_LISTENERS: `SASL://localhost:${SASL_HOST_PORT},INTERNAL://localhost:19092`,
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
      "CONTROLLER:PLAINTEXT,SASL:SASL_PLAINTEXT,INTERNAL:PLAINTEXT",
    KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
    KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9095",
    KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL",
    KAFKA_SASL_ENABLED_MECHANISMS: "PLAIN,SCRAM-SHA-256",
    // listener.name.sasl.plain.sasl.jaas.config
    KAFKA_LISTENER_NAME_SASL_PLAIN_SASL_JAAS_CONFIG: PLAIN_JAAS,
    // listener.name.sasl.scram-sha-256.sasl.jaas.config (server side SCRAM
    // carries no credentials — the user lives in metadata, created via ensureScramUser)
    KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___256_SASL_JAAS_CONFIG:
      "org.apache.kafka.common.security.scram.ScramLoginModule required;",
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
    KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: "1",
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0",
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true",
    KAFKA_NUM_PARTITIONS: "1",
    KAFKA_DEFAULT_REPLICATION_FACTOR: "1",
    KAFKA_LOG_RETENTION_MS: "-1",
    KAFKA_HEAP_OPTS: "-Xmx384m -Xms192m",
  };
  return Object.entries(env).flatMap(([k, v]) => ["-e", `${k}=${v}`]);
}

function saslFingerprint(image: string): string {
  return Bun.hash(`${image}\x00${saslContainerEnv().join("\x00")}`).toString(16);
}

async function saslState(): Promise<ContainerState> {
  const res = await docker(["inspect", "-f", "{{.State.Running}}", SASL_CONTAINER_NAME], 20_000);
  if (!res.ok) return "missing";
  return res.stdout.trim() === "true" ? "running" : "stopped";
}

async function canConnectPort(port: number): Promise<boolean> {
  try {
    const socket = await Bun.connect({
      hostname: "127.0.0.1",
      port,
      socket: { data() {}, error() {}, close() {} },
    });
    socket.end();
    return true;
  } catch {
    return false;
  }
}

async function waitForSaslReady(timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const startedAtRes = await docker(
    ["inspect", "-f", "{{.State.StartedAt}}", SASL_CONTAINER_NAME],
    20_000,
  );
  const startedAt = startedAtRes.ok ? startedAtRes.stdout.trim() : "";
  const startedMs = Date.parse(startedAt);
  let booted = false;
  let lastLog = "";
  while (Date.now() < deadline) {
    if (!booted) {
      const args = ["logs", "--tail", "5000"];
      if (startedAt) args.push("--since", startedAt);
      args.push(SASL_CONTAINER_NAME);
      const res = await docker(args, 30_000);
      lastLog = `${res.stdout}\n${res.stderr}`;
      booted =
        READY_LOG.test(lastLog) ||
        (Number.isFinite(startedMs) && Date.now() - startedMs > 60_000);
    }
    if (booted && (await canConnectPort(SASL_HOST_PORT))) return;
    await Bun.sleep(500);
  }
  throw new Error(
    `docker-kafka: the SASL broker was not ready after ${timeoutMs}ms.\n--- logs ---\n${lastLog.slice(-4000)}`,
  );
}

/** Creates/refreshes the SCRAM-SHA-256 user via the INTERNAL listener (KRaft ≥ 3.5). */
async function ensureScramUser(): Promise<void> {
  const res = await docker(
    [
      "exec",
      SASL_CONTAINER_NAME,
      "/opt/kafka/bin/kafka-configs.sh",
      "--bootstrap-server",
      "localhost:19092",
      "--alter",
      "--add-config",
      `SCRAM-SHA-256=[iterations=4096,password=${SASL_CREDENTIALS.password}]`,
      "--entity-type",
      "users",
      "--entity-name",
      SASL_CREDENTIALS.username,
    ],
    60_000,
  );
  if (!res.ok) {
    throw new Error(`docker-kafka: creating the SCRAM user failed:\n${res.stderr.trim()}`);
  }
}

/**
 * A SASL_PLAINTEXT broker (PLAIN + SCRAM-SHA-256, user admin/admin-secret) at
 * `localhost:${KAFKA_SASL_PORT ?? 9094}`. Idempotent like `startKafka()`.
 * NOT affected by `KAFKA_BROKERS` (always manages its own container).
 */
export async function startKafkaSasl(): Promise<KafkaHandle> {
  let state = await saslState();

  if (state !== "missing") {
    const currentImage = (
      await docker(["inspect", "-f", "{{.Config.Image}}", SASL_CONTAINER_NAME])
    ).stdout.trim();
    const label = await docker(
      ["inspect", "-f", '{{index .Config.Labels "bun-rdkafka.config"}}', SASL_CONTAINER_NAME],
      20_000,
    );
    if ((label.ok ? label.stdout.trim() : "") !== saslFingerprint(currentImage)) {
      await docker(["rm", "-f", SASL_CONTAINER_NAME], 120_000);
      state = "missing";
    }
  }

  let reused = state !== "missing";
  let image: string;

  if (state === "running") {
    image = (
      await docker(["inspect", "-f", "{{.Config.Image}}", SASL_CONTAINER_NAME])
    ).stdout.trim();
  } else if (state === "stopped") {
    image = (
      await docker(["inspect", "-f", "{{.Config.Image}}", SASL_CONTAINER_NAME])
    ).stdout.trim();
    const started = await docker(["start", SASL_CONTAINER_NAME], 60_000);
    if (!started.ok) {
      await docker(["rm", "-f", SASL_CONTAINER_NAME], 60_000);
      image = await resolveImage();
      await createSaslContainer(image);
      reused = false;
    }
  } else {
    image = await resolveImage();
    await createSaslContainer(image);
    reused = false;
  }

  await waitForSaslReady();
  await ensureScramUser();
  return { brokers: `localhost:${SASL_HOST_PORT}`, reused, managed: true, image };
}

async function createSaslContainer(image: string): Promise<void> {
  const res = await docker(
    [
      "run",
      "-d",
      "--name",
      SASL_CONTAINER_NAME,
      "--label",
      `bun-rdkafka.config=${saslFingerprint(image)}`,
      "-p",
      `${SASL_HOST_PORT}:${SASL_HOST_PORT}`,
      ...saslContainerEnv(),
      image,
    ],
    120_000,
  );
  if (!res.ok) {
    throw new Error(`docker-kafka: docker run (SASL) failed:\n${res.stderr.trim()}`);
  }
}

/** Cleans up the SASL broker (its own container — the shared broker is untouched). */
export async function stopKafkaSasl(): Promise<void> {
  if (process.env["KEEP_KAFKA"] === "1") return;
  await docker(["rm", "-f", SASL_CONTAINER_NAME], 120_000);
}
