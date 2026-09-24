# NioStream

Java 17+ large-file/directory transfer over a congested TCP network.

Implemented:
1. Automatic reconnect/retry.
2. Resume after interruption.
4. Multiple files/directories.
6. Parallel transfer streams.
7. Adaptive aggregate rate control.
8. `java.nio.Selector` non-blocking socket I/O.
10. Manifest + per-file bitmap state.

## Build

```bash
mvn package
```

## Receiver

```bash
java -cp target/classes NioStream receive \
  --port 5000 \
  --dest D:\backup \
  --streams 6 \
  --chunk 4M \
  --verify
```

## Sender

```bash
java -cp target/classes NioStream send \
  --host 192.168.1.50 \
  --port 5000 \
  --source D:\data\large-folder \
  --streams 6 \
  --chunk 4M \
  --rate 20M \
  --min-rate 1M \
  --max-rate 100M \
  --retry 2000 \
  --retries 0 \
  --verify
```

`--retries 0` means unlimited retries.

### Named arguments

- `--host` receiver host
- `--port` TCP port, default 5000
- `--source` source file or directory
- `--dest` destination directory
- `--streams` parallel TCP streams, default 4
- `--chunk` chunk size, default 4M
- `--rate` initial aggregate bandwidth; `0` means use max
- `--min-rate` adaptive lower bound
- `--max-rate` adaptive upper bound
- `--retry` reconnect delay in milliseconds
- `--retries` retry count; 0 = unlimited
- `--state` receiver resume-state directory
- `--verify` SHA-256 chunk/file verification
- `--keep-state` keep state after successful completion
- `--quiet` reserved for reduced logging

Sizes accept K/M/G/T.

## Notes

The receiver writes each chunk directly at its file offset, so chunks from
parallel streams can arrive out of order. The bitmap state is persisted after
each acknowledged chunk. Restarting the receiver therefore skips chunks
already committed to disk.

The sender's adaptive controller changes the aggregate target rate based on
observed transfer timing. `--rate`, `--min-rate`, and `--max-rate` bound that
behavior.

For Internet/WAN deployment, put TLS/authentication around this protocol before
using it with untrusted networks.
