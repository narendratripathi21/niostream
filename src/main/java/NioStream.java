import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class NioStream {

    static final String VERSION = "2.0.0";

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || contains(args, "--help") || contains(args, "-h")) {
            usage();
            return;
        }

        String command = args[0].toLowerCase(Locale.ROOT);

        if (!"copy".equals(command)) {
            throw new IllegalArgumentException(
                    "Unsupported command: " + command + ". Use: copy");
        }

        Args options = Args.parse(Arrays.copyOfRange(args, 1, args.length));
        new Copier(options).run();
    }

    static boolean contains(String[] args, String value) {
        for (String s : args) {
            if (s.equals(value)) return true;
        }
        return false;
    }

    static void usage() {
        System.out.println("""
                NioStream %s
                Single-process Windows UNC -> UNC large-file copier

                Usage:
                  java -jar niostream-2.0.0.jar copy ^
                    --source "\\\\NAS01\\Share\\Source" ^
                    --destination "\\\\NAS02\\Share\\Destination"

                Options:
                  --source PATH
                  --destination PATH
                  --streams N             Parallel workers. Default 4
                  --chunk SIZE            Resume chunk. Default 16M
                  --buffer SIZE            I/O buffer. Default = chunk
                  --rate SIZE              Initial aggregate rate. 0 = unlimited
                  --min-rate SIZE         Adaptive lower bound. Default 5M
                  --max-rate SIZE         Adaptive upper bound. Default 100M
                  --resume BOOL            Default true
                  --verify BOOL            SHA-256 verification. Default false
                  --retries N              Retries per file. 0 = unlimited
                  --retry-delay MS         Delay between retries. Default 2000
                  --state PATH             State directory
                  --preserve-time BOOL     Default true
                  --overwrite BOOL         Default true
                  --delete-extra BOOL      Default false
                  --quiet BOOL             Default false
                  --dry-run BOOL           Default false

                Size examples:
                  16M
                  1G
                  500M

                Example:
                  java -jar niostream-2.0.0.jar copy ^
                    --source "\\\\NAS01\\Media" ^
                    --destination "\\\\NAS02\\Backup\\Media" ^
                    --streams 8 ^
                    --chunk 16M ^
                    --rate 50M ^
                    --min-rate 5M ^
                    --max-rate 100M ^
                    --resume true ^
                    --verify true
                """.formatted(VERSION));
    }

    static final class Args {
        private final Map<String, String> values = new HashMap<>();

        static Args parse(String[] args) {
            Args result = new Args();

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];

                if (!arg.startsWith("--")) {
                    throw new IllegalArgumentException(
                            "Expected named argument, got: " + arg);
                }

                String value = arg.substring(2);
                int equals = value.indexOf('=');

                if (equals >= 0) {
                    result.values.put(
                            value.substring(0, equals),
                            value.substring(equals + 1));
                    continue;
                }

                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    result.values.put(value, args[++i]);
                } else {
                    result.values.put(value, "true");
                }
            }

            return result;
        }

        String get(String key, String defaultValue) {
            return values.getOrDefault(key, defaultValue);
        }

        boolean bool(String key, boolean defaultValue) {
            return Boolean.parseBoolean(
                    get(key, Boolean.toString(defaultValue)));
        }

        int integer(String key, int defaultValue) {
            return Integer.parseInt(
                    get(key, Integer.toString(defaultValue)));
        }

        long size(String key, long defaultValue) {
            return parseSize(
                    get(key, Long.toString(defaultValue)));
        }
    }

    static long parseSize(String value) {
        String s = value.trim().toUpperCase(Locale.ROOT);

        long multiplier = 1;

        if (s.endsWith("K")) {
            multiplier = 1024L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("M")) {
            multiplier = 1024L * 1024L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("G")) {
            multiplier = 1024L * 1024L * 1024L;
            s = s.substring(0, s.length() - 1);
        } else if (s.endsWith("T")) {
            multiplier = 1024L * 1024L * 1024L * 1024L;
            s = s.substring(0, s.length() - 1);
        }

        return (long) (Double.parseDouble(s) * multiplier);
    }

    static final class FileEntry {
        final String relativePath;
        final long size;
        final long modified;
        final int chunks;

        FileEntry(
                String relativePath,
                long size,
                long modified,
                int chunks) {
            this.relativePath = relativePath;
            this.size = size;
            this.modified = modified;
            this.chunks = chunks;
        }
    }

    static int chunkCount(long size, long chunkSize) {
        if (size == 0) return 1;

        long count = (size + chunkSize - 1) / chunkSize;

        if (count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Too many chunks. Increase --chunk.");
        }

        return (int) count;
    }

    static final class Manifest {
        final Path source;
        final Path destination;
        final List<FileEntry> files;

        Manifest(
                Path source,
                Path destination,
                List<FileEntry> files) {
            this.source = source;
            this.destination = destination;
            this.files = files;
        }
    }

    static Manifest createManifest(
            Path source,
            Path destination,
            long chunkSize) throws IOException {

        List<FileEntry> files = new ArrayList<>();

        if (Files.isRegularFile(source)) {
            long size = Files.size(source);

            files.add(new FileEntry(
                    source.getFileName().toString(),
                    size,
                    Files.getLastModifiedTime(source).toMillis(),
                    chunkCount(size, chunkSize)));

            return new Manifest(source, destination, files);
        }

        try (var stream = Files.walk(source)) {
            stream
                .filter(Files::isRegularFile)
                .sorted()
                .forEach(path -> {
                    try {
                        long size = Files.size(path);

                        String relative =
                                source.relativize(path)
                                        .toString()
                                        .replace('\\', '/');

                        files.add(new FileEntry(
                                relative,
                                size,
                                Files.getLastModifiedTime(path).toMillis(),
                                chunkCount(size, chunkSize)));

                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        return new Manifest(source, destination, files);
    }

    static final class ResumeState {
        private final Path root;

        ResumeState(Path root) {
            this.root = root;
        }

        private Path stateFile(String relativePath) {
            /*
             * The hash is only a filename locator. The actual source metadata
             * is checked separately before trusting the state.
             */
            return root.resolve(
                    Integer.toUnsignedString(relativePath.hashCode(), 16)
                            + ".state");
        }

        synchronized BitSet load(
                String relativePath,
                int chunkCount) {

            Path file = stateFile(relativePath);

            if (!Files.exists(file)) {
                return new BitSet(chunkCount);
            }

            try {
                BitSet bits =
                        BitSet.valueOf(Files.readAllBytes(file));

                bits.clear(chunkCount, bits.length());
                return bits;

            } catch (IOException e) {
                return new BitSet(chunkCount);
            }
        }

        synchronized void save(
                String relativePath,
                BitSet bits) throws IOException {

            Files.createDirectories(root);

            Path target = stateFile(relativePath);
            Path temp = root.resolve(
                    target.getFileName() + ".tmp");

            Files.write(
                    temp,
                    bits.toByteArray(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);

            try {
                Files.move(
                        temp,
                        target,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(
                        temp,
                        target,
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }

        synchronized void remove(String relativePath) {
            try {
                Files.deleteIfExists(stateFile(relativePath));
            } catch (IOException ignored) {
            }
        }
    }

    static final class AdaptiveRateController {
        private final long minRate;
        private final long maxRate;
        private final AtomicLong rate;

        AdaptiveRateController(
                long initialRate,
                long minRate,
                long maxRate) {

            this.minRate = Math.max(1, minRate);
            this.maxRate = Math.max(this.minRate, maxRate);

            long initial =
                    initialRate <= 0
                            ? this.maxRate
                            : initialRate;

            this.rate = new AtomicLong(
                    Math.max(
                            this.minRate,
                            Math.min(this.maxRate, initial)));
        }

        long currentRate() {
            return rate.get();
        }

        void sample(
                long bytes,
                long elapsedNanos) {

            if (bytes <= 0 || elapsedNanos <= 0) {
                return;
            }

            double observed =
                    bytes / (elapsedNanos / 1_000_000_000.0);

            long current = rate.get();

            /*
             * Very simple adaptive controller:
             *
             * observed > 90% of target -> cautiously increase
             * observed < 60% of target -> decrease
             *
             * Windows/SMB remains responsible for TCP congestion control.
             */
            if (observed > current * 0.90) {
                rate.compareAndSet(
                        current,
                        Math.min(
                                maxRate,
                                (long) (current * 1.08)));
            } else if (observed < current * 0.60) {
                rate.compareAndSet(
                        current,
                        Math.max(
                                minRate,
                                (long) (current * 0.78)));
            }
        }
    }

    static final class RateLimiter {
        private final AdaptiveRateController controller;
        private final AtomicLong nextAvailable = new AtomicLong();

        RateLimiter(AdaptiveRateController controller) {
            this.controller = controller;
        }

        void acquire(long bytes) {
            long rate = controller.currentRate();

            if (rate <= 0 || bytes <= 0) {
                return;
            }

            long duration =
                    (long) (
                            bytes
                                    * 1_000_000_000.0
                                    / rate);

            long now = System.nanoTime();

            long start =
                    nextAvailable.getAndUpdate(
                            previous ->
                                    Math.max(previous, now)
                                            + duration);

            long wait = start - now;

            if (wait > 0) {
                LockSupport.parkNanos(wait);
            }
        }
    }

    static final class Copier {

        private final Args args;

        private final Path source;
        private final Path destination;

        private final int streams;
        private final long chunkSize;
        private final long bufferSize;

        private final boolean resume;
        private final boolean verify;
        private final boolean preserveTime;
        private final boolean overwrite;
        private final boolean deleteExtra;
        private final boolean quiet;
        private final boolean dryRun;

        private final int retries;
        private final long retryDelay;

        private final ResumeState resumeState;
        private final AdaptiveRateController adaptive;

        private final ExecutorService workers;

        private final AtomicLong completedFiles =
                new AtomicLong();

        private final AtomicLong completedBytes =
                new AtomicLong();

        Copier(Args args) {

            this.args = args;

            String sourceValue =
                    args.get("source", null);

            String destinationValue =
                    args.get("destination", null);

            if (sourceValue == null) {
                throw new IllegalArgumentException(
                        "--source is required");
            }

            if (destinationValue == null) {
                throw new IllegalArgumentException(
                        "--destination is required");
            }

            source = Paths.get(sourceValue);
            destination = Paths.get(destinationValue);

            streams = Math.max(
                    1,
                    args.integer("streams", 4));

            chunkSize = args.size(
                    "chunk",
                    16L * 1024 * 1024);

            bufferSize = args.size(
                    "buffer",
                    chunkSize);

            if (chunkSize <= 0) {
                throw new IllegalArgumentException(
                        "--chunk must be > 0");
            }

            if (bufferSize <= 0) {
                throw new IllegalArgumentException(
                        "--buffer must be > 0");
            }

            resume = args.bool("resume", true);
            verify = args.bool("verify", false);
            preserveTime =
                    args.bool("preserve-time", true);
            overwrite =
                    args.bool("overwrite", true);
            deleteExtra =
                    args.bool("delete-extra", false);
            quiet = args.bool("quiet", false);
            dryRun = args.bool("dry-run", false);

            retries =
                    Math.max(0, args.integer("retries", 0));

            retryDelay =
                    Math.max(
                            0,
                            args.size("retry-delay", 2000));

            Path defaultState =
                    destination.resolve(".niostream");

            resumeState =
                    new ResumeState(
                            Paths.get(
                                    args.get(
                                            "state",
                                            defaultState.toString())));

            long initialRate =
                    args.size("rate", 0);

            long minRate =
                    args.size(
                            "min-rate",
                            5L * 1024 * 1024);

            long maxRate =
                    args.size(
                            "max-rate",
                            100L * 1024 * 1024);

            adaptive =
                    new AdaptiveRateController(
                            initialRate,
                            minRate,
                            maxRate);

            workers =
                    Executors.newFixedThreadPool(streams);
        }

        void run() throws Exception {

            if (!Files.exists(source)) {
                throw new FileNotFoundException(
                        "Source does not exist: " + source);
            }

            if (!Files.isDirectory(source)
                    && !Files.isRegularFile(source)) {
                throw new IOException(
                        "Source is not a regular file/directory: "
                                + source);
            }

            Files.createDirectories(destination);

            log("NioStream " + VERSION);
            log("Source      : " + source);
            log("Destination : " + destination);
            log("Streams     : " + streams);
            log("Chunk       : " + human(chunkSize));
            log("Initial rate: " + human(adaptive.currentRate())
                    + "/s");

            Manifest manifest =
                    createManifest(
                            source,
                            destination,
                            chunkSize);

            log("Files found : " + manifest.files.size());

            if (dryRun) {
                for (FileEntry entry : manifest.files) {
                    System.out.printf(
                            "%s  %12s  %d chunks%n",
                            entry.relativePath,
                            human(entry.size),
                            entry.chunks);
                }

                return;
            }

            if (deleteExtra) {
                deleteExtraFiles(manifest);
            }

            AtomicInteger next =
                    new AtomicInteger(0);

            List<Future<?>> futures =
                    new ArrayList<>();

            for (int i = 0; i < streams; i++) {

                futures.add(
                        workers.submit(() -> {

                            while (true) {

                                int index =
                                        next.getAndIncrement();

                                if (index >=
                                        manifest.files.size()) {
                                    return;
                                }

                                FileEntry entry =
                                        manifest.files.get(index);

                                try {
                                    copyWithRetry(entry);
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            }
                        }));
            }

            try {
                for (Future<?> future : futures) {
                    future.get();
                }
            } finally {
                workers.shutdownNow();
            }

            log("Completed files: "
                    + completedFiles.get()
                    + "/"
                    + manifest.files.size());

            log("Final rate: "
                    + human(adaptive.currentRate())
                    + "/s");
        }

        private void copyWithRetry(
                FileEntry entry) throws Exception {

            int attempt = 0;

            while (true) {
                try {
                    copyFile(entry);
                    return;
                } catch (Exception e) {

                    attempt++;

                    log("ERROR "
                            + entry.relativePath
                            + ": "
                            + e.getMessage());

                    if (retries > 0
                            && attempt >= retries) {
                        throw e;
                    }

                    if (retryDelay > 0) {
                        Thread.sleep(retryDelay);
                    }

                    log("Retrying "
                            + entry.relativePath
                            + " attempt "
                            + (attempt + 1));
                }
            }
        }

        private void copyFile(
                FileEntry entry) throws Exception {

            Path sourceFile =
                    resolveSource(entry.relativePath);

            Path destinationFile =
                    resolveDestination(entry.relativePath);

            Files.createDirectories(
                    destinationFile.getParent());

            /*
             * If overwrite=false, an existing destination file
             * is left untouched.
             */
            if (!overwrite
                    && Files.exists(destinationFile)) {
                log("SKIP "
                        + entry.relativePath
                        + " because destination exists");
                return;
            }

            BitSet completed =
                    resume
                            ? resumeState.load(
                                    entry.relativePath,
                                    entry.chunks)
                            : new BitSet(entry.chunks);

            /*
             * If state says everything is complete, validate the
             * destination size before trusting the state.
             */
            if (completed.cardinality()
                    == entry.chunks
                    && Files.exists(destinationFile)
                    && Files.size(destinationFile)
                    == entry.size) {

                if (verify) {
                    if (!Arrays.equals(
                            sha256(sourceFile),
                            sha256(destinationFile))) {
                        completed.clear();
                    } else {
                        completedFiles.incrementAndGet();
                        return;
                    }
                } else {
                    completedFiles.incrementAndGet();
                    return;
                }
            }

            long fileStart =
                    System.nanoTime();

            try (FileChannel sourceChannel =
                         FileChannel.open(
                                 sourceFile,
                                 StandardOpenOption.READ);

                 FileChannel destinationChannel =
                         FileChannel.open(
                                 destinationFile,
                                 StandardOpenOption.CREATE,
                                 StandardOpenOption.READ,
                                 StandardOpenOption.WRITE)) {

                /*
                 * Establish the expected destination length before
                 * writing chunks. This makes restart state much easier
                 * to validate.
                 */
                if (entry.size == 0) {
                    destinationChannel.truncate(0);
                } else {
                    destinationChannel.position(
                            entry.size - 1);

                    destinationChannel.write(
                            ByteBuffer.wrap(
                                    new byte[]{0}));

                    destinationChannel.truncate(entry.size);
                }

                RateLimiter limiter =
                        new RateLimiter(adaptive);

                int chunkIndex =
                        completed.nextClearBit(0);

                while (chunkIndex < entry.chunks) {

                    long offset =
                            (long) chunkIndex * chunkSize;

                    int length =
                            (int) Math.min(
                                    chunkSize,
                                    entry.size - offset);

                    if (entry.size == 0) {
                        length = 0;
                    }

                    ByteBuffer buffer =
                            ByteBuffer.allocateDirect(
                                    (int) Math.min(
                                            bufferSize,
                                            Math.max(
                                                    1,
                                                    length)));

                    long chunkStart =
                            System.nanoTime();

                    if (length > 0) {

                        sourceChannel.position(offset);

                        buffer.clear();
                        buffer.limit(length);

                        while (buffer.hasRemaining()) {

                            int read =
                                    sourceChannel.read(buffer);

                            if (read < 0) {
                                throw new EOFException(
                                        "Unexpected EOF: "
                                                + sourceFile);
                            }
                        }

                        buffer.flip();

                        /*
                         * Global aggregate bandwidth control.
                         */
                        limiter.acquire(length);

                        destinationChannel.position(offset);

                        while (buffer.hasRemaining()) {
                            destinationChannel.write(buffer);
                        }
                    }

                    /*
                     * Ensure the chunk is handed to the filesystem
                     * before recording it as completed.
                     */
                    destinationChannel.force(false);

                    completed.set(chunkIndex);

                    if (resume) {
                        resumeState.save(
                                entry.relativePath,
                                completed);
                    }

                    long elapsed =
                            System.nanoTime()
                                    - chunkStart;

                    adaptive.sample(
                            Math.max(1, length),
                            elapsed);

                    completedBytes.addAndGet(length);

                    if (!quiet) {
                        showProgress(
                                entry,
                                completed,
                                fileStart);
                    }

                    chunkIndex =
                            completed.nextClearBit(
                                    chunkIndex + 1);
                }
            }

            if (verify) {

                byte[] sourceHash =
                        sha256(sourceFile);

                byte[] destinationHash =
                        sha256(destinationFile);

                if (!Arrays.equals(
                        sourceHash,
                        destinationHash)) {

                    throw new IOException(
                            "SHA-256 verification failed: "
                                    + entry.relativePath);
                }
            }

            if (preserveTime) {
                Files.setLastModifiedTime(
                        destinationFile,
                        FileTime.fromMillis(
                                entry.modified));
            }

            resumeState.remove(
                    entry.relativePath);

            completedFiles.incrementAndGet();

            log("DONE "
                    + entry.relativePath
                    + " ("
                    + human(entry.size)
                    + ")");
        }

        private Path resolveSource(
                String relativePath) {

            if (Files.isDirectory(source)) {
                return source.resolve(relativePath);
            }

            return source;
        }

        private Path resolveDestination(
                String relativePath) {

            if (Files.isDirectory(source)) {
                return destination.resolve(relativePath);
            }

            Path sourceName =
                    Paths.get(relativePath).getFileName();

            return destination.resolve(sourceName);
        }

        private void showProgress(
                FileEntry entry,
                BitSet completed,
                long start) {

            long estimatedBytes =
                    Math.min(
                            entry.size,
                            (long) completed.cardinality()
                                    * chunkSize);

            double percent =
                    entry.size == 0
                            ? 100.0
                            : estimatedBytes
                                    * 100.0
                                    / entry.size;

            double seconds =
                    (System.nanoTime() - start)
                            / 1_000_000_000.0;

            long currentRate =
                    seconds <= 0
                            ? 0
                            : (long)
                                    (estimatedBytes
                                            / seconds);

            System.out.printf(
                    "\r%-60s %6.2f%%  %10s/s  target %10s/s",
                    trim(entry.relativePath, 60),
                    percent,
                    human(currentRate),
                    human(adaptive.currentRate()));

            if (completed.cardinality()
                    == entry.chunks) {
                System.out.println();
            }
        }

        private void deleteExtraFiles(
                Manifest manifest) throws IOException {

            if (!Files.isDirectory(destination)) {
                return;
            }

            Set<String> expected =
                    new HashSet<>();

            for (FileEntry entry : manifest.files) {
                expected.add(entry.relativePath);
            }

            try (var stream =
                         Files.walk(destination)) {

                stream
                        .sorted(Comparator.reverseOrder())
                        .filter(Files::isRegularFile)
                        .forEach(path -> {

                            String relative =
                                    destination
                                            .relativize(path)
                                            .toString()
                                            .replace('\\', '/');

                            if (relative.equals(".niostream")
                                    || relative.startsWith(
                                            ".niostream/")) {
                                return;
                            }

                            if (!expected.contains(relative)) {
                                try {
                                    Files.deleteIfExists(path);
                                    log("DELETE " + relative);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                        });

            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
    }

    static byte[] sha256(Path file)
            throws Exception {

        MessageDigest digest =
                MessageDigest.getInstance("SHA-256");

        try (var input =
                     Files.newInputStream(file)) {

            byte[] buffer =
                    new byte[4 * 1024 * 1024];

            int read;

            while ((read =
                    input.read(buffer)) != -1) {

                digest.update(
                        buffer,
                        0,
                        read);
            }
        }

        return digest.digest();
    }

    static String human(long value) {

        if (value < 1024) {
            return value + " B";
        }

        double number = value;

        String[] units =
                {"KB", "MB", "GB", "TB"};

        int index = 0;

        while (number >= 1024
                && index < units.length - 1) {

            number /= 1024;
            index++;
        }

        return String.format(
                Locale.ROOT,
                "%.1f %s",
                number,
                units[index]);
    }

    static String trim(
            String value,
            int max) {

        if (value.length() <= max) {
            return value;
        }

        return value.substring(
                0,
                max - 3)
                + "...";
    }

    static void log(String message) {
        System.out.println(
                "\n"
                        + java.time.LocalDateTime.now()
                        + " "
                        + message);
    }
}
