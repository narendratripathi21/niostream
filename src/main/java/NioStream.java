
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.zip.*;
import java.time.Instant;
import java.util.concurrent.locks.LockSupport;

/**
 * NioStream - resumable, parallel, adaptive, selector-based file/directory transfer.
 *
 * Java 17+
 *
 * Sender:
 *   java NioStream send --host 10.0.0.2 --port 5000 --source /data/bigdir \
 *       --streams 6 --chunk 4M --rate 20M
 *
 * Receiver:
 *   java NioStream receive --port 5000 --dest /backup
 *
 * Features:
 *   1 reconnect
 *   2 resume
 *   4 directory/multiple-file transfer
 *   6 parallel streams
 *   7 adaptive bandwidth based on RTT / transfer timing
 *   8 java.nio Selector based non-blocking sockets
 *   10 manifest + per-file resume state
 *
 * Named options:
 *   --host HOST          sender target
 *   --port N              default 5000
 *   --source PATH
 *   --dest PATH
 *   --streams N           default 4
 *   --chunk SIZE          e.g. 1M, 4M, 64K; default 4M
 *   --rate SIZE           initial aggregate rate, e.g. 20M; 0=unlimited
 *   --min-rate SIZE       adaptive floor; default 1M
 *   --max-rate SIZE       adaptive ceiling; default = --rate, or 1G if rate=0
 *   --connect-timeout MS  default 10000
 *   --retry MS             default 1000
 *   --retries N            default 0 (unlimited)
 *   --state PATH           receiver state directory; default <dest>/.niostream
 *   --keep-state           keep state after successful completion
 *   --verify               SHA-256 verify every completed file
 *   --quiet
 */
public class NioStream {
    static final int MAGIC = 0x4E535431; // NST1
    static final byte HELLO = 1, MANIFEST = 2, REQUEST = 3, DATA = 4,
                       ACK = 5, DONE = 6, ERROR = 7, CLOSE = 8;
    static final int VERSION = 1;
    static final long DEFAULT_CHUNK = 4L * 1024 * 1024;
    static final long MAX_FRAME = 128L * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args[0].equals("--help") || args[0].equals("-h")) {
            usage(); return;
        }
        String mode = args[0];
        Args a = Args.parse(Arrays.copyOfRange(args, 1, args.length));
        if ("send".equalsIgnoreCase(mode)) new Sender(a).run();
        else if ("receive".equalsIgnoreCase(mode) || "recv".equalsIgnoreCase(mode)) new Receiver(a).run();
        else throw new IllegalArgumentException("Mode must be send or receive");
    }

    static void usage() {
        System.out.println("""
        NioStream
          receive --port 5000 --dest /backup [--streams 6] [--chunk 4M] [--verify]
          send    --host 10.0.0.2 --port 5000 --source /data/foo
                  [--streams 6] [--chunk 4M] [--rate 20M]
                  [--min-rate 1M] [--max-rate 100M]
                  [--retry 1000] [--retries 0] [--state PATH]
                  [--keep-state] [--verify] [--quiet]

        Size suffixes: K, M, G, T (decimal or IEC-ish values accepted).
        """);
    }

    static final class Args {
        final Map<String,String> m = new HashMap<>();
        static Args parse(String[] xs) {
            Args a = new Args();
            for (int i=0; i<xs.length; i++) {
                String s = xs[i];
                if (!s.startsWith("--")) throw new IllegalArgumentException("Expected --name");
                s = s.substring(2);
                int eq = s.indexOf('=');
                if (eq >= 0) a.m.put(s.substring(0,eq), s.substring(eq+1));
                else {
                    if (i + 1 < xs.length && !xs[i+1].startsWith("--")) a.m.put(s, xs[++i]);
                    else a.m.put(s, "true");
                }
            }
            return a;
        }
        String get(String k, String d) { return m.getOrDefault(k,d); }
        boolean bool(String k, boolean d) { return Boolean.parseBoolean(get(k, String.valueOf(d))); }
        int integer(String k, int d) { return Integer.parseInt(get(k, String.valueOf(d))); }
        long size(String k, long d) { return parseSize(get(k, String.valueOf(d))); }
    }

    static long parseSize(String s) {
        s = s.trim().toUpperCase(Locale.ROOT);
        long mul = 1;
        if (s.endsWith("K")) { mul=1024L; s=s.substring(0,s.length()-1); }
        else if (s.endsWith("M")) { mul=1024L*1024; s=s.substring(0,s.length()-1); }
        else if (s.endsWith("G")) { mul=1024L*1024*1024; s=s.substring(0,s.length()-1); }
        else if (s.endsWith("T")) { mul=1024L*1024*1024*1024; s=s.substring(0,s.length()-1); }
        return (long)Double.parseDouble(s) * mul;
    }

    static final class FileEntry {
        final int id; final String rel; final long size; final long mtime; final int chunks;
        final byte[] sha256;
        FileEntry(int id, String rel, long size, long mtime, int chunks, byte[] sha256) {
            this.id=id; this.rel=rel; this.size=size; this.mtime=mtime; this.chunks=chunks; this.sha256=sha256;
        }
    }

    static List<FileEntry> buildManifest(Path source, long chunk, boolean verify) throws Exception {
        List<Path> files = new ArrayList<>();
        if (Files.isRegularFile(source)) files.add(source);
        else try (var st = Files.walk(source)) {
            st.filter(Files::isRegularFile).sorted().forEach(files::add);
        }
        Path base = Files.isDirectory(source) ? source : source.getParent();
        if (base == null) base = Paths.get(".");
        List<FileEntry> out = new ArrayList<>();
        int id=0;
        for (Path p : files) {
            String rel = Files.isDirectory(source) ? base.relativize(p).toString() : p.getFileName().toString();
            long size=Files.size(p), mt=Files.getLastModifiedTime(p).toMillis();
            int chunks=(int)Math.max(1, (size + chunk - 1)/chunk);
            byte[] hash=verify ? sha256(p) : new byte[0];
            out.add(new FileEntry(id++, rel.replace('\\','/'), size, mt, chunks, hash));
        }
        return out;
    }

    static byte[] sha256(Path p) throws Exception {
        MessageDigest md=MessageDigest.getInstance("SHA-256");
        try (InputStream in=Files.newInputStream(p)) {
            byte[] b=new byte[1024*1024]; int n;
            while((n=in.read(b))!=-1) md.update(b,0,n);
        }
        return md.digest();
    }

    static void writeManifest(DataOutputStream out, List<FileEntry> fs, long chunk, boolean verify) throws IOException {
        out.writeInt(MAGIC); out.writeInt(VERSION); out.writeLong(chunk); out.writeBoolean(verify); out.writeInt(fs.size());
        for (FileEntry f:fs) {
            out.writeInt(f.id); out.writeUTF(f.rel); out.writeLong(f.size); out.writeLong(f.mtime);
            out.writeInt(f.chunks); out.writeInt(f.sha256.length); out.write(f.sha256);
        }
    }

    static List<FileEntry> readManifest(DataInputStream in, long[] chunkOut, boolean[] verifyOut) throws IOException {
        if(in.readInt()!=MAGIC || in.readInt()!=VERSION) throw new IOException("Bad NioStream manifest");
        chunkOut[0]=in.readLong(); verifyOut[0]=in.readBoolean();
        int n=in.readInt(); if(n<0 || n>10_000_000) throw new IOException("Bad file count");
        List<FileEntry> fs=new ArrayList<>(n);
        for(int i=0;i<n;i++){
            int id=in.readInt(); String rel=in.readUTF(); long size=in.readLong(), mt=in.readLong();
            int chunks=in.readInt(), hn=in.readInt(); if(hn<0 || hn>64) throw new IOException("Bad hash");
            byte[] h=in.readNBytes(hn);
            fs.add(new FileEntry(id,rel,size,mt,chunks,h));
        }
        return fs;
    }

    static final class ResumeState {
        final Path dir;
        final Map<Integer, BitSet> done = new ConcurrentHashMap<>();
        ResumeState(Path dir){this.dir=dir;}
        Path file(int id){return dir.resolve("f-"+id+".state");}
        BitSet load(int id, int chunks) {
            try {
                Path p=file(id); if(!Files.exists(p)) return new BitSet(chunks);
                byte[] b=Files.readAllBytes(p);
                BitSet bs=BitSet.valueOf(b);
                bs.clear(chunks, Math.max(chunks, bs.length()));
                return bs;
            } catch(Exception e){ return new BitSet(chunks); }
        }
        synchronized void save(int id, BitSet bs) throws IOException {
            Files.createDirectories(dir);
            Path tmp=dir.resolve("f-"+id+".tmp");
            Files.write(tmp, bs.toByteArray(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp,file(id),StandardCopyOption.REPLACE_EXISTING,StandardCopyOption.ATOMIC_MOVE);
        }
        void clear() throws IOException {
            if(!Files.exists(dir)) return;
            try(var st=Files.list(dir)) { st.forEach(p->{try{Files.deleteIfExists(p);}catch(IOException ignored){}}); }
            Files.deleteIfExists(dir);
        }
    }

    static final class AdaptiveRate {
        final long min,max; final AtomicLong rate;
        final AtomicLong totalBytes=new AtomicLong(), totalNanos=new AtomicLong();
        AdaptiveRate(long initial,long min,long max) {
            this.min=Math.max(1,min); this.max=Math.max(this.min,max);
            this.rate=new AtomicLong(initial<=0?this.max:Math.min(this.max,Math.max(this.min,initial)));
        }
        long rate(){return rate.get();}
        void sample(long bytes,long nanos) {
            if(bytes<=0 || nanos<=0) return;
            totalBytes.addAndGet(bytes); totalNanos.addAndGet(nanos);
            double sec=nanos/1e9;
            double observed=bytes/sec;
            long r=rate.get();
            // Increase cautiously when observed throughput is healthy; reduce when the
            // transfer takes materially longer than the current pacing interval.
            if(observed > r*0.85) rate.compareAndSet(r, Math.min(max,(long)(r*1.10)));
            else if(observed < r*0.55) rate.compareAndSet(r, Math.max(min,(long)(r*0.80)));
        }
    }

    static final class RateLimiter {
        final AdaptiveRate adaptive;
        final AtomicLong nextNanos = new AtomicLong();
        RateLimiter(AdaptiveRate a){adaptive=a;}
        void acquire(int bytes) {
            if(adaptive.max<=0) return;
            long now=System.nanoTime(), r=adaptive.rate();
            long duration=(long)((bytes/(double)r)*1e9);
            long start=nextNanos.getAndUpdate(x -> Math.max(x,now)+duration);
            long sleep=start-now;
            if(sleep>0) LockSupport.parkNanos(sleep);
        }
    }

    static final class SelectorConnection implements Closeable {
        final SocketChannel ch; final Selector selector;
        SelectorConnection(SocketChannel ch) throws Exception {
            this.ch=ch; this.ch.configureBlocking(false);
            selector=Selector.open(); ch.register(selector,SelectionKey.OP_READ);
        }
        void writeFully(ByteBuffer b) throws Exception {
            while(b.hasRemaining()){
                int n=ch.write(b);
                if(n==0){ selector.select(1000); }
                else if(n<0) throw new EOFException();
            }
        }
        ByteBuffer readFully(int n) throws Exception {
            ByteBuffer b=ByteBuffer.allocate(n);
            while(b.hasRemaining()){
                int x=ch.read(b);
                if(x<0) throw new EOFException();
                if(x==0) selector.select(1000);
            }
            b.flip(); return b;
        }
        void writeFrame(byte type, byte[] payload) throws Exception {
            ByteBuffer h=ByteBuffer.allocate(9).putInt(MAGIC).put(type).putInt(payload.length);
            h.flip(); writeFully(h); writeFully(ByteBuffer.wrap(payload));
        }
        byte[] readFrame() throws Exception {
            ByteBuffer h=readFully(9);
            if(h.getInt()!=MAGIC) throw new IOException("Bad frame");
            byte t=h.get(); int n=h.getInt();
            if(n<0 || n>MAX_FRAME) throw new IOException("Frame too large");
            byte[] p=new byte[n]; readFully(n).get(p);
            byte[] r=new byte[n+1]; r[0]=t; System.arraycopy(p,0,r,1,n); return r;
        }
        public void close(){try{selector.close();}catch(Exception ignored){} try{ch.close();}catch(Exception ignored){}}
    }

    static byte[] packRequest(int fileId,int chunk) {
        ByteBuffer b=ByteBuffer.allocate(8).putInt(fileId).putInt(chunk); return b.array();
    }
    static int[] unpackRequest(byte[] p) {
        ByteBuffer b=ByteBuffer.wrap(p); return new int[]{b.getInt(),b.getInt()};
    }

    static final class Sender {
        final Args a;
        final int port, streams;
        final long chunk, retry;
        final int retries;
        final boolean verify, quiet;
        final AdaptiveRate adaptive;
        final ExecutorService pool;
        List<FileEntry> manifest;
        Path source;
        Sender(Args a){
            this.a=a; port=a.integer("port",5000); streams=Math.max(1,a.integer("streams",4));
            chunk=a.size("chunk",DEFAULT_CHUNK); retry=a.size("retry",1000); retries=a.integer("retries",0);
            verify=a.bool("verify",false); quiet=a.bool("quiet",false);
            long initial=a.size("rate",0), min=a.size("min-rate",1024*1024L), max=a.size("max-rate",initial>0?initial:1024L*1024*1024);
            adaptive=new AdaptiveRate(initial,min,max); pool=Executors.newFixedThreadPool(streams);
        }
        void run() throws Exception {
            source=Paths.get(a.get("source", ""));
            if(!Files.exists(source)) throw new FileNotFoundException(source.toString());
            manifest=buildManifest(source,chunk,verify);
            if(manifest.isEmpty()){log("Nothing to send");return;}
            log("Files: "+manifest.size()+", streams: "+streams+", chunk: "+chunk+", rate: "+adaptive.rate()+"/s");
            for(int attempt=1;;attempt++){
                try {
                    connectAndTransfer();
                    pool.shutdown(); return;
                } catch(Exception e) {
                    log("Connection/transfer interrupted: "+e);
                    if(retries>0 && attempt>=retries) throw e;
                    Thread.sleep(retry);
                }
            }
        }
        void connectAndTransfer() throws Exception {
            String host=a.get("host",null); if(host==null) throw new IllegalArgumentException("--host required");
            List<SocketChannel> sockets=new ArrayList<>();
            try {
                // One control connection carries the manifest. Each data stream is a
                // separate non-blocking SelectorConnection, allowing parallel chunk flow.
                SocketChannel control=SocketChannel.open();
                control.configureBlocking(true); control.connect(new InetSocketAddress(host,port));
                DataOutputStream dout=new DataOutputStream(Channels.newOutputStream(control));
                DataInputStream din=new DataInputStream(Channels.newInputStream(control));
                dout.writeInt(MAGIC); dout.writeInt(VERSION); dout.writeByte(HELLO); dout.flush();
                writeManifest(dout,manifest,chunk,verify); dout.flush();
                int accepted=din.readInt();
                if(accepted!=streams) log("Receiver accepted "+accepted+" streams");
                control.close();

                CompletionService<Boolean> cs=new ExecutorCompletionService<>(pool);
                for(int i=0;i<streams;i++) cs.submit(()->dataWorker(host));
                for(int i=0;i<streams;i++) cs.take().get();
                log("Transfer complete. final adaptive rate="+adaptive.rate()+"/s");
            } finally { for(SocketChannel s:sockets) try{s.close();}catch(Exception ignored){} }
        }
        boolean dataWorker(String host) throws Exception {
            for(int attempt=1;;attempt++){
                try {
                    SocketChannel ch=SocketChannel.open();
                    ch.configureBlocking(false);
                    ch.connect(new InetSocketAddress(host,port));
                    Selector sel=Selector.open(); ch.register(sel,SelectionKey.OP_CONNECT);
                    while(!ch.finishConnect()){sel.select(1000);}
                    ch.configureBlocking(false);
                    // Worker asks receiver for a chunk. Receiver's scheduler supplies the
                    // next missing chunk; this makes reconnect/resume naturally idempotent.
                    sendHello(ch,sel,DATA);
                    Set<String> localDone=new HashSet<>();
                    while(true){
                        long t0=System.nanoTime();
                        byte[] frame=readFrame(ch,sel);
                        byte type=frame[0];
                        if(type==DONE) break;
                        if(type!=REQUEST) throw new IOException("Expected REQUEST");
                        int[] rq=unpackRequest(Arrays.copyOfRange(frame,1,frame.length));
                        FileEntry f=manifest.get(rq[0]); int ci=rq[1];
                        if(localDone.contains(rq[0]+":"+ci)) continue;
                        long off=(long)ci*chunk; int len=(int)Math.min(chunk,f.size-off);
                        Path p=sourceFor(f.rel);
                        byte[] data=readAt(p,off,len);
                        ByteArrayOutputStream bo=new ByteArrayOutputStream(24+len);
                        DataOutputStream o=new DataOutputStream(bo);
                        o.writeInt(f.id); o.writeInt(ci); o.writeLong(off); o.writeInt(len);
                        o.write(data);
                        if(verify) o.write(sha256(data)); else o.write(new byte[0]);
                        RateLimiter limiter=new RateLimiter(adaptive);
                        limiter.acquire(len);
                        writeFrame(ch,sel,DATA,bo.toByteArray());
                        byte[] ack=readFrame(ch,sel);
                        if(ack[0]!=ACK) throw new IOException("Expected ACK");
                        long elapsed=System.nanoTime()-t0;
                        adaptive.sample(len,elapsed);
                        localDone.add(rq[0]+":"+ci);
                    }
                    sel.close(); ch.close(); return true;
                } catch(Exception e){
                    if(retries>0 && attempt>=retries) throw e;
                    Thread.sleep(retry);
                }
            }
        }
        Path sourceFor(String rel){
            return Files.isDirectory(source) ? source.resolve(rel) : source;
        }
        static byte[] readAt(Path p,long off,int len)throws Exception{
            ByteBuffer b=ByteBuffer.allocate(len);
            try(FileChannel fc=FileChannel.open(p,StandardOpenOption.READ)){
                fc.position(off); while(b.hasRemaining()){int n=fc.read(b);if(n<0)throw new EOFException();}
            }
            return b.array();
        }
    }

    static final class Receiver {
        final Args a; final int port, streams; final long retry; final boolean verify,quiet,keep;
        final Path dest,stateDir;
        final ExecutorService workers;
        final Map<Integer,BitSet> done=new ConcurrentHashMap<>();
        List<FileEntry> manifest;
        long chunk;
        Receiver(Args a){
            this.a=a;port=a.integer("port",5000);streams=Math.max(1,a.integer("streams",4));
            retry=a.size("retry",1000);verify=a.bool("verify",false);quiet=a.bool("quiet",false);keep=a.bool("keep-state",false);
            dest=Paths.get(a.get("dest",".")); stateDir=Paths.get(a.get("state",dest.resolve(".niostream").toString()));
            workers=Executors.newFixedThreadPool(streams);
        }
        void run() throws Exception {
            Files.createDirectories(dest); Files.createDirectories(stateDir);
            try(ServerSocketChannel server=ServerSocketChannel.open()){
                server.bind(new InetSocketAddress(a.integer("port",5000)));
                log("Listening on "+port+" -> "+dest);
                // First connection is control/manifest.
                SocketChannel c=server.accept();
                DataInputStream in=new DataInputStream(Channels.newInputStream(c));
                if(in.readInt()!=MAGIC || in.readInt()!=VERSION || in.readByte()!=HELLO) throw new IOException("Bad HELLO");
                long[] co={0}; boolean[] vo={false}; manifest=readManifest(in,co,vo); chunk=co[0];
                if(a.bool("verify",vo[0])) verify=true;
                for(FileEntry f:manifest) done.put(f.id,new ResumeState(stateDir).load(f.id,f.chunks));
                DataOutputStream out=new DataOutputStream(Channels.newOutputStream(c));out.writeInt(streams);out.flush();c.close();
                log("Manifest: "+manifest.size()+" files");
                CountDownLatch latch=new CountDownLatch(streams);
                for(int i=0;i<streams;i++) workers.submit(()->{
                    try{dataConnection(server.accept());}catch(Exception e){log("worker: "+e);}finally{latch.countDown();}
                });
                latch.await();
                if(allDone()){
                    if(!keep) new ResumeState(stateDir).clear();
                    log("All files received.");
                }
            } finally {workers.shutdownNow();}
        }
        void dataConnection(SocketChannel ch)throws Exception{
            ch.configureBlocking(false); Selector sel=Selector.open(); ch.register(sel,SelectionKey.OP_READ);
            byte[] h=readFrame(ch,sel); if(h[0]!=DATA) throw new IOException("Bad worker HELLO");
            while(true){
                int[] rq=nextMissing();
                if(rq==null){writeFrame(ch,sel,DONE,new byte[0]);break;}
                writeFrame(ch,sel,REQUEST,packRequest(rq[0],rq[1]));
                byte[] fr=readFrame(ch,sel);if(fr[0]!=DATA)throw new IOException("Expected DATA");
                applyData(fr); writeFrame(ch,sel,ACK,packRequest(rq[0],rq[1]));
            }
            sel.close();ch.close();
        }
        int[] nextMissing(){
            for(FileEntry f:manifest){
                BitSet b=done.get(f.id);
                int x=b.nextClearBit(0);
                if(x<f.chunks) return new int[]{f.id,x};
            }
            return null;
        }
        synchronized void applyData(byte[] fr)throws Exception{
            DataInputStream in=new DataInputStream(new ByteArrayInputStream(fr,1,fr.length-1));
            int fid=in.readInt(), ci=in.readInt(); long off=in.readLong(); int len=in.readInt();
            byte[] data=in.readNBytes(len); int hn=in.readInt(); byte[] hash=in.readNBytes(hn);
            FileEntry f=manifest.get(fid);
            if(ci<0||ci>=f.chunks||off!=(long)ci*chunk||len<0||len>chunk)throw new IOException("Invalid chunk");
            if(verify && hn>0 && !Arrays.equals(hash,sha256(data))) throw new IOException("Chunk checksum failed");
            Path p=dest.resolve(f.rel).normalize();
            if(!p.startsWith(dest.normalize())) throw new IOException("Path traversal");
            Files.createDirectories(p.getParent());
            try(FileChannel fc=FileChannel.open(p,StandardOpenOption.CREATE,StandardOpenOption.WRITE)){
                fc.position(off);ByteBuffer b=ByteBuffer.wrap(data);while(b.hasRemaining())fc.write(b);
            }
            BitSet bs=done.get(fid);bs.set(ci);new ResumeState(stateDir).save(fid,bs);
            if(bs.cardinality()==f.chunks){
                Files.setLastModifiedTime(p,FileTime.fromMillis(f.mtime));
                if(verify && f.sha256.length>0 && !Arrays.equals(f.sha256,sha256(p))) throw new IOException("File SHA-256 mismatch: "+f.rel);
                log("Completed: "+f.rel);
            }
        }
        boolean allDone(){
            for(FileEntry f:manifest)if(done.get(f.id).cardinality()!=f.chunks)return false;
            return true;
        }
    }

    // Frame helpers for non-blocking Selector connections.
    static void sendHello(SocketChannel ch,Selector sel,byte type)throws Exception{
        writeFrame(ch,sel,type,new byte[0]);
    }
    static void writeFrame(SocketChannel ch,Selector sel,byte type,byte[] p)throws Exception{
        ByteBuffer b=ByteBuffer.allocate(9+p.length);
        b.putInt(MAGIC).put(type).putInt(p.length).put(p).flip();
        while(b.hasRemaining()){
            int n=ch.write(b);if(n==0){sel.select(1000);for(var k:sel.selectedKeys())k.interestOps(SelectionKey.OP_WRITE);sel.selectedKeys().clear();}
            else if(n<0)throw new EOFException();
        }
    }
    static byte[] readFrame(SocketChannel ch,Selector sel)throws Exception{
        ByteBuffer h=ByteBuffer.allocate(9);
        readFully(ch,sel,h);h.flip();
        if(h.getInt()!=MAGIC)throw new IOException("Bad frame magic");
        byte t=h.get();int n=h.getInt();if(n<0||n>MAX_FRAME)throw new IOException("Bad frame length");
        ByteBuffer p=ByteBuffer.allocate(n);readFully(ch,sel,p);return ByteBuffer.allocate(n+1).put(t).put(p.flip()).array();
    }
    static void readFully(SocketChannel ch,Selector sel,ByteBuffer b)throws Exception{
        while(b.hasRemaining()){
            int n=ch.read(b);
            if(n<0)throw new EOFException();
            if(n==0){sel.select(1000);for(var k:sel.selectedKeys())k.interestOps(SelectionKey.OP_READ);sel.selectedKeys().clear();}
        }
    }
    static byte[] sha256(byte[] b)throws Exception{MessageDigest m=MessageDigest.getInstance("SHA-256");return m.digest(b);}
    static void log(String s){System.out.println(Instant.now()+" "+s);}
}
