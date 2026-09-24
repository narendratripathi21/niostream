# NioStream 2.0

NioStream is a Java 17 command-line utility designed for copying large files
and directory trees directly between two Windows SMB/UNC paths.

It runs as ONE process on a Windows Server:

    \\SOURCE-NAS\Share\Folder
              |
              | SMB
              v
       Windows Server
          NioStream
              |
              | SMB
              v
    \\DEST-NAS\Share\Folder

There is no Java sender process on the source NAS and no Java receiver process
on the destination NAS.

## Requirements

- Windows Server
- Java 17+
- Maven 3.8+
- Windows account with:
  - read permission on source share
  - modify/write permission on destination share

## Build

From the project directory:

    mvn clean package

The executable JAR is:

    target\niostream-2.0.0.jar

## Basic directory copy

    java -jar target\niostream-2.0.0.jar copy --source "\\NAS01\Media" --destination "\\NAS02\Backup\Media"

## Recommended first test

Start conservatively:

    java -jar target\niostream-2.0.0.jar copy --source "\\NAS01\Media" --destination "\\NAS02\Backup\Media" --streams 4 --chunk 16M --rate 20M --min-rate 5M --max-rate 50M --resume true --verify false

After confirming the copy works, increase the rate/streams.

## Large-file / congested-network configuration

    java -jar target\niostream-2.0.0.jar copy --source "\\NAS01\Media" --destination "\\NAS02\Backup\Media" --streams 8 --chunk 32M --rate 50M --min-rate 10M --max-rate 200M --resume true --verify true

## Single file

The destination is treated as a directory and the source filename is retained:

    java -jar target\niostream-2.0.0.jar copy --source "\\NAS01\Media\bigfile.iso" --destination "\\NAS02\Backup" --streams 4 --chunk 32M --resume true

## Dry run

    java -jar target\niostream-2.0.0.jar copy --source "\\NAS01\Media" --destination "\\NAS02\Backup\Media" --dry-run true

## Resume

The state directory defaults to:

    <destination>\.niostream

A state file contains a bitmap of completed chunks.

If a copy is interrupted:

1. Start the same command again.
2. Existing completed chunks are skipped.
3. Missing chunks are copied.
4. The state file is removed after successful completion.

You can put state on the local Windows Server instead:

    --state "C:\ProgramData\NioStream\state"

That is recommended if you don't want control/state files on the destination NAS.

## Retry

Unlimited retries:

    --retries 0 --retry-delay 5000

Five retries:

    --retries 5 --retry-delay 5000

Retries are at file level. Completed chunks remain in the resume bitmap.

## Verification

With:

    --verify true

NioStream calculates SHA-256 for source and destination after the file is
copied.

This provides strong end-to-end verification, but it requires another full
read of the source and destination file. Do not enable it for every transfer
if the NAS/network is already heavily loaded unless verification is required.

## Bandwidth adaptation

`--rate` establishes the starting aggregate transfer rate.

Example:

    --rate 50M --min-rate 10M --max-rate 100M

The controller increases the target gradually when observed throughput is
healthy and decreases it when observed throughput drops substantially.

This does NOT replace Windows/SMB/TCP congestion control. It simply prevents
NioStream from continuously adding application-level load.

## Parallelism

`--streams` controls the number of Java worker threads.

Start with:

    --streams 4

Then test:

    --streams 8

For a network that is already congested, more workers are not necessarily
faster. They can increase NAS queue depth and SMB contention.

## Selector

The previous TCP sender/receiver design used `java.nio.Selector`.

That is intentionally NOT used in this version.

For UNC-to-UNC copying, Java is not creating the network socket. Windows is
providing the SMB client underneath the filesystem API. A Java Selector cannot
control or multiplex that SMB traffic.

The relevant NIO APIs here are:

- Path
- Files
- FileChannel
- ByteBuffer
- FileTime

## Important operational notes

### Credentials

Do not put SMB passwords in the Java command line.

Run the Java process using a Windows account that already has access to both
shares, or establish the appropriate Windows SMB sessions before launching
NioStream.

For Windows scheduled tasks/services, use UNC paths rather than mapped drives.

### Source consistency

Do not copy files while applications are actively changing them unless you
have a snapshot/versioning mechanism.

The manifest records source size and modification time when scanning begins,
but NioStream is not a snapshot engine.

### Delete-extra

Do NOT enable this casually:

    --delete-extra true

It deletes destination files that are not in the source manifest.

Use it only when the destination is intended to mirror the source.

## Known design boundaries

This version intentionally does not implement:

- custom SMB protocol
- custom TCP transport
- BitTorrent-style seeding
- NAS-side agents
- compression
- encryption
- deduplication
- file locking
- snapshot creation

The purpose is a reliable Windows Server-based UNC-to-UNC transfer engine.
