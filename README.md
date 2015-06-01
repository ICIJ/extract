# Extract #

A cross-platform command line tool for parellelised, distributed content-analysis. Built on top of [Apache Tika](https://tika.apache.org/).

Extract streams the output from Tika instead of bufferring it all into memory before writing. This allows it to operate on very large files without memory issues.

It supports Redis-backed queueing for distributed extraction and will write to Solr, plain text files or standard output.

## Workflows ##

If you're only processing a few thousand files, then running a single instance of Extract without a queue is sufficient.

`workstation-1$ extract spew -d /path/to/files -r redis -o file --file-output-directory /path/to/text`

The `-r` parameter is used to tell Extract to save the result of each file processed to Redis. In this way, if you have to stop the process, then you can resume where you left off as successfully processed files will be skipped.

### Distributed text extraction ###

This is the workflow we use at ICIJ for processing millions of files. The `-n` parameter is used to namespace the job and avoid conflicts with unrelated jobs using the same Redis server.

 - First, queue the files from your directory.  
`workstation-1$ extract queue -n job-1 -d /path/to/files -q redis -v info --redis-address redis-1:6379 2> queue.log`

 - Dump the queue to a backup file in case we need to restore it later on.  
`workstation-1$ extract dump-queue -n job-1 --redis-address redis-1:6379 > queue.json`

 - Start processing the queue on each of your machines.  
`workstation-2$ extract spew -n job-1 -q redis -o solr -s https://solr-1:8983/solr/core1 -i id -r redis -v info --redis-address redis-1:6379 2> extract.log`  
`workstation-3$ ...`

In the last step, we instruct Extract to use the queue from Redis, to output extracted text to Solr (`-o solr`) at the given address, to automatically generate an ID for each path (`-i id`), and to report results to Redis (`-r redis`).

## Compiling ##

Requires [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and Maven:

```bash
cd extract/
mvn install
```

The executables are packaged using [Capsule](https://github.com/puniverse/capsule). Look in `target/` for the appropriate executable for your platform.

## Credits and collaboration ##

Developed by [Matthew Caruana Galizia](https://twitter.com/mcaruanagalizia) at the [ICIJ](http://www.icij.org/).

## License ##

Copyright (c) 2015 The Center for Public Integrity®. See `LICENSE`.
