# Extract

A cross-platform command line tool for parallelized, distributed content-analysis. Built on top of [Apache Tika](https://tika.apache.org/).

Extract streams the output from Tika instead of buffering it all into memory before writing. This allows it to operate on very large files without memory issues.

It supports Redis-backed queueing for distributed extraction and will write to Solr, plain text files or standard output.

## Workflow

If you're only processing a few thousand files, then running a single instance of Extract without a queue is sufficient:

`workstation-1$ extract spew -r redis -o file --file-output-directory /path/to/text /path/to/files`

The `-r` parameter is used to tell Extract to save the result of each file processed to Redis. In this way, if you have to stop the process, then you can resume where you left off as successfully processed files will be skipped.

### Distributed text extraction

This is the workflow we use at ICIJ for processing millions of files. The `-n` parameter is used to namespace the job
 and avoid conflicts with unrelated jobs using the same Redis server.

 - First, queue the files from your directory.  
`nfs-1$ extract queue -n job-1 -v info --redis-address redis-1:6379 /media/my_files 2> queue.log`

 - Export your directory as an NFS share.

 - Dump the queue to a backup file in case we need to restore it later on.  
 `nfs-1$ extract dump-queue -n job-1 --redis-address redis-1:6379 > queue.json`

 - Mount the NFS share to the same path on each of your extraction cluster machines.  
 `extract-1$ sudo mkdir /media/my_files`  
 `extract-1$ sudo mount -t nfs4 -o ro,proto=tcp,port=2049 nfs-1:/my_files /media/my_files`  
 `extract-2$ ...`

 - Start processing the queue on each of your machines.  
`extract-1$ extract spew -n job-1 -q redis -o solr -s https://solr-1:8983/solr/core1 -r redis -v info --redis-address redis-1:6379 2> extract.log`  
`extract-2$ ...`

In the last step, we instruct Extract to use the queue from Redis, to output extracted text to Solr (`-o solr`) at the given address and to report results to Redis (`-r redis`).

## Increasing the amount of memory available

Extract is set to pass whatever's in the `JAVA_OPTS` environment variable to the JVM. You can set this variable to increase the amount of memory available to it.

```bash
echo "export JAVA_OPTS=\"-Xms1024m -Xmx10240m\"" >> ~/.bashrc
source ~/.bashrc
```

From then on, Extract will have up to 10GB of memory available to it.

## Metadata

If you enable the metadata option, Extract adds a few of its own fields that we think are very useful.

 - `Content-Base-Type`: the `Content-Type` without any parameters. Useful for file type based faceting.
 - `Parent-Path`: the file's parent path. Useful for drill-down faceting when combined with Solr's `PathHierarchyTokenizerFactory`.

When outputting to Solr, all metadata field names are converted to lowercase and non-alphanumeric characters are converted to underscores.

## Cheating at reindexing after Solr schema changes

You might have made a mistake in your original schema and now need to change the type of a field, or changed the way it's tokenized. You can edit the schema and make as many changes as you like, but the original data would still be stored and indexed as specified in the old schema.

There are two ways you can work around this: reindex all your files again, or use the `solr-copy` command, which pulls the fields you specify from each document and adds them back to the same document, forcing reindexing.

A common example is when you change a string field to a `Trie` number field after indexing. Solr will then return an error message in place of these fields. To fix them automatically, run `solr-copy` filtering on the bad field.

```bash
extract solr-copy -f "my_numeric_field:* AND -my_numeric_field:[0 TO *]" -s ...
```

This will cause the copy command to run only on those fields which have a non-number value on the number-type field.

## Compiling

Requires [JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), Tesseract (in order to pass tests) and Maven:

```bash
cd extract/
mvn install
```

The executables are packaged using [Capsule](https://github.com/puniverse/capsule). Look in `target/` for the appropriate executable for your platform.

## Credits and collaboration

Developed by [Matthew Caruana Galizia](https://twitter.com/mcaruanagalizia) at the [ICIJ](http://www.icij.org/).

## License

Copyright (c) 2015 The Center for Public Integrity®. See `LICENSE`.
