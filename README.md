# nifi-directory-watcher

nifi-directory-watcher is a [NiFi](https://nifi.apache.org) extension that makes it possible to
watch a directory for file changes. This implementation, unlike ListFile, makes use of [Java NIO2
WatchService](https://docs.oracle.com/javase/7/docs/api/java/nio/file/WatchService.html). 
A drawback with the WatchService is that it doesn't support subdirectories.

This processor supports watching multiple directories at the same time.