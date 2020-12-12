# FSClientCache
This is the code repo for the file system client-side cache paper titled 'Improving In-Memory File System Reading Performance by Fine-Grained User-Space Cache Mechanisms'.

This project is built on the famous open source big data storage system [Alluxio](https://github.com/Alluxio/alluxio). We have added the client-side cache related feature into that. 

The detailed setup and running methods are the same as Alluxio, you can refer to [Alluxio Documentations](https://docs.alluxio.io/os/user/stable/en/Overview.html).

## Prerequisites
- Java 1.8
- maven 3.5.4

## Installation
```bash
mvn clean install -Dmaven.javadoc.skip=true -D skipTests -D license.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true
```

## Test
```bash

```

## Workloads
We use real-world workload from Microsoft Research.
You can download the Microsoft Research workload in [here](http://iotta.snia.org/traces/388).

### Analysis on Microsoft Workload
bla
