# FSClientCache
This is the code repo for the file system client-side cache paper titled 'Improving In-Memory File System Reading Performance by Fine-Grained User-Space Cache Mechanisms'.

This project is built on the famous open source big data storage system [Alluxio](https://github.com/Alluxio/alluxio). We have added the client-side cache related feature into that. 

The detailed setup and running methods are the same as Alluxio, you can refer to [Alluxio Documentations](https://docs.alluxio.io/os/user/stable/en/Overview.html).

## Prerequisites
- Git
- Java 1.8
- Maven 3.5.4
- Alluxio 1.8

> NOTES:
> Please refer to [Alluxio's officical document](https://docs.alluxio.io/os/user/1.8/en/Getting-Started.html) about how to deploy Alluxio.

## Quick Start

Please make sure Alluxio is already deployed on your server before going to the next steps.
If your Alluxio is working normally,  executing `jps` command will show at least these processes:

```bash
$ jps
17045 Jps
15513 AlluxioWorker
15515 AlluxioProxy
15276 AlluxioMaster
15278 AlluxioSecondaryMaster
```

1. Clone Source Code
```bash
$ git clone https://github.com/PasaLab/FSClientCache
```

2. Build
```bash
$ mvn clean install -Dmaven.javadoc.skip=true -D skipTests -D license.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true
```

> It may take a long time to pull all the depencies in your first build. 


3. Create Target File
```bash
$ dd if=/dev/zero of=/tmp/test.20G bs=1G count=20
```

4. Mount Target File on Alluxio
```bash
$ alluxio fs mount /test.20G /tmp/test.20G
```

5. Run Test
```bash
$ java -cp examples/target/alluxio-examples-1.9.0-SNAPSHOT.jar alluxio.examples.cache.CacheBenchmark test.20G isk 1073741824 102400 evict 10
```

**Parameter Description:**

- `test.20G`: the path to file which you want to read

- `isk`: the policy you want to test, optional {`isk`, `gr`, `lru`, `lfu`, `arc`, `eva`}

- `1073741824`: the cache capacity in bytes, i.e. 1 GB

- `evict`: the cache mode, optional {`evict`, `promotion`}

- `10`: how much GigaBytes you want to read (1000 read per GigaBytes)

After test is finished, the statistics (including reading time, bytes hit ratio etc.) will be printed.

## Workloads

We use real-world workload from Microsoft Research.

You can download the Microsoft Research workload in [here](http://iotta.snia.org/traces/388).

### About Microsoft Workload

The Microsoft workload recorded one-week block accesses performed by 13 enterprise servers in Microsoft Research. It is composed of read/write requests whose offsets range from 0 to 840 GB.  More information about these is available in the [FAST 2008 paper](https://www.usenix.org/legacy/event/fast08/tech/narayanan.html).

In Microsoft traces, each trace file is named as `hostname_disknumber`. The hostnames are "friendly" names corresponding to the names used in that [paper](https://www.usenix.org/legacy/event/fast08/tech/narayanan.html). Disk numbers are logical block device numbers.

The characteristics of Microsoft traces are listed below:

| Trace Name | # Total Records | Server Description    |
| ---------- | --------------- | --------------------- |
| **proj**   | 599,716,005     | Project directories   |
| wdev       | 3,024,140       | Test web server       |
| ts         | 4,181,323       | Terminal server       |
| rsrch      | 3,508,103       | Research projects     |
| hm         | 11,183,061      | Hardware monitoring   |
| prxy       | 351,361,438     | Firewall/web proxy    |
| src2       | 28,997,811      | Source control        |
| web        | 78,662,064      | Web/SQL Server        |
| stg        | 28,538,432      | Web Staging           |
| mds        | 26,169,810      | Media Server          |
| prn        | 73,135,443      | Print server          |
| src1       | 818,619,317     | Source control        |
| usr        | 637,227,335     | User home directories |

Each record contains these fields:

```
Timestamp,Hostname,DiskNumber,Type,Offset,Size,ResponseTime
```

- **Timestamp** is the time the I/O was issued in "Windows filetime"
- **Hostname** is the hostname (should be the same as that in the trace file name)
- **DiskNumber** is the disknumber (should be the same as in the trace file name)
- **Type** is "Read" or "Write"
- **Offset** is the starting offset of the I/O in bytes from the start of the logical disk.
- **Size** is the transfer size of the I/O request in bytes.
- **ResponseTime** is the time taken by the I/O to complete, in Windows filetime
  units.

As a matter of fact, only **Offset** and **Size** are used to evaluate our cache performance.

### How We Use Microsoft Workload

In our experiments, we chose the `proj_0` trace considering the overall file size and the total size of requested data. Our analysis on the tarted trace shows that, the start address of reading ranges from **0** to **17 GB**, and the sizes of each reading range from **0.5 KB** to **724 KB**. Although all the access units are scattered in a space of nearly 20 GB, the size of the covered space is actually no more than **1.8 GB**. 

You can simply read a 20 GB file using this trace (only **Offset** and **Size** is used) to reproduce the experiments mentioned in out paper.

