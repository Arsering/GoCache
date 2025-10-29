# vmcache

Code repository for ICDE'26 paper GOCACHE: Accelerating Out-of-Core Graph Queries with Pattern-Driven Caching

## Environment Variables in the file called run.sh

* DB_PATH: path of raw file (e.g., /test/raw_file)
* FILE_SIZE_MB: size of raw file
* WORKER_NUM: number of threads
* POOL_NUM: number of GoCache's partition
* IO_SERVER_NUM: number of I/O engine (for io_uring)
* POOL_SIZE_MB: physical memory allocation in MByte, should be less than available RAM
* IO_SIZE_Byte: page size in Byte

## Example Command Lines

* `sudo bash run.sh`

## Dependencies and Configuration

We need the libio_uring library. On Ubuntu: `sudo apt install liburing-dev`.

## Citation

TBC

## Low-Hanging Fruit (TODO)
TBC
