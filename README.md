Build:

```bash
$ git clone https://github.com/facebook/rocksdb
$ cd rocksdb
$ make shared_lib -j8
$ cd ../
$ zig build
$ ./zig-out/bin/main
```

References:
* [rocksdb/c.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/c.h)
* [Minimal RocksDB/C example](https://gist.github.com/nitingupta910/4640638be7e7ad39c41e)
* [Zig build explained](https://zig.news/xq/zig-build-explained-part-3-1ima)

Issues:
* Tried a version running `make static_lib -j8` in rocksdb but couldn't get it linked correctly in zig build
