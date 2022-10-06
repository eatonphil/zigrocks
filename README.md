Build:

```bash
$ git clone https://github.com/facebook/rocksdb
$ cd rocksdb
$ make shared_lib -j8
$ cd ../
$ zig build
$ ./zig-out/bin/main
```
