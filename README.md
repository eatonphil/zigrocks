# zigrocks: a basic SQL database in Zig, with storage via RocksDB

Build:

```bash
$ git clone https://github.com/facebook/rocksdb
$ ( cd rocksdb && make shared_lib -j8 )
$ zig build
```

And run!

```bash
$ ./main --database data --script <(echo "CREATE TABLE y (year int, age int)")
ok
$ ./main --database data --script <(echo "INSERT INTO y VALUES (2010, 30)")
ok
$ ./main --database data --script <(echo "INSERT INTO y VALUES (2000, 20)")
ok
$ ./main --database data --script <(echo "INSERT INTO y VALUES (2000, 18)")
ok
$ ./main --database data --script <(echo "SELECT age, year FROM y")
| age           |year           |
+ ===           +====           +
| 18            |2000           |
| 30            |2010           |
| 20            |2000           |
```

References:
* [RocksDB C header](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/c.h)
* [RocksDB C wrapper implementation](https://github.com/facebook/rocksdb/blob/main/db/c.cc)
* [RocksDB C tests](https://github.com/facebook/rocksdb/blob/main/db/c_test.c)
* [Minimal RocksDB/C example](https://gist.github.com/nitingupta910/4640638be7e7ad39c41e)
* [Zig build explained](https://zig.news/xq/zig-build-explained-part-3-1ima)
* [Zig Programming Language Discord's #zig-help channel](https://discord.gg/gxsFFjE)
* [gosql](https://github.com/eatonphil/gosql)
