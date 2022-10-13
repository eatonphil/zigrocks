# zigrocks: a basic SQL database in Zig, with storage via RocksDB

Build:

```bash
$ git clone https://github.com/facebook/rocksdb
$ ( cd rocksdb && make shared_lib -j8 )
$ zig build
```

And run! (Currently, it only parses and reprints the parsed code.)

```bash
$ cat tests/select.sql
SELECT
  a,b
FROM
  main
WHERE x = 1

$ ./main tests/select.sql
SELECT
  a,
  b
FROM
  main
WHERE
  x = 1
```

References:
* [rocksdb/c.h](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/c.h)
* [Minimal RocksDB/C example](https://gist.github.com/nitingupta910/4640638be7e7ad39c41e)
* [Zig build explained](https://zig.news/xq/zig-build-explained-part-3-1ima)
* [gosql](https://github.com/eatonphil/gosql)
