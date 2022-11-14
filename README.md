# zigrocks: a basic SQL database in Zig, with storage via RocksDB

See [Writing a SQL database, take two: Zig and
RocksDB](https://notes.eatonphil.com/zigrocks-sql.html) for a walkthrough!

Build:

```bash
$ git clone https://github.com/facebook/rocksdb
$ ( cd rocksdb && make shared_lib -j8 )
$ zig build
```

And run!

```bash
$ ./main --database data --script <(echo "CREATE TABLE y (year int, age int, name text)")
echo "CREATE TABLE y (year int, age int, name text)"
ok
$ ./main --database data --script <(echo "INSERT INTO y VALUES (2010, 38, 'Gary')")
echo "INSERT INTO y VALUES (2010, 38, 'Gary')"
ok
$ ./main --database data --script <(echo "INSERT INTO y VALUES (2021, 92, 'Teej')")
echo "INSERT INTO y VALUES (2021, 92, 'Teej')"
ok
$ ./main --database data --script <(echo "INSERT INTO y VALUES (1994, 18, 'Mel')")
echo "INSERT INTO y VALUES (1994, 18, 'Mel')"
ok

# Basic query
$ ./main --database data --script <(echo "SELECT name, age, year FROM y")
echo "SELECT name, age, year FROM y"
| name          |age            |year           |
+ ====          +===            +====           +
| Mel           |18             |1994           |
| Gary          |38             |2010           |
| Teej          |92             |2021           |

# With WHERE
$ ./main --database data --script <(echo "SELECT name, year, age FROM y WHERE age < 40")
echo "SELECT name, year, age FROM y WHERE age < 40"
| name          |year           |age            |
+ ====          +====           +===            +
| Mel           |1994           |18             |
| Gary          |2010           |38             |

# With operations
$ ./main --database data --script <(echo "SELECT 'Name: ' || name, year + 30, age FROM y WHERE age < 40")
echo "SELECT 'Name: ' || name, year + 30, age FROM y WHERE age < 40"
| unknown               |unknown                |age            |
+ =======               +=======                +===            +
| Name: Mel             |2024           |18             |
| Name: Gary            |2040           |38             |
```

References:
* [RocksDB C header](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/c.h)
* [RocksDB C wrapper implementation](https://github.com/facebook/rocksdb/blob/main/db/c.cc)
* [RocksDB C tests](https://github.com/facebook/rocksdb/blob/main/db/c_test.c)
* [Minimal RocksDB/C example](https://gist.github.com/nitingupta910/4640638be7e7ad39c41e)
* [Zig build explained](https://zig.news/xq/zig-build-explained-part-3-1ima)
* [Zig Programming Language Discord's #zig-help channel](https://discord.gg/gxsFFjE)
* [gosql](https://github.com/eatonphil/gosql)
