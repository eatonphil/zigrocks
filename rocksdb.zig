const std = @import("std");

const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub const RocksDB = struct {
    db: *rdb.rocksdb_t,

    pub fn open(dir: []const u8) union(enum) { val: RocksDB, err: []u8 } {
        var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        var err: ?[*:0]u8 = null;
        var db = rdb.rocksdb_open(options, dir.ptr, &err);
        if (err) |errStr| {
            return .{ .err = std.mem.span(errStr) };
        }
        return .{ .val = RocksDB{ .db = db.? } };
    }

    pub fn close(self: RocksDB) void {
        rdb.rocksdb_close(self.db);
    }

    pub fn set(self: RocksDB, key: [:0]const u8, value: [:0]const u8) ?[]u8 {
        var writeOptions = rdb.rocksdb_writeoptions_create();
        var err: ?[*:0]u8 = null;
        rdb.rocksdb_put(
            self.db,
            writeOptions,
            key.ptr,
            key.len,
            value.ptr,
            value.len,
            &err,
        );
        if (err) |errStr| {
            return std.mem.span(errStr);
        }

        return null;
    }

    pub fn get(self: RocksDB, key: [:0]const u8) union(enum) { val: []u8, err: []u8, not_found: bool } {
        var readOptions = rdb.rocksdb_readoptions_create();
        var valueLength: usize = 0;
        var err: ?[*:0]u8 = null;
        var v = rdb.rocksdb_get(
            self.db,
            readOptions,
            key.ptr,
            key.len,
            &valueLength,
            &err,
        );
        if (err) |errStr| {
            return .{ .err = std.mem.span(errStr) };
        }
        if (v == 0) {
            return .{ .not_found = true };
        }

        return .{ .val = v[0..valueLength] };
    }

    pub const IterEntry = struct {
        key: []const u8,
        value: []const u8,
    };

    pub const Iter = struct {
        iter: *rdb.rocksdb_iterator_t,
        first: bool,
        prefix: []const u8,

        pub fn next(self: *Iter) ?IterEntry {
            if (!self.first) {
                rdb.rocksdb_iter_next(self.iter);
            }

            self.first = false;
            if (rdb.rocksdb_iter_valid(self.iter) != 1) {
                return null;
            }

            var keySize: usize = 0;
            var key = rdb.rocksdb_iter_key(self.iter, &keySize);

            // Make sure key is still within the prefix
            if (self.prefix.len > 0) {
                if (self.prefix.len > keySize or
                    !std.mem.eql(u8, key[0..self.prefix.len], self.prefix))
                {
                    return null;
                }
            }

            var valueSize: usize = 0;
            var value = rdb.rocksdb_iter_value(self.iter, &valueSize);

            return IterEntry{
                .key = key[0..keySize],
                .value = value[0..valueSize],
            };
        }

        pub fn close(self: Iter) void {
            rdb.rocksdb_iter_destroy(self.iter);
        }
    };

    pub fn iter(self: RocksDB, prefix: [:0]const u8) union(enum) { val: Iter, err: []const u8 } {
        var readOptions = rdb.rocksdb_readoptions_create();
        var it = Iter{
            .iter = undefined,
            .first = true,
            .prefix = prefix,
        };
        if (rdb.rocksdb_create_iterator(self.db, readOptions)) |i| {
            it.iter = i;
        } else {
            return .{ .err = "Could not create iterator" };
        }

        if (prefix.len > 0) {
            rdb.rocksdb_iter_seek(
                it.iter,
                prefix.ptr,
                prefix.len,
            );
        } else {
            rdb.rocksdb_iter_seek_to_first(it.iter);
        }
        return .{ .val = it };
    }
};

pub fn main() !void {
    var db: RocksDB = undefined;
    switch (RocksDB.open("/tmp/db")) {
        .val => |_db| {
            db = _db;
        },
        .err => |err| {
            std.debug.print("Failed to open: {s}.\n", .{err});
            return;
        },
    }
    defer db.close();

    var args = std.process.args();
    _ = args.next();
    var key: [:0]const u8 = "";
    var value: [:0]const u8 = "";
    var command = "get";
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "set")) {
            command = "set";
            key = args.next().?;
            value = args.next().?;
        } else if (std.mem.eql(u8, arg, "get")) {
            command = "get";
            key = args.next().?;
        } else if (std.mem.eql(u8, arg, "list")) {
            command = "lst";
            if (args.next()) |argNext| {
                key = argNext;
            }
        } else {
            std.debug.print("Must specify command (get, set, or list). Got: '{s}'.\n", .{arg});
            return;
        }
    }

    if (std.mem.eql(u8, command, "set")) {
        var setErr = db.set(key, value);
        if (setErr) |err| {
            std.debug.print("Error setting key: {s}.\n", .{err});
            return;
        }
    } else if (std.mem.eql(u8, command, "get")) {
        switch (db.get(key)) {
            .err => |err| {
                std.debug.print("Error getting key: {s}.\n", .{err});
                return;
            },
            .val => |val| std.debug.print("{s}\n", .{val}),
            .not_found => std.debug.print("Key not found.\n", .{}),
        }
    } else {
        var prefix = key;
        switch (db.iter(prefix)) {
            .err => |err| std.debug.print("Error getting iterator: {s}.\n", .{err}),
            .val => |iter| {
                // Create a local variable so that it.next() can
                // mutate it as a reference.
                var it = iter;
                defer it.close();
                while (it.next()) |entry| {
                    std.debug.print("{s} = {s}\n", .{ entry.key, entry.value });
                }
            },
        }
    }
}
