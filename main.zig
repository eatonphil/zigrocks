const std = @import("std");
const rdb = @cImport(@cInclude("rocksdb/c.h"));

pub fn set(db: *rdb.rocksdb_t, key: [:0]const u8, value: [:0]const u8) void {
    var writeOptions = rdb.rocksdb_writeoptions_create();
    var err: ?[*:0]u8 = null;
    rdb.rocksdb_put(db, writeOptions, @ptrCast([*c]const u8, key), key.len, @ptrCast([*c]const u8, value), value.len, &err);
    if (err) |errStr| {
        std.debug.print("Could not write key-value pair: {s}", .{errStr});
        std.c.free(@ptrCast(*anyopaque, err));
        return;
    }
}

const RocksDBError = error {
    NotFound,
};

pub fn get(db: *rdb.rocksdb_t, key: [:0]const u8) ![*c]const u8 {
    var readOptions = rdb.rocksdb_readoptions_create();
    var valueLength: usize = 0;
    var err: ?[*:0]u8 = null;
    var v = rdb.rocksdb_get(db, readOptions, @ptrCast([*c]const u8, key), key.len, &valueLength, &err);
    if (err) |errStr| {
        std.debug.print("Could not get value for key: {s}", .{errStr});
        std.c.free(@ptrCast(*anyopaque, err));
        return "";
    }
    if (v == 0) {
        return RocksDBError.NotFound;
    }

    return v;
}

pub fn main() !void {
    var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
    rdb.rocksdb_options_set_create_if_missing(options, 1);
    var err: ?[*:0]u8 = null;
    var db = rdb.rocksdb_open(options, "/tmp/testdb", &err);
    if (err) |errStr| {
        std.debug.print("err: {s}", .{errStr});
        std.c.free(@ptrCast(*anyopaque, err));
        return;
    }
    defer rdb.rocksdb_close(db);

    var args = std.process.args();
    var key: [:0]const u8 = "";
    var value: [:0]const u8 = "";
    var command = "get";
    _ = args.next(); // Skip first arg
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "set")) {
            key = args.next().?;
            value = args.next().?;
            command = "set";
        } else if (std.mem.eql(u8, arg, "get")) {
            key = args.next().?;
        } else {
            std.debug.print("Must specify command (get or set). Got: {s}", .{arg});
            return;
        }
    }

    if (std.mem.eql(u8, command, "set")) {
        set(db.?, key, value);
    } else {
        var v = get(db.?, key) catch {
            std.debug.print("Key not found.", .{});
            return;
        };
        std.debug.print("{s}", .{v});
    }
}
