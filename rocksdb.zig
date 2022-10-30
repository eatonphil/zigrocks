const std = @import("std");

const rdb = @cImport(@cInclude("rocksdb/c.h"));

const CError = [*:0]u8;
const RocksDBError = error{
    NotFound,
};

pub const RocksDB = struct {
    db: *rdb.rocksdb_t,

    pub fn open() struct { val: ?RocksDB, err: ?CError } {
        var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        var err: ?CError = null;
        var db = rdb.rocksdb_open(options, "/tmp/testdb", &err);
        if (err != null) {
            return .{ .val = null, .err = err };
        }
        return .{ .val = RocksDB{ .db = db.? }, .err = null };
    }

    pub fn close(self: RocksDB) void {
        rdb.rocksdb_close(self.db);
    }

    fn set(self: RocksDB, key: [:0]const u8, value: [:0]const u8) ?CError {
        var writeOptions = rdb.rocksdb_writeoptions_create();
        var err: ?CError = null;
        rdb.rocksdb_put(
            self.db,
            writeOptions,
            @ptrCast([*c]const u8, key),
            key.len,
            @ptrCast([*c]const u8, value),
            value.len,
            &err,
        );
        if (err) |errStr| {
            std.c.free(@ptrCast(*anyopaque, err));
            return errStr;
        }

        return null;
    }

    fn getByPrefix(self: RocksDB, key: [:0]const u8) struct { val: std.ArrayList([]u8), err: ?CError } {
        var readOptions = rdb.rocksdb_readoptions_create();
        var valueLength: usize = 0;
        var err: ?CError = null;
        var v = rdb.rocksdb_get(
            self.db,
            readOptions,
            @ptrCast([*c]const u8, key),
            key.len,
            &valueLength,
            &err,
        );
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
};

// Unused
fn kvMain() !void {
    var db = RocksDB.open();
    defer db.close();

    var args = std.process.args();
    var key: [:0]const u8 = "";
    var value: [:0]const u8 = "";
    var command = "get";
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
        db.set(key, value);
    } else {
        var v = db.get(key) catch {
            std.debug.print("Key not found.", .{});
            return;
        };
        std.debug.print("{s}", .{v});
    }
}
