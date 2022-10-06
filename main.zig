const std = @import("std");
const cstd = @cImport(@cInclude("stdlib.h"));
const c = @cImport(@cInclude("rocksdb/c.h"));

pub fn main() !void {
    var options: ?*c.rocksdb_options_t = c.rocksdb_options_create();
    c.rocksdb_options_set_create_if_missing(options, 1);
    var err: []u8 = "";
    _ = c.rocksdb_open(options, "/tmp/testdb", @ptrCast([*c][*c]u8, &err));
    if (err.len != 0) {
        std.debug.print("err: {s}", .{err});
    }
}
