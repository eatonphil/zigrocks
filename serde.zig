const std = @import("std");

pub const String = []const u8;

pub fn serializeInteger(comptime T: type, buf: *std.ArrayList(u8), i: T) !void {
    var length: [@sizeOf(T)]u8 = undefined;
    std.mem.writeIntBig(T, &length, i);
    try buf.appendSlice(length[0..8]);
}

pub fn deserializeInteger(comptime T: type, buf: []const u8) T {
    return std.mem.readIntBig(T, buf[0..@sizeOf(T)]);
}

pub fn serializeString(buf: *std.ArrayList(u8), string: String) !void {
    try serializeInteger(u64, buf, string.len);
    try buf.appendSlice(string);
}

pub fn deserializeString(string: String) struct {
    offset: usize,
    string: []const u8,
} {
    var length = deserializeInteger(u64, string);
    var offset = length + 8;
    return .{ .offset = offset, .string = string[8..offset] };
}
