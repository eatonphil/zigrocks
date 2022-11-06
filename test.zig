const std = @import("std");

fn ownZeroString(zstr: [*:0]u8) []u8 {
    var spanned = std.mem.span(zstr);
    const result = std.heap.c_allocator.alloc(u8, spanned.len) catch unreachable;
    std.mem.copy(u8, result, spanned);
    std.heap.c_allocator.free(zstr);
    return result;
}

pub fn main() void {
    var err: ?[*:0]u8 = null;
    if (err) |errStr| {
        var x = ownZeroString(errStr);
        _ = x;
    }
}
