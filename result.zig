pub const Error = []const u8;

pub fn Result(comptime T: type) type {
    return union(enum) {
        val: T,
        err: Error,
    };
}
