pub const String = []const u8;

pub const Error = String;

pub fn Result(comptime T: type) type {
    return union(enum) {
        val: T,
        err: Error,
    };
}
