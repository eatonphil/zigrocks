const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const exe = b.addExecutable("main", "main.zig");
    exe.linkLibC();
    exe.addIncludeDir("rocksdb/include");
    exe.addLibPath("rocksdb");
    exe.linkSystemLibraryName("rocksdb");
    exe.install();
}
