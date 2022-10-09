const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const exe = b.addExecutable("main", "main.zig");
    exe.linkLibC();
    exe.linkSystemLibraryName("rocksdb");
    exe.addLibraryPath("./rocksdb");
    exe.addIncludePath("./rocksdb/include");
    exe.setOutputDir(".");

    if (exe.target.isDarwin()) {
        b.installFile("./rocksdb/librocksdb.7.8.dylib", "../librocksdb.7.8.dylib");
        exe.addRPath(".");
    }
        
    exe.install();
}
