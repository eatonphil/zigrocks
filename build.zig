const version = @import("builtin").zig_version;
const std = @import("std");

pub fn build(b: *std.build.Builder) void {
    const exe = b.addExecutable("main", "main.zig");
    exe.linkLibC();
    exe.linkSystemLibraryName("rocksdb");

    if (@hasDecl(@TypeOf(exe.*), "addLibraryPath")) {
        exe.addLibraryPath("./rocksdb");
        exe.addIncludePath("./rocksdb/include");
    } else {
        exe.addLibPath("./rocksdb");
        exe.addIncludeDir("./rocksdb/include");
    }

    exe.setOutputDir(".");

    if (exe.target.isDarwin()) {
        b.installFile("./rocksdb/librocksdb.7.8.dylib", "../librocksdb.7.8.dylib");
        exe.addRPath(".");
    }

    exe.install();
}
