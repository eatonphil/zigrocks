const std = @import("std");

const RocksDB = @import("rocksdb.zig").RocksDB;
const lex = @import("lex.zig");
const parse = @import("parse.zig");

pub fn main() !void {
    if (std.os.argv.len < 2) {
        std.debug.print("Expected file name to interpret", .{});
        return;
    }

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var debugTokens = false;
    var debugAST = false;
    var args = std.process.args();
    while (args.nextPosix()) |arg| {
        if (std.mem.eql(u8, arg, "--debug-tokens")) {
            debugTokens = true;
        }

        if (std.mem.eql(u8, arg, "--debug-ast")) {
            debugAST = true;
        }
    }

    const file = try std.fs.cwd().openFileZ(std.os.argv[1], .{});
    defer file.close();

    const file_size = try file.getEndPos();
    var prog = try allocator.alloc(u8, file_size);

    _ = try file.read(prog);

    var tokens = std.ArrayList(lex.Token).init(allocator);
    const lexErr = lex.lex(prog, &tokens);
    if (lexErr) |err| {
        std.debug.print("Failed to lex: {s}", .{err});
        return;
    }

    if (debugTokens) {
        for (tokens.items) |token| {
            std.debug.print("Token: {s}", .{token.string()});
        }
    }

    if (tokens.items.len == 0) {
        std.debug.print("Program is empty", .{});
        return;
    }

    const parser = parse.Parser.init(allocator);
    const parseRes = parser.parse(tokens);
    if (parseRes.err) |err| {
        std.debug.print("Failed to parse: {s}", .{err});
        return;
    }

    if (debugAST) {
        if (parseRes.val) |ast| {
            ast.print();
        }
    }

    // const openRes = RocksDB.open();
    // if (openRes.err) |err| {
    //     std.debug.print("Failed to open database: {s}", .{err});
    //     return;
    // }
    // defer openRes.val.?.close();

    // const executor = Executor.init(allocator);
    // const executeRes = executor.execute(openRes.val.?, parseRes.val.?);
    // if (executeRes.err) |err| {
    //     std.debug.print("Failed to execute: {s}", .{err});
    //     return;
    // }
}
