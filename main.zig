const std = @import("std");

const RocksDB = @import("rocksdb.zig").RocksDB;
const lex = @import("lex.zig");
const parse = @import("parse.zig");
const execute = @import("execute.zig");
const Storage = @import("storage.zig").Storage;

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var debugTokens = false;
    var debugAST = false;
    var args = std.process.args();
    var scriptArg: usize = 0;
    var databaseArg: usize = 0;
    var i: usize = 1;
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--debug-tokens")) {
            debugTokens = true;
        }

        if (std.mem.eql(u8, arg, "--debug-ast")) {
            debugAST = true;
        }

        if (std.mem.eql(u8, arg, "--database")) {
            databaseArg = i + 1;
            _ = args.next();
        }

        if (std.mem.eql(u8, arg, "--script")) {
            scriptArg = i + 1;
            _ = args.next();
        }

        i += 1;
    }

    if (databaseArg == 0) {
        std.debug.print("--database is a required flag. Should be a directory for data.\n", .{});
        return;
    }

    if (scriptArg == 0) {
        std.debug.print("--script is a required flag. Should be a file containing SQL.\n", .{});
        return;
    }

    const file = try std.fs.cwd().openFileZ(std.os.argv[scriptArg], .{});
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
    var ast: parse.AST = undefined;
    switch (parser.parse(tokens)) {
        .err => |err| {
            std.debug.print("Failed to parse: {s}", .{err});
            return;
        },
        .val => |val| ast = val,
    }

    if (debugAST) {
        ast.print();
    }

    var db: RocksDB = undefined;
    var dataDirectory = std.mem.span(std.os.argv[databaseArg]);
    switch (RocksDB.open(allocator, dataDirectory)) {
        .err => |err| {
            std.debug.print("Failed to open database: {s}", .{err});
            return;
        },
        .val => |val| db = val,
    }
    defer db.close();

    const storage = Storage.init(allocator, db);

    const executor = execute.Executor.init(allocator, storage);
    switch (executor.execute(ast)) {
        .err => |err| {
            std.debug.print("Failed to execute: {s}", .{err});
            return;
        },
        .val => |val| {
            if (val.rows.len == 0) {
                std.debug.print("ok\n", .{});
                return;
            }

            std.debug.print("| ", .{});
            for (val.fields) |field| {
                std.debug.print("{s}\t\t |", .{field});
            }
            std.debug.print("\n", .{});

            for (val.rows) |row| {
                std.debug.print("| ", .{});
                for (row) |cell| {
                    std.debug.print("{s}\t\t |", .{cell});
                }
                std.debug.print("\n", .{});
            }
        },
    }
}
