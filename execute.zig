const std = @import("std");

const parse = @import("parse.zig");
const Result = @import("result.zig").Result;
const RocksDB = @import("rocksdb.zig").RocksDB;
const Storage = @import("storage.zig").Storage;

pub const Executor = struct {
    allocator: std.mem.Allocator,
    storage: Storage,

    pub fn init(allocator: std.mem.Allocator, storage: Storage) Executor {
        return Executor{ .allocator = allocator, .storage = storage };
    }

    const QueryResponse = struct {
        fields: [][]const u8,
        // Array of cells (which is an array of strings (which is an array of u8))
        rows: [][][]const u8,
        empty: bool,
    };
    const QueryResponseResult = Result(QueryResponse);

    fn executeExpression(self: Executor, e: parse.ExpressionAST, row: Storage.Row) []const u8 {
        return switch (e) {
            .literal => |lit| switch (lit.kind) {
                .numeric => lit.string(),
                .string => lit.string(),
                .identifier => {
                    // Storage.Row's results are internal buffer
                    // views. So make a copy.
                    var copy = std.ArrayList(u8).init(self.allocator);
                    _ = copy.writer().write(row.get(lit.string())) catch return "";
                    return copy.items;
                },
                else => unreachable,
            },
            else => "[UNSUPPORTED]",
        };
    }

    fn executeSelect(self: Executor, s: parse.SelectAST) QueryResponseResult {
        switch (self.storage.getTable(s.from.string())) {
            .err => |err| return .{ .err = err },
            else => _ = 1,
        }

        // Now validate and store requested fields
        var requestedFields = std.ArrayList([]const u8).init(self.allocator);
        for (s.columns.items) |requestedColumn| {
            var fieldName = switch (requestedColumn) {
                .literal => |lit| switch (lit.kind) {
                    .identifier => lit.string(),
                    // TODO: give reasonable names
                    else => "unknown",
                },
                // TODO: give reasonable names
                else => "unknown",
            };
            requestedFields.append(fieldName) catch return .{
                .err = "Could not allocate for requested field.",
            };
        }

        // Prepare response
        var rows = std.ArrayList([][]const u8).init(self.allocator);
        var response = QueryResponse{
            .fields = requestedFields.items,
            .rows = undefined,
            .empty = false,
        };

        var iter = switch (self.storage.getRowIter(s.from.string())) {
            .err => |err| return .{ .err = err },
            .val => |it| it,
        };
        defer iter.close();

        while (iter.next()) |row| {
            var add = false;
            if (s.where) |where| {
                if (!std.mem.eql(u8, self.executeExpression(where, row), "0")) {
                    add = true;
                }
            } else {
                add = true;
            }

            if (add) {
                var requested = std.ArrayList([]const u8).init(self.allocator);
                for (s.columns.items) |exp| {
                    var val = self.executeExpression(exp, row);
                    requested.append(val) catch return .{
                        .err = "Could not allocate for requested cell",
                    };
                }
                rows.append(requested.items) catch return .{
                    .err = "Could not allocate for row",
                };
            }
        }

        response.rows = rows.items;
        return .{ .val = response };
    }

    fn executeInsert(self: Executor, i: parse.InsertAST) QueryResponseResult {
        var cells = std.ArrayList([]const u8).init(self.allocator);
        var empty = std.ArrayList([]u8).init(self.allocator);
        for (i.values.items) |v| {
            var exp = self.executeExpression(v, Storage.Row.init(self.allocator, empty.items));
            cells.append(exp) catch return .{ .err = "Could not allocate for cell" };
        }

        if (self.storage.writeRow(i.table.string(), cells.items)) |err| {
            return .{ .err = err };
        }

        return .{
            .val = .{ .fields = undefined, .rows = undefined, .empty = true },
        };
    }

    fn executeCreateTable(self: Executor, c: parse.CreateTableAST) QueryResponseResult {
        var columns = std.ArrayList([]const u8).init(self.allocator);
        var types = std.ArrayList([]const u8).init(self.allocator);

        for (c.columns.items) |column| {
            columns.append(column.name.string()) catch return .{
                .err = "Could not allocate for column name",
            };
            types.append(column.kind.string()) catch return .{
                .err = "Could not allocate for column kind",
            };
        }

        var table = Storage.Table{
            .name = c.table.string(),
            .columns = columns.items,
            .types = types.items,
        };

        if (self.storage.writeTable(table)) |err| {
            return .{ .err = err };
        }
        return .{
            .val = .{ .fields = undefined, .rows = undefined, .empty = true },
        };
    }

    pub fn execute(self: Executor, ast: parse.AST) QueryResponseResult {
        return switch (ast) {
            .select => |select| switch (self.executeSelect(select)) {
                .val => |val| .{ .val = val },
                .err => |err| .{ .err = err },
            },
            .insert => |insert| switch (self.executeInsert(insert)) {
                .val => |val| .{ .val = val },
                .err => |err| .{ .err = err },
            },
            .create_table => |createTable| switch (self.executeCreateTable(createTable)) {
                .val => |val| .{ .val = val },
                .err => |err| .{ .err = err },
            },
        };
    }
};
