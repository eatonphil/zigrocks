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

    fn executeExpression(_: Executor, e: parse.ExpressionAST, _: Storage.Row) []const u8 {
        return switch (e) {
            .literal => |lit| lit.string(),
            else => "[UNSUPPORTED]",
        };
    }

    fn executeSelect(self: Executor, s: parse.SelectAST) QueryResponseResult {
        const table = switch (self.storage.getTable(s.from.string())) {
            .err => |err| return .{ .err = err },
            .val => |val| val,
        };

        // Now validate and store requested fields
        var requestedFields = std.ArrayList([]const u8).init(self.allocator);
        var requestedFieldIndexes = std.ArrayList(usize).init(self.allocator);
        for (s.columns.items) |requestedColumn, i| {
            var found = false;
            for (table.columns) |column| {
                if (std.mem.eql(u8, column, requestedColumn.string())) {
                    found = true;
                }
            }

            if (!found) {
                return .{ .err = "No such column exists." };
            }

            requestedFields.append(requestedColumn.string()) catch return .{
                .err = "Could not allocate for requested field.",
            };
            requestedFieldIndexes.append(i) catch return .{
                .err = "Could not allocate for requested field index.",
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
            if (s.where) |where| {
                if (!std.mem.eql(u8, self.executeExpression(where, row), "0")) {
                    var requested = Storage.Row.init(self.allocator, requestedFields.items);
                    var items = row.items();
                    for (requestedFieldIndexes.items) |i| {
                        requested.append(items[i]) catch return .{
                            .err = "Could not allocate for requested cell",
                        };
                    }
                    rows.append(requested.items()) catch return .{
                        .err = "Could not allocate for row",
                    };
                }
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
