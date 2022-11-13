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
        // Array of cells (which is an array of serde (which is an array of u8))
        rows: [][][]const u8,
        empty: bool,
    };
    const QueryResponseResult = Result(QueryResponse);

    fn executeExpression(self: Executor, e: parse.ExpressionAST, row: Storage.Row) Storage.Value {
        return switch (e) {
            .literal => |lit| switch (lit.kind) {
                .string => Storage.Value{ .string_value = lit.string() },
                .numeric => Storage.Value.fromIntegerString(lit.string()),
                .identifier => {
                    // Storage.Row's results are internal buffer
                    // views. So make a copy.
                    var copy = std.ArrayList(u8).init(self.allocator);
                    copy.appendSlice(row.get(lit.string())) catch return Storage.Value.NULL;
                    return Storage.Value.deserialize(copy.items);
                },
                else => unreachable,
            },
            .binary_operation => |bin_op| {
                var left = self.executeExpression(bin_op.left.*, row);
                var right = self.executeExpression(bin_op.right.*, row);

                if (bin_op.operator.kind == .equal_operator) {
                    // Cast dissimilar types to serde
                    if (@enumToInt(left) != @enumToInt(right)) {
                        var leftBuf = std.ArrayList(u8).init(self.allocator);
                        left.asString(&leftBuf) catch unreachable;
                        left = Storage.Value{ .string_value = leftBuf.items };

                        var rightBuf = std.ArrayList(u8).init(self.allocator);
                        right.asString(&rightBuf) catch unreachable;
                        right = Storage.Value{ .string_value = rightBuf.items };
                    }

                    return Storage.Value{
                        .bool_value = switch (left) {
                            .null_value => true,
                            .bool_value => |v| v == right.asBool(),
                            .string_value => blk: {
                                var leftBuf = std.ArrayList(u8).init(self.allocator);
                                left.asString(&leftBuf) catch unreachable;

                                var rightBuf = std.ArrayList(u8).init(self.allocator);
                                right.asString(&rightBuf) catch unreachable;

                                break :blk std.mem.eql(u8, leftBuf.items, rightBuf.items);
                            },
                            .integer_value => left.asInteger() == right.asInteger(),
                        },
                    };
                }

                if (bin_op.operator.kind == .concat_operator) {
                    var copy = std.ArrayList(u8).init(self.allocator);
                    left.asString(&copy) catch unreachable;
                    right.asString(&copy) catch unreachable;
                    return Storage.Value{ .string_value = copy.items };
                }

                return switch (bin_op.operator.kind) {
                    .lt_operator => if (left.asInteger() < right.asInteger()) Storage.Value.TRUE else Storage.Value.FALSE,
                    .plus_operator => Storage.Value{ .integer_value = left.asInteger() + right.asInteger() },
                    else => Storage.Value.NULL,
                };
            },
        };
    }

    fn executeSelect(self: Executor, s: parse.SelectAST) QueryResponseResult {
        switch (self.storage.getTable(s.from.string())) {
            .err => |err| return .{ .err = err },
            else => _ = 1,
        }

        // Now validate and store requested fields
        var requestedFields = std.ArrayList([]const u8).init(self.allocator);
        for (s.columns) |requestedColumn| {
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
                if (self.executeExpression(where, row).asBool()) {
                    add = true;
                }
            } else {
                add = true;
            }

            if (add) {
                var requested = std.ArrayList([]const u8).init(self.allocator);
                for (s.columns) |exp| {
                    var val = self.executeExpression(exp, row);
                    var valBuf = std.ArrayList(u8).init(self.allocator);
                    val.asString(&valBuf) catch unreachable;
                    requested.append(valBuf.items) catch return .{
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
        var emptyRow = Storage.Row.init(self.allocator, undefined);
        var row = Storage.Row.init(self.allocator, undefined);
        for (i.values) |v| {
            var exp = self.executeExpression(v, emptyRow);
            row.append(exp) catch return .{ .err = "Could not allocate for cell" };
        }

        if (self.storage.writeRow(i.table.string(), row)) |err| {
            return .{ .err = err };
        }

        return .{
            .val = .{ .fields = undefined, .rows = undefined, .empty = true },
        };
    }

    fn executeCreateTable(self: Executor, c: parse.CreateTableAST) QueryResponseResult {
        var columns = std.ArrayList([]const u8).init(self.allocator);
        var types = std.ArrayList([]const u8).init(self.allocator);

        for (c.columns) |column| {
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
