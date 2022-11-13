const std = @import("std");

const parse = @import("parse.zig");
const Result = @import("result.zig").Result;
const RocksDB = @import("rocksdb.zig").RocksDB;
const Storage = @import("storage.zig").Storage;
const serializeString = @import("storage.zig").serializeString;
const deserializeString = @import("storage.zig").deserializeString;

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

    const Value = union(enum) {
        bool_value: bool,
        null_value: bool,
        string_value: []const u8,
        integer_value: i64,

        const TRUE = Value{ .bool_value = true };
        const FALSE = Value{ .bool_value = false };
        const NULL = Value{ .null_value = true };

        fn fromIntegerString(iBytes: []const u8) Value {
            const i = std.fmt.parseInt(i64, iBytes, 10) catch return Value{
                .integer_value = 0,
            };
            return Value{ .integer_value = i };
        }

        fn asBool(self: Value) bool {
            return switch (self) {
                .null_value => false,
                .bool_value => |value| value,
                .string_value => |value| value.len > 0,
                .integer_value => |value| value != 0,
            };
        }

        fn asString(self: Value) []const u8 {
            return switch (self) {
                .null_value => "",
                .bool_value => |value| if (value) "true" else "false",
                .string_value => |value| value,
                .integer_value => |value| {
                    var iBytes: [20]u8 = undefined;
                    _ = std.fmt.bufPrint(&iBytes, "{d}", .{value}) catch return "";
                    // Truncate non-numeric bytes
                    var end: usize = 20;
                    while (end > 0) {
                        if (iBytes[end - 1] == '-' or
                            (iBytes[end - 1] >= '0' and iBytes[end - 1] <= '9'))
                        {
                            break;
                        }

                        end -= 1;
                    }
                    return iBytes[0..end];
                },
            };
        }

        fn asInteger(self: Value) i64 {
            return switch (self) {
                .null_value => 0,
                .bool_value => |value| if (value) 1 else 0,
                .string_value => |value| fromIntegerString(value).integer_value,
                .integer_value => |value| value,
            };
        }

        fn serialize(self: Value, buf: *std.ArrayList(u8)) []const u8 {
            switch (self) {
                .null_value => buf.append('0') catch return "",

                .bool_value => |value| {
                    buf.append('1') catch return "";
                    buf.append(if (value) '1' else '0') catch return "";
                },

                .string_value => |value| {
                    buf.append('2') catch return "";
                    serializeString(buf.writer(), value);
                },

                .integer_value => {
                    buf.append('3') catch return "";
                    var s = self.asString();
                    _ = buf.appendSlice(s) catch return "";
                },
            }

            return buf.items;
        }

        fn deserialize(data: []const u8) Value {
            return switch (data[0]) {
                '0' => Value.NULL,
                '1' => Value{ .bool_value = data[1] == '1' },
                '2' => Value{ .string_value = deserializeString(data[1..]).string },
                '3' => Value{ .integer_value = std.mem.readIntBig(i64, data[0..8]) },
                else => unreachable,
            };
        }
    };

    fn executeExpression(self: Executor, e: parse.ExpressionAST, row: Storage.Row) Value {
        return switch (e) {
            .literal => |lit| switch (lit.kind) {
                .string => Value{ .string_value = lit.string() },
                .numeric => Value.fromIntegerString(lit.string()),
                .identifier => {
                    // Storage.Row's results are internal buffer
                    // views. So make a copy.
                    var copy = std.ArrayList(u8).init(self.allocator);
                    copy.appendSlice(row.get(lit.string())) catch return Value.NULL;
                    return Value.deserialize(copy.items);
                },
                else => unreachable,
            },
            .binary_operation => |bin_op| {
                var left = self.executeExpression(bin_op.left.*, row);
                var right = self.executeExpression(bin_op.right.*, row);

                if (bin_op.operator.kind == .equal_operator) {
                    // Cast dissimilar types to strings
                    if (@enumToInt(left) != @enumToInt(right)) {
                        left = Value{ .string_value = left.asString() };
                        right = Value{ .string_value = right.asString() };
                    }

                    return Value{
                        .bool_value = switch (left) {
                            .null_value => true,
                            .bool_value => |v| v == right.asBool(),
                            .string_value => std.mem.eql(u8, left.asString(), right.asString()),
                            .integer_value => left.asInteger() == right.asInteger(),
                        },
                    };
                }

                if (bin_op.operator.kind == .concat_operator) {
                    var copy = std.ArrayList(u8).init(self.allocator);
                    copy.appendSlice(left.asString()) catch return Value.NULL;
                    copy.appendSlice(right.asString()) catch return Value.NULL;
                    return Value{ .string_value = copy.items };
                }

                return switch (bin_op.operator.kind) {
                    .lt_operator => if (left.asInteger() < right.asInteger()) Value.TRUE else Value.FALSE,
                    .plus_operator => Value{ .integer_value = left.asInteger() + right.asInteger() },
                    else => Value.NULL,
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

        var cellBuffer = std.ArrayList(u8).init(self.allocator);
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
                for (s.columns.items) |exp| {
                    var val = self.executeExpression(exp, row);
                    cellBuffer.clearRetainingCapacity();
                    requested.append(val.serialize(&cellBuffer)) catch return .{
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
        var cellBuffer = std.ArrayList(u8).init(self.allocator);
        var cells = std.ArrayList([]const u8).init(self.allocator);
        var empty = std.ArrayList([]u8).init(self.allocator);
        for (i.values.items) |v| {
            var exp = self.executeExpression(v, Storage.Row.init(self.allocator, empty.items));
            cellBuffer.clearRetainingCapacity();
            cells.append(exp.serialize(&cellBuffer)) catch return .{ .err = "Could not allocate for cell" };
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

test "serialize/deserialize strings" {
    const expect = std.testing.expect;
    const Value = Executor.Value;

    var stringTests = [_]struct {
        value: Value,
        string: []const u8,
    }{
        .{ .value = Value.fromIntegerString("1"), .string = "1" },
        .{ .value = Value{ .integer_value = 1 }, .string = "1" },
        .{ .value = Value{ .integer_value = 1003 }, .string = "1003" },
    };

    for (stringTests) |testCase| {
        try expect(std.mem.eql(u8, testCase.value.asString(), testCase.string) == true);
    }
}

test "serialize/deserialize integers" {
    const expect = std.testing.expect;
    const Value = Executor.Value;

    var integerTests = [_]struct {
        value: Value,
        integer: i64,
    }{
        .{ .value = Value.fromIntegerString("1"), .integer = 1 },
        .{ .value = Value{ .integer_value = 1 }, .integer = 1 },
        .{ .value = Value.FALSE, .integer = 0 },
        .{ .value = Value.TRUE, .integer = 1 },
        .{ .value = Value{ .string_value = "1" }, .integer = 1 },
        .{ .value = Value{ .string_value = "1002" }, .integer = 1002 },
    };

    for (integerTests) |testCase| {
        try expect(testCase.value.asInteger() == testCase.integer);
    }
}
