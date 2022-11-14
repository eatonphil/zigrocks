const std = @import("std");

const RocksDB = @import("rocksdb.zig").RocksDB;
const Error = @import("types.zig").Error;
const Result = @import("types.zig").Result;
const String = @import("types.zig").String;

pub fn serializeInteger(comptime T: type, buf: *std.ArrayList(u8), i: T) !void {
    var length: [@sizeOf(T)]u8 = undefined;
    std.mem.writeIntBig(T, &length, i);
    try buf.appendSlice(length[0..8]);
}

pub fn deserializeInteger(comptime T: type, buf: String) T {
    return std.mem.readIntBig(T, buf[0..@sizeOf(T)]);
}

pub fn serializeBytes(buf: *std.ArrayList(u8), bytes: String) !void {
    try serializeInteger(u64, buf, bytes.len);
    try buf.appendSlice(bytes);
}

pub fn deserializeBytes(bytes: String) struct {
    offset: usize,
    bytes: String,
} {
    var length = deserializeInteger(u64, bytes);
    var offset = length + 8;
    return .{ .offset = offset, .bytes = bytes[8..offset] };
}

pub const Storage = struct {
    db: RocksDB,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, db: RocksDB) Storage {
        return Storage{
            .db = db,
            .allocator = allocator,
        };
    }

    pub const Value = union(enum) {
        bool_value: bool,
        null_value: bool,
        string_value: String,
        integer_value: i64,

        pub const TRUE = Value{ .bool_value = true };
        pub const FALSE = Value{ .bool_value = false };
        pub const NULL = Value{ .null_value = true };

        pub fn fromIntegerString(iBytes: String) Value {
            const i = std.fmt.parseInt(i64, iBytes, 10) catch return Value{
                .integer_value = 0,
            };
            return Value{ .integer_value = i };
        }

        pub fn asBool(self: Value) bool {
            return switch (self) {
                .null_value => false,
                .bool_value => |value| value,
                .string_value => |value| value.len > 0,
                .integer_value => |value| value != 0,
            };
        }

        pub fn asString(self: Value, buf: *std.ArrayList(u8)) !void {
            try switch (self) {
                .null_value => _ = 1, // Do nothing
                .bool_value => |value| buf.appendSlice(if (value) "true" else "false"),
                .string_value => |value| buf.appendSlice(value),
                .integer_value => |value| buf.writer().print("{d}", .{value}),
            };
        }

        pub fn asInteger(self: Value) i64 {
            return switch (self) {
                .null_value => 0,
                .bool_value => |value| if (value) 1 else 0,
                .string_value => |value| fromIntegerString(value).integer_value,
                .integer_value => |value| value,
            };
        }

        pub fn serialize(self: Value, buf: *std.ArrayList(u8)) String {
            switch (self) {
                .null_value => buf.append('0') catch return "",

                .bool_value => |value| {
                    buf.append('1') catch return "";
                    buf.append(if (value) '1' else '0') catch return "";
                },

                .string_value => |value| {
                    buf.append('2') catch return "";
                    buf.appendSlice(value) catch return "";
                },

                .integer_value => |value| {
                    buf.append('3') catch return "";
                    serializeInteger(i64, buf, value) catch return "";
                },
            }

            return buf.items;
        }

        pub fn deserialize(data: String) Value {
            return switch (data[0]) {
                '0' => Value.NULL,
                '1' => Value{ .bool_value = data[1] == '1' },
                '2' => Value{ .string_value = data[1..] },
                '3' => Value{ .integer_value = deserializeInteger(i64, data[1..]) },
                else => unreachable,
            };
        }
    };

    pub const Table = struct {
        name: String,
        columns: []String,
        types: []String,
    };

    pub fn getTable(self: Storage, name: String) Result(Table) {
        var tableKey = std.ArrayList(u8).init(self.allocator);
        tableKey.writer().print("tbl{s}", .{name}) catch return .{
            .err = "Could not allocate for table prefix",
        };

        var columns = std.ArrayList(String).init(self.allocator);
        var types = std.ArrayList(String).init(self.allocator);
        var table = Table{
            .name = name,
            .columns = undefined,
            .types = undefined,
        };
        // First grab table info
        var columnInfo = switch (self.db.get(tableKey.items)) {
            .err => |err| return .{ .err = err },
            .val => |val| val,
            .not_found => return .{ .err = "No such table" },
        };

        var columnOffset: usize = 0;
        while (columnOffset < columnInfo.len) {
            var column = deserializeBytes(columnInfo[columnOffset..]);
            columnOffset += column.offset;
            columns.append(column.bytes) catch return .{
                .err = "Could not allocate for column name.",
            };

            var kind = deserializeBytes(columnInfo[columnOffset..]);
            columnOffset += kind.offset;
            types.append(kind.bytes) catch return .{
                .err = "Could not allocate for column kind.",
            };
        }

        table.columns = columns.items;
        table.types = types.items;

        return .{ .val = table };
    }

    pub fn writeTable(self: Storage, table: Table) ?Error {
        // Table name prefix
        var key = std.ArrayList(u8).init(self.allocator);
        key.writer().print("tbl{s}", .{table.name}) catch return "Could not allocate key for table";

        var value = std.ArrayList(u8).init(self.allocator);
        for (table.columns) |column, i| {
            serializeBytes(&value, column) catch return "Could not allocate for column";
            serializeBytes(&value, table.types[i]) catch return "Could not allocate for column type";
        }

        return self.db.set(key.items, value.items);
    }

    fn generateId() ![]u8 {
        const file = try std.fs.cwd().openFileZ("/dev/random", .{});
        defer file.close();

        var buf: [16]u8 = .{};
        _ = try file.read(&buf);
        return buf[0..];
    }

    pub const Row = struct {
        allocator: std.mem.Allocator,
        cells: std.ArrayList(String),
        fields: []String,

        pub fn init(allocator: std.mem.Allocator, fields: []String) Row {
            return Row{
                .allocator = allocator,
                .cells = std.ArrayList(String).init(allocator),
                .fields = fields,
            };
        }

        pub fn append(self: *Row, cell: Value) !void {
            var cellBuffer = std.ArrayList(u8).init(self.allocator);
            try self.cells.append(cell.serialize(&cellBuffer));
        }

        pub fn appendBytes(self: *Row, cell: String) !void {
            try self.cells.append(cell);
        }

        pub fn get(self: Row, field: String) String {
            for (self.fields) |f, i| {
                if (std.mem.eql(u8, field, f)) {
                    return self.cells.items[i];
                }
            }

            return "";
        }

        pub fn items(self: Row) []String {
            return self.cells.items;
        }

        fn reset(self: *Row) void {
            self.cells.clearRetainingCapacity();
        }
    };

    pub fn writeRow(self: Storage, table: String, row: Row) ?Error {
        // Table name prefix
        var key = std.ArrayList(u8).init(self.allocator);
        key.writer().print("row{s}", .{table}) catch return "Could not allocate row key";

        // Unique row id
        var id = generateId() catch return "Could not generate id";
        key.appendSlice(id) catch return "Could not allocate for id";

        var value = std.ArrayList(u8).init(self.allocator);
        for (row.cells.items) |cell| {
            serializeBytes(&value, cell) catch return "Could not allocate for cell";
        }

        return self.db.set(key.items, value.items);
    }

    pub const RowIter = struct {
        row: Row,
        iter: RocksDB.Iter,

        fn init(allocator: std.mem.Allocator, iter: RocksDB.Iter, fields: []String) RowIter {
            return RowIter{
                .iter = iter,
                .row = Row.init(allocator, fields),
            };
        }

        pub fn next(self: *RowIter) ?Row {
            var rowBytes: String = undefined;
            if (self.iter.next()) |b| {
                rowBytes = b.value;
            } else {
                return null;
            }

            self.row.reset();
            var offset: usize = 0;
            while (offset < rowBytes.len) {
                var d = deserializeBytes(rowBytes[offset..]);
                offset += d.offset;
                self.row.appendBytes(d.bytes) catch return null;
            }

            return self.row;
        }

        pub fn close(self: RowIter) void {
            self.iter.close();
        }
    };

    pub fn getRowIter(self: Storage, table: String) Result(RowIter) {
        var rowPrefix = std.ArrayList(u8).init(self.allocator);
        rowPrefix.writer().print("row{s}", .{table}) catch return .{
            .err = "Could not allocate for row prefix",
        };

        var iter = switch (self.db.iter(rowPrefix.items)) {
            .err => |err| return .{ .err = err },
            .val => |it| it,
        };

        var tableInfo = switch (self.getTable(table)) {
            .err => |err| return .{ .err = err },
            .val => |t| t,
        };

        return .{
            .val = RowIter.init(self.allocator, iter, tableInfo.columns),
        };
    }
};

test "serialize/deserialize Value strings" {
    const expectEqualStrings = std.testing.expectEqualStrings;
    const Value = Storage.Value;

    var stringTests = [_]struct {
        value: Value,
        string: String,
    }{
        .{ .value = Value.fromIntegerString("1"), .string = "1" },
        .{ .value = Value{ .integer_value = 1 }, .string = "1" },
        .{ .value = Value{ .integer_value = 1003 }, .string = "1003" },
        .{ .value = Value{ .bool_value = false }, .string = "false" },
        .{ .value = Value{ .bool_value = true }, .string = "true" },
    };

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    var buf2 = std.ArrayList(u8).init(std.testing.allocator);
    defer buf2.deinit();

    for (stringTests) |testCase| {
        buf.clearRetainingCapacity();
        var serialized = testCase.value.serialize(&buf);

        buf2.clearRetainingCapacity();
        try Value.deserialize(serialized).asString(&buf2);
        try expectEqualStrings(testCase.string, buf2.items);
    }
}

test "serialize/deserialize Value integers" {
    const expectEqual = std.testing.expectEqual;
    const Value = Storage.Value;

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

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    for (integerTests) |testCase| {
        buf.clearRetainingCapacity();
        var serialized = testCase.value.serialize(&buf);
        try expectEqual(testCase.integer, Value.deserialize(serialized).asInteger());
    }
}
