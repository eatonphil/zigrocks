const std = @import("std");

const RocksDB = @import("rocksdb.zig").RocksDB;
const Result = @import("result.zig").Result;
const Error = @import("result.zig").Error;

fn serializeString(writer: std.ArrayList(u8).Writer, string: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeIntBig(u64, &length, string.len);
    var n = writer.write(length[0..8]) catch unreachable;
    std.debug.assert(n == 8);
    n = writer.write(string) catch unreachable;
    std.debug.assert(n == string.len);
}

fn deserializeString(string: []const u8) struct {
    offset: usize,
    string: []const u8,
} {
    var length = std.mem.readIntBig(u64, string[0..8]);
    var offset = length + 8;
    return .{ .offset = offset, .string = string[8..offset] };
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

    pub const Table = struct {
        name: []const u8,
        columns: [][]const u8,
        types: [][]const u8,
    };

    pub fn getTable(self: Storage, name: []const u8) Result(Table) {
        var tableKey = std.ArrayList(u8).init(self.allocator);
        var tableKeyWriter = tableKey.writer();
        _ = tableKeyWriter.write("tbl") catch return .{
            .err = "Could not allocate for table prefix",
        };
        _ = tableKeyWriter.write(name) catch return .{
            .err = "Could not allocate for table name",
        };

        var columns = std.ArrayList([]const u8).init(self.allocator);
        var types = std.ArrayList([]const u8).init(self.allocator);
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
            var column = deserializeString(columnInfo[columnOffset..]);
            columnOffset += column.offset;
            columns.append(column.string) catch return .{
                .err = "Could not allocate for column name.",
            };

            var kind = deserializeString(columnInfo[columnOffset..]);
            columnOffset += kind.offset;
            types.append(kind.string) catch return .{
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
        var keyWriter = key.writer();
        _ = keyWriter.write("tbl") catch return "Could not write table prefix";
        _ = keyWriter.write(table.name) catch return "Could not write table name";

        var value = std.ArrayList(u8).init(self.allocator);
        var valueWriter = value.writer();
        for (table.columns) |column, i| {
            serializeString(valueWriter, column);
            serializeString(valueWriter, table.types[i]);
        }

        return self.db.set(key.items, value.items);
    }

    fn generateId() ![16]u8 {
        const file = try std.fs.cwd().openFileZ("/dev/random", .{});
        defer file.close();

        var buf: [16]u8 = .{};
        _ = try file.read(&buf);
        return buf;
    }

    pub fn writeRow(self: Storage, table: []const u8, cells: [][]const u8) ?Error {
        // Table name prefix
        var key = std.ArrayList(u8).init(self.allocator);
        var keyWriter = key.writer();
        _ = keyWriter.write("row") catch return "Could not write row prefix";
        _ = keyWriter.write(table) catch return "Could not write row's table name";

        // Unique row id
        var id = generateId() catch return "Could not generate id";
        _ = keyWriter.write(&id) catch return "Could not write id";

        var value = std.ArrayList(u8).init(self.allocator);
        var valueWriter = value.writer();
        for (cells) |cell| {
            serializeString(valueWriter, cell);
        }

        return self.db.set(key.items, value.items);
    }

    pub const Row = struct {
        cells: std.ArrayList([]const u8),
        fields: [][]const u8,

        pub fn init(allocator: std.mem.Allocator, fields: [][]const u8) Row {
            return Row{
                .cells = std.ArrayList([]const u8).init(allocator),
                .fields = fields,
            };
        }

        pub fn append(self: *Row, cell: []const u8) !void {
            try self.cells.append(cell);
        }

        pub fn get(self: Row, field: []const u8) []const u8 {
            for (self.fields) |f, i| {
                if (std.mem.eql(u8, field, f)) {
                    return self.cells.items[i];
                }
            }

            return "";
        }

        pub fn items(self: Row) [][]const u8 {
            return self.cells.items;
        }

        fn reset(self: *Row) void {
            self.cells.clearRetainingCapacity();
        }
    };

    pub const RowIter = struct {
        row: Row,
        iter: RocksDB.Iter,

        fn init(allocator: std.mem.Allocator, iter: RocksDB.Iter, fields: [][]const u8) RowIter {
            return RowIter{
                .iter = iter,
                .row = Row.init(allocator, fields),
            };
        }

        pub fn next(self: *RowIter) ?Row {
            var rowBytes: []const u8 = undefined;
            if (self.iter.next()) |b| {
                rowBytes = b.value;
            } else {
                return null;
            }

            self.row.reset();
            var offset: usize = 0;
            while (offset < rowBytes.len) {
                var d = deserializeString(rowBytes[offset..]);
                offset += d.offset;
                self.row.append(d.string) catch return null;
            }

            return self.row;
        }

        pub fn close(self: RowIter) void {
            self.iter.close();
        }
    };

    pub fn getRowIter(self: Storage, table: []const u8) Result(RowIter) {
        var rowPrefix = std.ArrayList(u8).init(self.allocator);
        var rowPrefixWriter = rowPrefix.writer();
        _ = rowPrefixWriter.write("row") catch return .{ .err = "Could not allocate for table prefix" };
        _ = rowPrefixWriter.write(table) catch return .{ .err = "Could not allocate for table name" };

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
