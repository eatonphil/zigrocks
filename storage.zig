const std = @import("std");

const RocksDB = @import("rocksdb.zig").RocksDB;
const Result = @import("result.zig").Result;
const Error = @import("result.zig").Error;
const serde = @import("serde.zig");

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
        name: serde.String,
        columns: []serde.String,
        types: []serde.String,
    };

    pub fn getTable(self: Storage, name: serde.String) Result(Table) {
        var tableKey = std.ArrayList(u8).init(self.allocator);
        tableKey.writer().print("tbl{s}", .{name}) catch return .{
            .err = "Could not allocate for table prefix",
        };

        var columns = std.ArrayList(serde.String).init(self.allocator);
        var types = std.ArrayList(serde.String).init(self.allocator);
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
            var column = serde.deserializeString(columnInfo[columnOffset..]);
            columnOffset += column.offset;
            columns.append(column.string) catch return .{
                .err = "Could not allocate for column name.",
            };

            var kind = serde.deserializeString(columnInfo[columnOffset..]);
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
        key.writer().print("tbl{s}", .{table.name}) catch return "Could not allocate key for table";

        var value = std.ArrayList(u8).init(self.allocator);
        for (table.columns) |column, i| {
            serde.serializeString(&value, column) catch return "Could not allocate for column";
            serde.serializeString(&value, table.types[i]) catch return "Could not allocate for column type";
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

    pub fn writeRow(self: Storage, table: serde.String, cells: []serde.String) ?Error {
        // Table name prefix
        var key = std.ArrayList(u8).init(self.allocator);
        key.writer().print("row{s}", .{table}) catch return "Could not allocate row key";

        // Unique row id
        var id = generateId() catch return "Could not generate id";
        key.appendSlice(id) catch return "Could not allocate for id";

        var value = std.ArrayList(u8).init(self.allocator);
        for (cells) |cell| {
            serde.serializeString(&value, cell) catch return "Could not allocate for cell";
        }

        return self.db.set(key.items, value.items);
    }

    pub const Row = struct {
        cells: std.ArrayList(serde.String),
        fields: []serde.String,

        pub fn init(allocator: std.mem.Allocator, fields: []serde.String) Row {
            return Row{
                .cells = std.ArrayList(serde.String).init(allocator),
                .fields = fields,
            };
        }

        pub fn append(self: *Row, cell: serde.String) !void {
            try self.cells.append(cell);
        }

        pub fn get(self: Row, field: serde.String) serde.String {
            for (self.fields) |f, i| {
                if (std.mem.eql(u8, field, f)) {
                    return self.cells.items[i];
                }
            }

            return "";
        }

        pub fn items(self: Row) []serde.String {
            return self.cells.items;
        }

        fn reset(self: *Row) void {
            self.cells.clearRetainingCapacity();
        }
    };

    pub const RowIter = struct {
        row: Row,
        iter: RocksDB.Iter,

        fn init(allocator: std.mem.Allocator, iter: RocksDB.Iter, fields: []serde.String) RowIter {
            return RowIter{
                .iter = iter,
                .row = Row.init(allocator, fields),
            };
        }

        pub fn next(self: *RowIter) ?Row {
            var rowBytes: serde.String = undefined;
            if (self.iter.next()) |b| {
                rowBytes = b.value;
            } else {
                return null;
            }

            self.row.reset();
            var offset: usize = 0;
            while (offset < rowBytes.len) {
                var d = serde.deserializeString(rowBytes[offset..]);
                offset += d.offset;
                self.row.append(d.string) catch return null;
            }

            return self.row;
        }

        pub fn close(self: RowIter) void {
            self.iter.close();
        }
    };

    pub fn getRowIter(self: Storage, table: serde.String) Result(RowIter) {
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
