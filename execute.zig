const std = @import("std");

const parse = @import("parse.zig");
const RocksDB = @import("rocksdb.zig").RocksDB;

const Error = []const u8;

pub const Executor = struct {
    allocator: std.mem.Allocator,
    db: RocksDB,

    pub fn init(allocator: std.mem.Allocator, db: RocksDB) Executor {
        return Executor{ .allocator = allocator, .db = db };
    }

    const QueryResponse = struct {
        fields: std.ArrayList(std.ArrayList(u8)),
        rows: std.ArrayList(std.ArrayList(std.ArrayList(u8))),
        empty: bool,
    };

    fn serializeString(write: fn ([]u8) void, string: []u8) void {
        var length = @intCast(u64, string.len);
        write(*@ptrCast(*[8]u8, &length));
        write(string);
    }

    fn deserializeString(string: []u8) struct {
        string: u8,
        offset: usize,
    } {
        var length = *@ptrCast(*u64, &string[0..8]);
        return .{
            .offset = length + 8,
            .string = string[8..length],
        };
    }

    // Row data is an internal RocksDB buffer so we need to find the
    // correct cell and then make a copy of it.
    fn cellFromRawRow(self: Executor, row: u8, index: usize) []u8 {
        var cell = std.ArrayList(u8).init(self.allocator);
        var offset = 0;
        var nth = 0;
        while (offset < row.len) {
            var d = deserializeString(row[offset..]);
            offset += d.offset;

            if (nth == index) {
                cell.insertSlice(d.string);
                break;
            }
        }

        return cell.items;
    }

    fn executeExpression(_: Executor, _: parse.ExpressionAST) []u8 {
        return "";
    }

    fn executeSelect(self: Executor, s: parse.SelectAST) union(enum) {
        val: QueryResponse,
        err: Error,
    } {
        var tableKey = std.ArrayList(u8).init(self.allocator);
        var tableKeyWriter = tableKey.writer();
        _ = tableKeyWriter.write("tbl") catch return .{ .err = "Could not allocate for table prefix" };
        _ = tableKeyWriter.write(s.from.string()) catch return .{ .err = "Could not allocate for table name" };

        // First grab table info
        var tableInfo = switch (self.db.get(tableKey.items[0..tableKey.items.len :0])) {
            .err => |err| return .{ .err = err },
            .val => |val| val,
            .not_found => return .{ .err = "No such table exists" },
        };
        var tableColumns = std.ArrayList(std.ArrayList(u8)).init(self.allocator);
        var columnOffset: usize = 0;
        while (columnOffset < tableInfo.len) {
            var column = deserializeString(tableInfo[columnOffset..]);
            columnOffset += column.offset;
            tableColumns.append(column.string) catch return .{
                .err = "Could not allocate for column.",
            };

            // And skip past the column kind
            var kind = deserializeString(tableInfo[columnOffset..]);
            columnOffset += kind.offset;
        }

        // Now validate and store requested fields
        var realFields = std.ArrayList(std.ArrayList(u8)).init(self.allocator);
        var realFieldIndexes = std.ArrayList(usize).init(self.allocator);
        for (s.columns) |requestedColumn, i| {
            var found = false;
            for (tableColumns) |column| {
                if (std.mem.eql(u8, column, requestedColumn)) {
                    found = true;
                }
            }

            if (!found) {
                return .{ .err = "No such column exists: " ++ requestedColumn };
            }

            realFields.append(requestedColumn) catch return .{
                .err = "Could not allocate for real field.",
            };
            realFieldIndexes.append(i) catch return .{
                .err = "Could not allocate for real field index.",
            };
        }

        // Prepare response
        var response = QueryResponse{
            .fields = realFields,
            .values = std.ArrayList(std.ArrayList(u8)).init(self.allocator),
        };

        var rowPrefix = std.ArrayList([]u8).init(self.allocator);
        var rowPrefixWriter = rowPrefix.writer();
        _ = rowPrefixWriter.write("tbl") catch return .{ .err = "Could not allocate for table prefix" };
        _ = rowPrefixWriter.write(s.table.string()) catch return .{ .err = "Could not allocate for table name" };

        var rowIter = self.db.iter(rowPrefix);
        defer rowIter.close();
        for (rowIter.next()) |row| {
            // Filter out if necessary
            if (s.where) |where| {
                var filtered = self.evaluateExpression(where, row);
                if (filtered) {
                    continue;
                }
            }

            // Copy requested fields into response
            var cells = std.ArrayList([]u8).init(self.allocator);
            for (realFieldIndexes.items) |index| {
                var cell = self.cellFromRawRow(row, index);
                cells.append(cell) catch return .{
                    .err = "Could not allocate for cell.",
                };
            }

            response.values.append(cells) catch return .{
                .err = "Could not allocate for row.",
            };
        }

        return .{ .val = response };
    }

    fn generateId() ![16]u8 {
        const file = try std.fs.cwd().openFileZ("/dev/random", .{});
        defer file.close();

        var buf: [16]u8 = .{};
        _ = try file.read(&buf);
        return buf;
    }

    fn executeInsert(self: Executor, i: parse.InsertAST) union(enum) { val: QueryResponse, err: Error } {
        // Table name prefix
        var key = std.ArrayList(u8).init(self.allocator);
        var keyWriter = key.writer();
        _ = keyWriter.write("row") catch return .{ .err = "Could not write row prefix" };
        _ = keyWriter.write(i.table.string()) catch return .{ .err = "Could not write row's table name" };

        // Unique row id
        var id = generateId() catch return .{ .err = "Could not generate id" };
        _ = keyWriter.write(&id) catch return .{ .err = "Could not write id" };

        // Row values
        var value = std.ArrayList(u8).init(self.allocator);
        var valueWriter = value.writer();
        for (i.values.items) |v| {
            var exp = self.executeExpression(v);
            serializeString(valueWriter.appendWrite, exp);
        }

        if (self.db.set(key, value)) |err| {
            return .{ .err = err };
        }

        return .{ .val = .{ .empty = true } };
    }

    fn executeCreateTable(self: Executor, c: parse.CreateTableAST) union(enum) {
        val: QueryResponse,
        err: Error,
    } {
        // Table name prefix
        var key = std.ArrayList(u8).init(self.allocator);
        var keyWriter = key.writer();
        _ = keyWriter.write("row") catch return .{ .err = "Could not write row prefix" };
        _ = keyWriter.write(c.table.string()) catch return .{ .err = "Could not write row's table name" };

        var value = std.ArrayList(u8).init(self.allocator);
        var valueWriter = value.writer();
        for (c.columns.items) |column| {
            serializeString(valueWriter.write, column.name.string());
            serializeString(valueWriter.write, column.kind.string());
        }

        if (self.db.set(key, value)) |err| {
            return .{ .err = err };
        }
        return .{ .val = .{ .empty = true } };
    }

    pub fn execute(self: Executor, ast: parse.AST) union(enum) { val: QueryResponse, err: Error } {
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
