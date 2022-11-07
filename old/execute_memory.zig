const std = @import("std");

const parse = @import("parse.zig");

const Error = []const u8;

pub const Executor = struct {
    const ColumnInfo = struct {
        name: []u8,
        kind: []u8,
    };
    const RowInfo = struct {
        cells: std.ArrayList([]u8),
    };
    const TableInfo = struct {
        name: []u8,
        columns: std.ArrayList([]u8),
        rows: std.ArrayList(RowInfo),
    };

    allocator: std.mem.Allocator,
    tables: std.ArrayList(TableInfo),

    pub fn init(allocator: std.mem.Allocator) Executor {
        return Executor{ .allocator = allocator };
    }

    const QueryResponse = struct {
        fields: std.ArrayList([]u8),
        rows: std.ArrayList(std.ArrayList([]u8)),
    };

    fn executeSelect(self: Executor, s: parse.SelectAST) struct {
        val: ?QueryResponse,
        err: ?Error,
    } {
        var table: ?TableInfo = null;
        for (self.tables.items) |t| {
            if (!std.mem.eql(u8, t.name, s.table.string())) {
                continue;
            }

            table = t;
        }

        if (table == null) {
            return .{ .val = null, .err = "No such table exists: " ++ s.table.string() };
        }

        var realFields = std.ArrayList([]u8).init(self.allocator);
        var realFieldIndexes = std.ArrayList(usize).init(self.allocator);
        for (s.columns) |requestedColumn, i| {
            var found = false;
            for (tableColumns) |column| {
                if (std.mem.eql(u8, column, requestedColumn)) {
                    found = true;
                }
            }

            if (!found) {
                return .{ .val = null, .err = "No such column exists: " ++ requestedColumn };
            }

            realFields.append(column) catch return .{
                .val = null,
                .err = "Could not allocate for real field.",
            };
            realFieldIndexes.append(i) catch return .{
                .val = null,
                .err = "Could not allocate for real field index.",
            };
        }

        var response = QueryResponse{
            .fields = realFields,
            .values = std.ArrayList(RowInfo).init(self.allocator),
        };

        for (table.rows.items) |row| {
            if (s.where) |where| {
                var filtered = self.evaluateExpression(where, row);
                if (filtered) {
                    continue;
                }
            }

            var ri = RowInfo{ .cells = std.ArrayList([]u8).init(self.allocator) };
            for (realFieldIndexes.items) |index| {}
        }

        // First grab table info
        var tableInfo = db.get("tbl" ++ s.table.string());
        var tableColumns = std.ArrayList([]u8).init(self.allocator);
        while (tableInfo.length > 0) {
            var column = deserializeString(tableInfo);
            tableColumns.append(column) catch return .{
                .val = null,
                .err = "Could not allocate for column.",
            };
            tableInfo = tableInfo[8 + column.length ..];
        }

        return .{ .val = response, .err = null };
    }

    fn executeInsert(self: Executor, c: parse.InsertAST) struct { val: ?QueryResponse, err: ?Error } {
        var key = std.ArrayList(u8).init(self.allocator);
        var keyWriter = key.writer();
        _ = keyWriter.write("row" ++ c.table.string()) catch return .{ .val = null, .err = "Could not write row's table name" };
        var uuid = self.generateUUID() catch return .{ .val = null, .err = "Could not generate UUID" };
        _ = keyWriter.write(uuid) catch return .{ .val = null, .err = "Could not write UUID" };

        var value = std.ArrayList(u8).init(self.allocator);
        var valueWriter = value.writer();
        for (c.values.items) |v| {
            var exp = self.executeExpression(v);
            var err = serializeString(valueWriter, exp);
            if (err != null) {
                return .{ .val = null, .err = err };
            }
        }

        return .{ .val = null, .err = null };
    }

    fn executeCreateTable(self: Executor, c: parse.CreateTableAST) struct {
        val: ?QueryResponse,
        err: ?Error,
    } {
        var table = TableInfo{
            .name = c.table.string(),
            .columns = std.ArrayList(ColumnInfo).init(self.allocator),
            .rows = std.ArrayList(RowInfo).init(self.allocator),
        };

        for (c.columns.items) |column| {
            var info = ColumnInfo{
                .name = column.name.string(),
                .kind = column.kind.string(),
            };
            table.columns.append(info) catch return .{
                .val = null,
                .err = "Could not allocate for column info.",
            };
        }

        return .{ .val = QueryResponse{}, .err = null };
    }

    fn execute(self: Executor, ast: parse.AST) struct { val: ?QueryResponse, err: ?Error } {
        if (ast.kind == .select_keyword) {
            var res = self.executeSelect(ast.select.*);
            return .{ .val = res.val, .err = res.err };
        } else if (ast.kind == .insert_keyword) {
            var res = self.executeInsert(ast.insert.*);
            return .{ .val = res.val, .err = res.err };
        } else if (ast.kind == .create_table_keyword) {
            var res = self.executeCreateTable(ast.create_table.*);
            return .{ .val = res.val, .err = res.err };
        }

        return .{ .val = null, .err = "Cannot execute unknown statement" };
    }
};
