// const std = @import("std");
//
// const RocksDB = @import("./rocksdb.zig");
// const parse = @import("parse.zig");
//
// const Error = []const u8;
//
// pub const Executor = struct {
//     allocator: std.mem.Allocator,
//
//     const QueryResponse = struct {
//         fields: std.ArrayList([]u8),
//         rows: std.ArrayList([]u8),
//     };
//
//     pub fn init(allocator: std.mem.Allocator) Executor {
//         return Executor{ .allocator = allocator };
//     }
//
//     const TableMetadata = struct {};
//
//     fn serializeString(writer: std.io.Writer, string: []u8) ?Error {
//         var length: [8]u8 = @as(u64, string.len);
//         writer.write(length) catch return "Could not write string length";
//         writer.write(string) catch return "Could not write string";
//         return null;
//     }
//
//     fn deserializeString(serialized: []u8) []u8 {
//         var lengthBytes = serialized[0..8];
//         var length = @as(u64, lengthBytes);
//         return serialized[8..length];
//     }
//
//     fn executeSelect(self: Executor, db: RocksDB, s: parse.SelectAST) struct {
//         val: ?QueryResponse,
//         err: ?Error,
//     } {
//         // First grab table info
//         var tableInfo = db.get("tbl" ++ s.table.string());
//         var tableColumns = std.ArrayList([]u8).init(self.allocator);
//         while (tableInfo.length > 0) {
//             var column = deserializeString(tableInfo);
//             tableColumns.append(column) catch return .{
//                 .val = null,
//                 .err = "Could not allocate for column.",
//             };
//             tableInfo = tableInfo[8 + column.length ..];
//         }
//
//         var realFields = std.ArrayList([]u8).init(self.allocator);
//         for (s.columns) |requestedColumn| {
//             var found = false;
//             for (tableColumns) |column| {
//                 if (std.mem.eql(u8, column, requestedColumn)) {
//                     found = true;
//                 }
//             }
//
//             if (!found) {
//                 return .{ .val = null, .err = "No such column exists: " ++ requestedColumn };
//             }
//
//             realFields.append(column) catch return .{ .val = null, .err = "Could not allocate for real field." };
//         }
//
//         var response = QueryResponse{
//             .fields = realFields,
//             .values = std.ArrayList([]u8).init(self.allocator),
//         };
//     }
//
//     fn executeInsert(self: Executor, db: RocksDB, c: parse.InsertAST) struct { val: ?QueryResponse, err: ?Error } {
//         var key = std.ArrayList(u8).init(self.allocator);
//         var keyWriter = key.writer();
//         _ = keyWriter.write("row" ++ c.table.string()) catch return .{ .val = null, .err = "Could not write row's table name" };
//         var uuid = self.generateUUID() catch return .{ .val = null, .err = "Could not generate UUID" };
//         _ = keyWriter.write(uuid) catch return .{ .val = null, .err = "Could not write UUID" };
//
//         var value = std.ArrayList(u8).init(self.allocator);
//         var valueWriter = value.writer();
//         for (c.values.items) |v| {
//             var exp = self.executeExpression(v);
//             var err = serializeString(valueWriter, exp);
//             if (err != null) {
//                 return .{ .val = null, .err = err };
//             }
//         }
//
//         // TODO: can we get rid of all ptrCasts?
//
//         var keySlice: [:0]const u8 = std.mem.span(@ptrCast(*[:0]const u8, &key.items[0..key.items.len]).*);
//         var valueSlice: [:0]const u8 = std.mem.span(@ptrCast(*[:0]const u8, &value.items[0..value.items.len]).*);
//         _ = db.set(keySlice, valueSlice);
//         return .{ .val = null, .err = null };
//     }
//
//     fn executeCreateTable(self: Executor, db: RocksDB, c: parse.CreateTableAST) struct { val: ?QueryResponse, err: ?Error } {
//         var key = std.ArrayList(u8).init(self.allocator);
//         var keyWriter = key.writer();
//         _ = keyWriter.write("tbl" ++ c.table.string()) catch return .{ .val = null, .err = "Could not write table name" };
//
//         var value = std.ArrayList(u8).init(self.allocator);
//         var valueWriter = value.writer();
//         for (c.columns.items) |column| {
//             serializeString(
//                 valueWriter,
//                 column.string(),
//             );
//         }
//
//         // TODO: can we get rid of all ptrCasts?
//         var keySlice: [:0]const u8 = std.mem.span(@ptrCast(*[:0]const u8, &key.items[0..key.items.len]).*);
//         var valueSlice: [:0]const u8 = std.mem.span(@ptrCast(*[:0]const u8, &value.items[0..value.items.len]).*);
//         _ = db.set(keySlice, valueSlice);
//         return .{ .val = null, .err = null };
//     }
//
//     fn execute(self: Executor, db: RocksDB, ast: parse.AST) struct { val: ?QueryResponse, err: ?Error } {
//         if (ast.kind == .select_keyword) {
//             var res = self.executeSelect(db, ast.select.*);
//             return .{ .val = res.val, .err = res.err };
//         } else if (ast.kind == .insert_keyword) {
//             var res = self.executeInsert(db, ast.insert.*);
//             return .{ .val = res.val, .err = res.err };
//         } else if (ast.kind == .create_table_keyword) {
//             var res = self.executeCreateTable(db, ast.create_table.*);
//             return .{ .val = res.val, .err = res.err };
//         }
//
//         return .{ .val = null, .err = "Cannot execute unknown statement" };
//     }
// };
