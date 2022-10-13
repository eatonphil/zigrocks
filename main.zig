const std = @import("std");
const mem = std.mem;
const rdb = @cImport(@cInclude("rocksdb/c.h"));

const ArrayList = std.ArrayList;

const CError = [*:0]u8;
const Error = []const u8;

const RocksDBError = error{
    NotFound,
};

const RocksDB = struct {
    db: *rdb.rocksdb_t,

    fn open() struct { val: ?RocksDB, err: ?CError } {
        var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        var err: ?CError = null;
        var db = rdb.rocksdb_open(options, "/tmp/testdb", &err);
        if (err != null) {
            return .{ .val = null, .err = err };
        }
        return .{ .val = RocksDB{ .db = db.? }, .err = null };
    }

    fn close(self: RocksDB) void {
        rdb.rocksdb_close(self.db);
    }

    fn set(self: RocksDB, key: [:0]const u8, value: [:0]const u8) ?CError {
        var writeOptions = rdb.rocksdb_writeoptions_create();
        var err: ?CError = null;
        rdb.rocksdb_put(self.db, writeOptions, @ptrCast([*c]const u8, key), key.len, @ptrCast([*c]const u8, value), value.len, &err);
        if (err) |errStr| {
            std.c.free(@ptrCast(*anyopaque, err));
            return errStr;
        }
    }

    fn get(self: RocksDB, key: [:0]const u8) struct { val: [*c]const u8, err: ?CError } {
        var readOptions = rdb.rocksdb_readoptions_create();
        var valueLength: usize = 0;
        var err: ?CError = null;
        var v = rdb.rocksdb_get(self.db, readOptions, @ptrCast([*c]const u8, key), key.len, &valueLength, &err);
        if (err) |errStr| {
            std.debug.print("Could not get value for key: {s}", .{errStr});
            std.c.free(@ptrCast(*anyopaque, err));
            return "";
        }
        if (v == 0) {
            return RocksDBError.NotFound;
        }

        return v;
    }
};

// Unused
fn kvMain() !void {
    var db = RocksDB.open();
    defer db.close();

    var args = std.process.args();
    var key: [:0]const u8 = "";
    var value: [:0]const u8 = "";
    var command = "get";
    _ = args.next(); // Skip first arg
    while (args.next()) |arg| {
        if (mem.eql(u8, arg, "set")) {
            key = args.next().?;
            value = args.next().?;
            command = "set";
        } else if (mem.eql(u8, arg, "get")) {
            key = args.next().?;
        } else {
            std.debug.print("Must specify command (get or set). Got: {s}", .{arg});
            return;
        }
    }

    if (mem.eql(u8, command, "set")) {
        db.set(key, value);
    } else {
        var v = db.get(key) catch {
            std.debug.print("Key not found.", .{});
            return;
        };
        std.debug.print("{s}", .{v});
    }
}

const Token = struct {
    start: u64,
    end: u64,
    kind: Kind,
    source: []const u8,

    const Kind = enum {
        select_keyword,
        create_keyword,
        insert_keyword,
        values_keyword,
        from_keyword,
        where_keyword,

        equal_operator,

        left_paren_syntax,
        right_paren_syntax,
        comma_syntax,

        identifier,
        numeric,
    };

    fn print(self: Token) void {
        std.debug.print("{s}", .{self.source[self.start..self.end]});
    }

    fn debug(self: Token, msg: []const u8) void {
        var line: usize = 0;
        var column: usize = 0;
        var lineStartIndex: usize = 0;
        var lineEndIndex: usize = 0;
        var i: usize = 0;
        var source = self.source;
        while (i < source.len) {
            if (source[i] == '\n') {
                line = line + 1;
                column = 0;
                lineStartIndex = i;
            } else {
                column = column + 1;
            }

            if (i == self.start) {
                // Find the end of the line
                lineEndIndex = i;
                while (source[lineEndIndex] != '\n') {
                    lineEndIndex = lineEndIndex + 1;
                }
                break;
            }

            i = i + 1;
        }

        std.debug.print("{s}\nNear line {}, column {}.\n{s}\n", .{ msg, line + 1, column, source[lineStartIndex..lineEndIndex] });
        while (column - 1 > 0) {
            std.debug.print(" ", .{});
            column = column - 1;
        }
        std.debug.print("^ Near here\n\n", .{});
    }
};

fn debug(tokens: ArrayList(Token), preferredIndex: usize, msg: []const u8) void {
    var i = preferredIndex;
    while (i >= tokens.items.len) {
        i = i - 0;
    }

    tokens.items[i].debug(msg);
}

fn eatWhitespace(source: []const u8, index: usize) usize {
    var res = index;
    while (source[res] == ' ' or
        source[res] == '\n' or
        source[res] == '\t' or
        source[res] == '\r')
    {
        res = res + 1;
        if (res == source.len) {
            break;
        }
    }

    return res;
}

const Builtin = struct {
    name: []const u8,
    kind: Token.Kind,
};

// These must be sorted by length of the name text, descending
var BUILTINS = [_]Builtin{
    .{ .name = "CREATE TABLE", .kind = Token.Kind.create_keyword },
    .{ .name = "INSERT INTO", .kind = Token.Kind.insert_keyword },
    .{ .name = "SELECT", .kind = Token.Kind.select_keyword },
    .{ .name = "VALUES", .kind = Token.Kind.values_keyword },
    .{ .name = "WHERE", .kind = Token.Kind.where_keyword },
    .{ .name = "FROM", .kind = Token.Kind.from_keyword },
    .{ .name = "=", .kind = Token.Kind.equal_operator },
    .{ .name = "(", .kind = Token.Kind.left_paren_syntax },
    .{ .name = ")", .kind = Token.Kind.right_paren_syntax },
    .{ .name = ",", .kind = Token.Kind.comma_syntax },
};

comptime {
    std.debug.assert(BUILTINS.len == @typeInfo(Token.Kind).Enum.fields.len - 2);
}

fn lexKeyword(source: []const u8, index: usize) struct { nextPosition: usize, token: ?Token } {
    var longestLen: usize = 0;
    var kind = Token.Kind.select_keyword;
    for (BUILTINS) |builtin| {
        if (index + builtin.name.len >= source.len) {
            continue;
        }

        if (mem.eql(u8, source[index .. index + builtin.name.len], builtin.name)) {
            longestLen = builtin.name.len;
            kind = builtin.kind;
            // First match is the longest match
            break;
        }
    }

    if (longestLen == 0) {
        return .{ .nextPosition = 0, .token = null };
    }

    return .{ .nextPosition = index + longestLen, .token = Token{ .source = source, .start = index, .end = index + longestLen, .kind = kind } };
}

fn lexNumeric(source: []const u8, index: usize) struct { nextPosition: usize, token: ?Token } {
    var start = index;
    var end = index;
    var i = index;
    while (source[i] >= '0' and source[i] <= '9') {
        end = end + 1;
        i = i + 1;
    }

    if (start == end) {
        return .{ .nextPosition = 0, .token = null };
    }

    return .{ .nextPosition = end, .token = Token{ .source = source, .start = start, .end = end, .kind = Token.Kind.numeric } };
}

fn lexIdentifier(source: []const u8, index: usize) struct { nextPosition: usize, token: ?Token } {
    var start = index;
    var end = index;
    var i = index;
    while ((source[i] >= 'a' and source[i] <= 'z') or
        (source[i] >= 'A' and source[i] <= 'Z') or
        (source[i] == '*'))
    {
        end = end + 1;
        i = i + 1;
    }

    if (start == end) {
        return .{ .nextPosition = 0, .token = null };
    }

    return .{ .nextPosition = end, .token = Token{ .source = source, .start = start, .end = end, .kind = Token.Kind.identifier } };
}

fn lex(source: []const u8, tokens: *ArrayList(Token)) ?Error {
    var i: usize = 0;
    while (true) {
        i = eatWhitespace(source, i);
        if (i >= source.len) {
            break;
        }

        const keywordRes = lexKeyword(source, i);
        if (keywordRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate space for keyword token";
            i = keywordRes.nextPosition;
            continue;
        }

        const numericRes = lexNumeric(source, i);
        if (numericRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate space for numeric token";
            i = numericRes.nextPosition;
            continue;
        }

        const identifierRes = lexIdentifier(source, i);
        if (identifierRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate space for identifier token";
            i = identifierRes.nextPosition;
            continue;
        }

        if (tokens.items.len > 0) {
            debug(tokens.*, tokens.items.len - 1, "Last good token.\n");
        }
        return "Bad token";
    }

    return null;
}

const BinaryOperationAST = struct {
    operator: Token,
    left: ExpressionAST,
    right: ExpressionAST,

    fn print(self: BinaryOperationAST) void {
        self.left.print();
        std.debug.print(" ", .{});
        self.operator.print();
        std.debug.print(" ", .{});
        self.right.print();
    }
};

const ExpressionAST = struct {
    kind: Kind,
    literal: *Token,
    binary_operation: *BinaryOperationAST,

    const Kind = enum {
        literal,
        binary_operation,
    };

    fn print(self: ExpressionAST) void {
        if (self.kind == .literal) {
            self.literal.print();
        } else {
            self.binary_operation.print();
        }
    }
};

const SelectAST = struct {
    columns: ArrayList(Token),
    from: Token,
    where: ?*ExpressionAST,

    fn print(self: SelectAST) void {
        std.debug.print("SELECT\n", .{});
        for (self.columns.items) |column, i| {
            std.debug.print("  ", .{});
            column.print();
            if (i < self.columns.items.len - 1) {
                std.debug.print(",", .{});
            }
            std.debug.print("\n", .{});
        }
        std.debug.print("FROM\n  ", .{});
        self.from.print();

        if (self.where != null) {
            std.debug.print("\nWHERE\n  ", .{});
            self.where.?.print();
        }

        std.debug.print("\n", .{});
    }
};

const InsertAST = struct {
    table: Token,
    columns: ArrayList(Token),
    values: ArrayList(ExpressionAST),

    fn print(self: InsertAST) void {
        std.debug.print("INSERT INTO ", .{});
        self.table.print();
        std.debug.print(" VALUES (", .{});
        for (self.values.items) |value, i| {
            value.print();
            if (i < self.values.items.len - 1) {
                std.debug.print(", ", .{});
            }
        }
        std.debug.print(")\n", .{});
    }
};

const CreateColumnAST = struct {
    name: Token,
    kind: Token,
};

const CreateAST = struct {
    table: Token,
    columns: ArrayList(CreateColumnAST),

    fn print(self: CreateAST) void {
        std.debug.print("CREATE TABLE ", .{});
        self.table.print();
        std.debug.print(" (\n", .{});
        for (self.columns.items) |column, i| {
            std.debug.print("  ", .{});
            column.name.print();
            std.debug.print(" ", .{});
            column.kind.print();
            if (i < self.columns.items.len - 1) {
                std.debug.print(",", .{});
            }
            std.debug.print("\n", .{});
        }
        std.debug.print(")\n", .{});
    }
};

const AST = struct {
    select: *SelectAST,
    insert: *InsertAST,
    create: *CreateAST,
    kind: Token.Kind,

    fn print(self: AST) void {
        if (self.kind == .select_keyword) {
            self.select.print();
        } else if (self.kind == .insert_keyword) {
            self.insert.print();
        } else if (self.kind == .create_keyword) {
            self.create.print();
        } else {
            std.debug.print("[UNKNOWN!]", .{});
        }
    }
};

const Parser = struct {
    allocator: mem.Allocator,

    fn init(allocator: mem.Allocator) Parser {
        return Parser{ .allocator = allocator };
    }

    fn expectTokenKind(tokens: ArrayList(Token), index: usize, kind: Token.Kind) bool {
        if (index >= tokens.items.len) {
            return false;
        }

        return tokens.items[index].kind == kind;
    }

    fn parseExpression(self: Parser, tokens: ArrayList(Token), index: usize) struct { val: ?ExpressionAST, nextPosition: usize, err: ?Error } {
        var i = index;

        var e = ExpressionAST{
            .kind = undefined,
            .literal = undefined,
            .binary_operation = undefined,
        };

        if (expectTokenKind(tokens, i, Token.Kind.numeric) or
            expectTokenKind(tokens, i, Token.Kind.identifier))
        {
            e.kind = ExpressionAST.Kind.literal;
            e.literal = self.allocator.create(Token) catch return .{ .val = null, .nextPosition = 0, .err = "Could not allocate for token." };
            e.literal.* = tokens.items[i];
            i = i + 1;
        } else {
            return .{ .val = null, .nextPosition = 0, .err = "No expression" };
        }

        if (expectTokenKind(tokens, i, Token.Kind.equal_operator)) {
            var oldE = e;
            e = ExpressionAST{
                .kind = ExpressionAST.Kind.binary_operation,
                .literal = undefined,
                .binary_operation = undefined,
            };
            e.binary_operation = self.allocator.create(BinaryOperationAST) catch return .{ .val = null, .nextPosition = 0, .err = "Could not allocate for BinaryOperationAST." };
            e.binary_operation.* = BinaryOperationAST{
                .operator = tokens.items[i],
                .left = oldE,
                .right = undefined,
            };

            var rightRes = self.parseExpression(tokens, i + 1);
            if (rightRes.err != null) {
                return .{ .val = null, .nextPosition = 0, .err = rightRes.err };
            }

            e.binary_operation.right = rightRes.val.?;
            i = rightRes.nextPosition;
        }

        return .{ .val = e, .nextPosition = i, .err = null };
    }

    fn parseSelect(self: Parser, tokens: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.select_keyword)) {
            return .{ .val = null, .err = "Expected SELECT keyword" };
        }
        i = i + 1;

        var select = self.allocator.create(SelectAST) catch return .{ .val = null, .err = "Could not allocate SelectAST" };
        select.columns = ArrayList(Token).init(self.allocator);

        // Parse columns
        while (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            if (select.columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    debug(tokens, i, "Expected comma.\n");
                    return .{ .val = null, .err = "Expected comma." };
                }

                i = i + 1;
            }

            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                debug(tokens, i, "Expected identifier after this.\n");
                return .{ .val = null, .err = "Expected identifier." };
            }

            select.columns.append(tokens.items[i]) catch return .{ .val = null, .err = "Could not allocate for token." };
            i = i + 1;
        }

        if (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            debug(tokens, i, "Expected FROM keyword after this.\n");
            return .{ .val = null, .err = "Expected FROM keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            debug(tokens, i, "Expected FROM table name after this.\n");
            return .{ .val = null, .err = "Expected FROM keyword" };
        }
        select.from = tokens.items[i];
        i = i + 1;

        if (expectTokenKind(tokens, i, Token.Kind.where_keyword)) {
            // i + 1, skip past the where
            var res = self.parseExpression(tokens, i + 1);
            if (res.err != null) {
                return .{ .val = null, .err = res.err };
            }

            select.where = self.allocator.create(ExpressionAST) catch return .{ .val = null, .err = "Could not allocate ExpressionAST" };
            select.where.?.* = res.val.?;
            i = res.nextPosition;
        } else {
            std.debug.print("{}\n", .{tokens.items[i]});
        }

        if (i < tokens.items.len) {
            debug(tokens, i, "Unexpected token.");
            return .{ .val = null, .err = "Did not complete parsing SELECT" };
        }

        return .{ .val = AST{
            .kind = Token.Kind.select_keyword,
            .select = select,
            .create = undefined,
            .insert = undefined,
        }, .err = null };
    }

    fn parseCreate(self: Parser, tokens: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.create_keyword)) {
            return .{ .val = null, .err = "Expected CREATE TABLE keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            debug(tokens, i, "Expected table name after CREATE TABLE keyword.\n");
            return .{ .val = null, .err = "Expected CREATE TABLE name" };
        }

        var create = self.allocator.create(CreateAST) catch return .{ .val = null, .err = "Could not allocate CreateAST" };
        create.columns = ArrayList(CreateColumnAST).init(self.allocator);
        create.table = tokens.items[i];
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.left_paren_syntax)) {
            debug(tokens, i, "Expected opening paren after CREATE TABLE name.\n");
            return .{ .val = null, .err = "Expected opening paren" };
        }
        i = i + 1;

        while (!expectTokenKind(tokens, i, Token.Kind.right_paren_syntax)) {
            if (create.columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    debug(tokens, i, "Expected comma.\n");
                    return .{ .val = null, .err = "Expected comma." };
                }

                i = i + 1;
            }

            var column = CreateColumnAST{ .name = undefined, .kind = undefined };
            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                debug(tokens, i, "Expected column name after comma.\n");
                return .{ .val = null, .err = "Expected identifier." };
            }

            column.name = tokens.items[i];
            i = i + 1;

            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                debug(tokens, i, "Expected column type after column name.\n");
                return .{ .val = null, .err = "Expected identifier." };
            }

            column.kind = tokens.items[i];
            i = i + 1;

            create.columns.append(column) catch return .{ .val = null, .err = "Could not allocate for column." };
        }

        // Skip past final paren.
        i = i + 1;

        if (i < tokens.items.len) {
            debug(tokens, i, "Unexpected token.");
            return .{ .val = null, .err = "Did not complete parsing CREATE TABLE" };
        }

        return .{ .val = AST{
            .kind = Token.Kind.create_keyword,
            .select = undefined,
            .create = create,
            .insert = undefined,
        }, .err = null };
    }

    fn parseInsert(self: Parser, tokens: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.insert_keyword)) {
            return .{ .val = null, .err = "Expected INSERT INTO keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            debug(tokens, i, "Expected table name after INSERT INTO keyword.\n");
            return .{ .val = null, .err = "Expected INSERT INTO table name" };
        }

        var insert = self.allocator.create(InsertAST) catch return .{ .val = null, .err = "Could not allocate InsertAST" };
        insert.values = ArrayList(ExpressionAST).init(self.allocator);
        insert.table = tokens.items[i];
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.values_keyword)) {
            debug(tokens, i, "Expected VALUES keyword.\n");
            return .{ .val = null, .err = "Expected VALUES keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.left_paren_syntax)) {
            debug(tokens, i, "Expected opening paren after CREATE TABLE name.\n");
            return .{ .val = null, .err = "Expected opening paren" };
        }
        i = i + 1;

        while (!expectTokenKind(tokens, i, Token.Kind.right_paren_syntax)) {
            if (insert.values.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    debug(tokens, i, "Expected comma.\n");
                    return .{ .val = null, .err = "Expected comma." };
                }

                i = i + 1;
            }

            var expressionRes = self.parseExpression(tokens, i);
            if (expressionRes.err != null) {
                return .{ .val = null, .err = expressionRes.err };
            }

            insert.values.append(expressionRes.val.?) catch return .{ .val = null, .err = "Could not allocate for expression." };
            i = expressionRes.nextPosition;
        }

        // Skip past final paren.
        i = i + 1;

        if (i < tokens.items.len) {
            debug(tokens, i, "Unexpected token.");
            return .{ .val = null, .err = "Did not complete parsing INSERT INTO" };
        }

        return .{ .val = AST{
            .kind = Token.Kind.insert_keyword,
            .select = undefined,
            .create = undefined,
            .insert = insert,
        }, .err = null };
    }

    fn parse(self: Parser, tokens: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        if (expectTokenKind(tokens, 0, Token.Kind.select_keyword)) {
            var res = self.parseSelect(tokens);
            return .{ .val = res.val, .err = res.err };
        }

        if (expectTokenKind(tokens, 0, Token.Kind.create_keyword)) {
            var res = self.parseCreate(tokens);
            return .{ .val = res.val, .err = res.err };
        }

        if (expectTokenKind(tokens, 0, Token.Kind.insert_keyword)) {
            var res = self.parseInsert(tokens);
            return .{ .val = res.val, .err = res.err };
        }

        return .{ .val = null, .err = "Unknown statement" };
    }
};

fn execute(_: RocksDB, _: AST) struct { val: ?AST, err: ?Error } {
    return .{ .val = null, .err = null };
}

pub fn main() !void {
    if (std.os.argv.len < 2) {
        std.debug.print("Expected file name to interpret", .{});
        return;
    }

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const file = try std.fs.cwd().openFileZ(std.os.argv[1], .{});
    defer file.close();

    const file_size = try file.getEndPos();
    var prog = try allocator.alloc(u8, file_size);

    _ = try file.read(prog);

    var tokens = ArrayList(Token).init(allocator);
    const lexErr = lex(prog, &tokens);
    if (lexErr) |err| {
        std.debug.print("Failed to lex: {s}", .{err});
        return;
    }

    // for (tokens.items) |token| {
    //     std.debug.print("Token: ", .{});
    //     token.print();
    //     std.debug.print("\n", .{});
    // }

    if (tokens.items.len == 0) {
        std.debug.print("Program is empty", .{});
        return;
    }

    const parser = Parser.init(allocator);
    const parseRes = parser.parse(tokens);
    if (parseRes.err) |err| {
        std.debug.print("Failed to parse: {s}", .{err});
        return;
    }

    if (parseRes.val) |ast| {
        ast.print();
    }

    const openRes = RocksDB.open();
    if (openRes.err) |err| {
        std.debug.print("Failed to open database: {s}", .{err});
        return;
    }

    const executeRes = execute(openRes.val.?, parseRes.val.?);
    if (executeRes.err) |err| {
        std.debug.print("Failed to execute: {s}", .{err});
        return;
    }
}
