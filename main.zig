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

    pub fn open() struct { val: ?RocksDB, err: ?CError } {
        var options: ?*rdb.rocksdb_options_t = rdb.rocksdb_options_create();
        rdb.rocksdb_options_set_create_if_missing(options, 1);
        var err: ?CError = null;
        var db = rdb.rocksdb_open(options, "/tmp/testdb", &err);
        if (err != null) {
            return .{ .val = null, .err = err };
        }
        return .{ .val = RocksDB{ .db = db.? }, .err = null };
    }

    pub fn close(self: RocksDB) void {
        rdb.rocksdb_close(self.db);
    }

    pub fn set(self: RocksDB, key: [:0]const u8, value: [:0]const u8) ?CError {
        var writeOptions = rdb.rocksdb_writeoptions_create();
        var err: ?CError = null;
        rdb.rocksdb_put(self.db, writeOptions, @ptrCast([*c]const u8, key), key.len, @ptrCast([*c]const u8, value), value.len, &err);
        if (err) |errStr| {
            std.c.free(@ptrCast(*anyopaque, err));
            return errStr;
        }
    }

    pub fn get(self: RocksDB, key: [:0]const u8) struct { val: [*c]const u8, err: ?CError } {
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
        into_keyword,
        where_keyword,
        int_keyword,

        true_value,
        false_value,

        equal_operator,

        left_paren_syntax,
        right_paren_syntax,
        comma_syntax,

        identifier,
        numeric,
    };

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
                lineEndIndex = i;
            } else {
                column = column + 1;
            }

            if (i == self.start) {
                while (source[i] != '\n') {
                    lineEndIndex = lineEndIndex + 1;
                    i = i + 1;
                }
                break;
            }

            i = i + 1;
        }

        std.debug.print("{s}\nNear line {}, column {}.\n{s}", .{ msg, line + 1, column, source });
        while (column - 1 > 0) {
            std.debug.print(" ", .{});
            column = column - 1;
        }
        std.debug.print("^ Near here\n\n", .{});
    }
};

fn eatWhitespace(source: []const u8, index: usize) usize {
    var res = index;
    while (source[index] == ' ' or
        source[index] == '\n' or
        source[index] == '\t' or
        source[index] == '\r')
    {
        res = res + 1;
    }

    return res;
}

const Builtin = struct {
    name: []const u8,
    kind: Token.Kind,
};

var BUILTINS = block: {
    var builtins: [13]Builtin = undefined;
    builtins[0] = .{ .name = "SELECT", .kind = Token.Kind.select_keyword };
    builtins[1] = .{ .name = "CREATE", .kind = Token.Kind.create_keyword };
    builtins[2] = .{ .name = "INSERT", .kind = Token.Kind.insert_keyword };
    builtins[3] = .{ .name = "VALUES", .kind = Token.Kind.values_keyword };
    builtins[4] = .{ .name = "WHERE", .kind = Token.Kind.where_keyword };
    builtins[5] = .{ .name = "FALSE", .kind = Token.Kind.false_value };
    builtins[6] = .{ .name = "FROM", .kind = Token.Kind.from_keyword };
    builtins[7] = .{ .name = "INTO", .kind = Token.Kind.into_keyword };
    builtins[8] = .{ .name = "TRUE", .kind = Token.Kind.true_value };
    builtins[9] = .{ .name = "=", .kind = Token.Kind.equal_operator };
    builtins[10] = .{ .name = "(", .kind = Token.Kind.left_paren_syntax };
    builtins[11] = .{ .name = ")", .kind = Token.Kind.right_paren_syntax };
    builtins[12] = .{ .name = ",", .kind = Token.Kind.comma_syntax };

    break :block builtins;
};

fn lexKeyword(source: []const u8, index: usize) struct { nextPosition: usize, token: ?Token } {
    var longestLen: usize = 0;
    var kind = Token.Kind.select_keyword;
    for (BUILTINS) |builtin| {
        if (index + builtin.name.len >= source.len or builtin.name.len < longestLen) {
            continue;
        }

        if (mem.eql(u8, source[index .. index + builtin.name.len], builtin.name)) {
            longestLen = builtin.name.len;
            kind = builtin.kind;
        }
    }

    if (longestLen == 0) {
        return .{ .nextPosition = 0, .token = null };
    }

    return .{ .nextPosition = index + longestLen + 1, .token = Token{ .source = source, .start = index, .end = index + longestLen, .kind = kind } };
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

    return .{ .nextPosition = end + 1, .token = Token{ .source=source, .start = start, .end = end, .kind = Token.Kind.numeric } };
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

    return .{ .nextPosition = end + 1, .token = Token{.source=source, .start = start, .end = end, .kind = Token.Kind.identifier } };
}

fn lex(source: []const u8, tokens: *ArrayList(Token)) ?Error {
    var i: usize = 0;
    while (i < source.len) {
        i = eatWhitespace(source, i);

        const keywordRes = lexKeyword(source, i);
        if (keywordRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate";
            i = keywordRes.nextPosition;
            continue;
        }

        const numericRes = lexNumeric(source, i);
        if (numericRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate";
            i = numericRes.nextPosition;
            continue;
        }

        const identifierRes = lexIdentifier(source, i);
        if (identifierRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate";
            i = identifierRes.nextPosition;
            continue;
        }

        if (tokens.items.len > 0) {
            tokens.items[tokens.items.len - 1].debug("Last good token.\n");
        }
        return "Bad token";
    }

    return null;
}

const BinaryOperationAST = struct {
    operator: Token,
    left: ExpressionAST,
    right: ExpressionAST,
};

const ExpressionAST = struct {
    kind: Kind,
    literal: *Token,
    binary_operation: *BinaryOperationAST,

    const Kind = enum {
        literal,
        binary_operation,
    };
};

const SelectAST = struct {
    columns: ArrayList(Token),
    from: Token,
    where: *ExpressionAST,
};

const InsertAST = struct {
    table: Token,
    columns: ArrayList(Token),
    values: ArrayList(ExpressionAST),
};

const CreateColumnAST = struct {
    name: Token,
    kind: Token,
};

const CreateAST = struct {
    table: Token,
    columns: ArrayList(CreateColumnAST),
};

const AST = struct {
    select: *SelectAST,
    insert: *InsertAST,
    create: *CreateAST,
    kind: Token.Kind,
};

const Parser = struct {
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator) Parser {
        return Parser{.allocator=allocator};
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
            expectTokenKind(tokens, i, Token.Kind.identifier)) {
            e.kind = ExpressionAST.Kind.literal;
            e.literal = self.allocator.create(Token) catch return .{.val = null, .nextPosition = 0, .err = "Could not allocate for token."};
            e.literal.* = tokens.items[i];
        }

        return .{.val = e, .nextPosition = i, .err = null};
    }

    fn parseSelect(self: Parser, tokens: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.select_keyword)) {
            return .{ .val = null, .err = "Expected SELECT keyword" };
        }
        i = i + 1;

        var select = SelectAST{
            .columns = ArrayList(Token).init(self.allocator),
            .from = undefined,
            .where = undefined,
        };

        // Parse columns
        while (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            if (select.columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    tokens.items[i-1].debug("Expected comma after this.\n");
                    return .{.val = null, .err = "Expected comma."};
                }

                i = i + 1;
            }

            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                select.columns.append(tokens.items[i]) catch return .{.val = null, .err = "Could not allocate for token."};
                tokens.items[i-1].debug("Expected identifier after this.\n");
            }
            i = i + 1;
        }

        if (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            tokens.items[i-1].debug("Expected FROM keyword after this.\n");
            return .{ .val = null, .err = "Expected FROM keyword" };
        }
        select.from = tokens.items[i];
        i = i + 1;

        if (expectTokenKind(tokens, i, Token.Kind.where_keyword)) {
            var res = self.parseExpression(tokens, i + 1);
            if (res.err != null) {
                return .{.val = null, .err = res.err};
            }

            select.where = self.allocator.create(ExpressionAST) catch return .{.val = null, .err = "Could not allocate ExpressionAST"};
            select.where.* = res.val.?;
            i = res.nextPosition;
        }

        var s = self.allocator.create(SelectAST) catch return .{.val = null, .err = "Could not allocate SelectAST"};
        s.* = select;

        return .{
            .val=AST{
                .kind = Token.Kind.select_keyword,
                .select = s,
                .create = undefined,
                .insert = undefined,
            },
            .err = null};
    }

    fn parseCreate(_:Parser, _: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        return .{ .val = null, .err = "Expected CREATE keyword" };
    }

    fn parseInsert(_: Parser, _: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        return .{ .val = null, .err = "Expected INSERT keyword" };
    }

    pub fn parse(self: Parser, tokens: ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        if (expectTokenKind(tokens, 0, Token.Kind.select_keyword)) {
            var res = self.parseSelect(tokens);
            return .{.val = res.val, .err = res.err};
        }

        if (expectTokenKind(tokens, 0, Token.Kind.create_keyword)) {
            var res = self.parseCreate(tokens);
            return .{.val = res.val, .err = res.err};
        }

        if (expectTokenKind(tokens, 0, Token.Kind.insert_keyword)) {
            var res = self.parseInsert(tokens);
            return .{.val = res.val, .err = res.err};
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
