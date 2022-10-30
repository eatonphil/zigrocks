const std = @import("std");

const lex = @import("lex.zig");
const Token = lex.Token;

const Error = []const u8;

const BinaryOperationAST = struct {
    operator: Token,
    left: ExpressionAST,
    right: ExpressionAST,

    fn print(self: BinaryOperationAST) void {
        self.left.print();
        std.debug.print(" {s} ", .{self.operator.string()});
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
            std.debug.print("{s}", .{self.literal.string()});
        } else {
            self.binary_operation.print();
        }
    }
};

pub const SelectAST = struct {
    columns: std.ArrayList(Token),
    from: Token,
    where: ?*ExpressionAST,

    fn print(self: SelectAST) void {
        std.debug.print("SELECT\n", .{});
        for (self.columns.items) |column, i| {
            std.debug.print("  {s}", .{column.string()});
            if (i < self.columns.items.len - 1) {
                std.debug.print(",", .{});
            }
            std.debug.print("\n", .{});
        }
        std.debug.print("FROM\n  {s}", .{self.from.string()});

        if (self.where) |where| {
            std.debug.print("\nWHERE\n  ", .{});
            where.print();
        }

        std.debug.print("\n", .{});
    }
};

pub const InsertAST = struct {
    table: Token,
    values: std.ArrayList(ExpressionAST),

    fn print(self: InsertAST) void {
        std.debug.print("INSERT INTO {s} VALUES (", .{self.table.string()});
        for (self.values.items) |value, i| {
            value.print();
            if (i < self.values.items.len - 1) {
                std.debug.print(", ", .{});
            }
        }
        std.debug.print(")\n", .{});
    }
};

const CreateTableColumnAST = struct {
    name: Token,
    kind: Token,
};

pub const CreateTableAST = struct {
    table: Token,
    columns: std.ArrayList(CreateTableColumnAST),

    fn print(self: CreateTableAST) void {
        std.debug.print("CREATE TABLE {s} (\n", .{self.table.string()});
        for (self.columns.items) |column, i| {
            std.debug.print(
                "  {s} {s}",
                .{ column.name.string(), column.kind.string() },
            );
            if (i < self.columns.items.len - 1) {
                std.debug.print(",", .{});
            }
            std.debug.print("\n", .{});
        }
        std.debug.print(")\n", .{});
    }
};

pub const AST = struct {
    select: *SelectAST,
    insert: *InsertAST,
    create_table: *CreateTableAST,
    kind: Token.Kind,

    pub fn print(self: AST) void {
        if (self.kind == .select_keyword) {
            self.select.print();
        } else if (self.kind == .insert_keyword) {
            self.insert.print();
        } else if (self.kind == .create_table_keyword) {
            self.create_table.print();
        } else {
            std.debug.print("Cannot print unknown statement", .{});
        }
    }
};

pub const Parser = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{ .allocator = allocator };
    }

    fn expectTokenKind(tokens: std.ArrayList(Token), index: usize, kind: Token.Kind) bool {
        if (index >= tokens.items.len) {
            return false;
        }

        return tokens.items[index].kind == kind;
    }

    fn parseExpression(self: Parser, tokens: std.ArrayList(Token), index: usize) struct {
        val: ?ExpressionAST,
        nextPosition: usize,
        err: ?Error,
    } {
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
            e.literal = self.allocator.create(Token) catch return .{
                .val = null,
                .nextPosition = 0,
                .err = "Could not allocate for token.",
            };
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
            e.binary_operation = self.allocator.create(BinaryOperationAST) catch return .{
                .val = null,
                .nextPosition = 0,
                .err = "Could not allocate for BinaryOperationAST.",
            };
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

    fn parseSelect(self: Parser, tokens: std.ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.select_keyword)) {
            return .{ .val = null, .err = "Expected SELECT keyword" };
        }
        i = i + 1;

        var select = self.allocator.create(SelectAST) catch return .{
            .val = null,
            .err = "Could not allocate SelectAST",
        };
        select.columns = std.ArrayList(Token).init(self.allocator);

        // Parse columns
        while (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            if (select.columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    lex.debug(tokens, i, "Expected comma.\n");
                    return .{ .val = null, .err = "Expected comma." };
                }

                i = i + 1;
            }

            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                lex.debug(tokens, i, "Expected identifier after this.\n");
                return .{ .val = null, .err = "Expected identifier." };
            }

            select.columns.append(tokens.items[i]) catch return .{
                .val = null,
                .err = "Could not allocate for token.",
            };
            i = i + 1;
        }

        if (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            lex.debug(tokens, i, "Expected FROM keyword after this.\n");
            return .{ .val = null, .err = "Expected FROM keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            lex.debug(tokens, i, "Expected FROM table name after this.\n");
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

            select.where = self.allocator.create(ExpressionAST) catch return .{
                .val = null,
                .err = "Could not allocate ExpressionAST",
            };
            select.where.?.* = res.val.?;
            i = res.nextPosition;
        }

        if (i < tokens.items.len) {
            lex.debug(tokens, i, "Unexpected token.");
            return .{ .val = null, .err = "Did not complete parsing SELECT" };
        }

        return .{ .val = AST{
            .kind = Token.Kind.select_keyword,
            .select = select,
            .create_table = undefined,
            .insert = undefined,
        }, .err = null };
    }

    fn parseCreateTable(self: Parser, tokens: std.ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.create_table_keyword)) {
            return .{ .val = null, .err = "Expected CREATE TABLE keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            lex.debug(tokens, i, "Expected table name after CREATE TABLE keyword.\n");
            return .{ .val = null, .err = "Expected CREATE TABLE name" };
        }

        var create_table = self.allocator.create(CreateTableAST) catch return .{
            .val = null,
            .err = "Could not allocate CreateTableAST",
        };
        create_table.columns = std.ArrayList(CreateTableColumnAST).init(self.allocator);
        create_table.table = tokens.items[i];
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.left_paren_syntax)) {
            lex.debug(tokens, i, "Expected opening paren after CREATE TABLE name.\n");
            return .{ .val = null, .err = "Expected opening paren" };
        }
        i = i + 1;

        while (!expectTokenKind(tokens, i, Token.Kind.right_paren_syntax)) {
            if (create_table.columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    lex.debug(tokens, i, "Expected comma.\n");
                    return .{ .val = null, .err = "Expected comma." };
                }

                i = i + 1;
            }

            var column = CreateTableColumnAST{ .name = undefined, .kind = undefined };
            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                lex.debug(tokens, i, "Expected column name after comma.\n");
                return .{ .val = null, .err = "Expected identifier." };
            }

            column.name = tokens.items[i];
            i = i + 1;

            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                lex.debug(tokens, i, "Expected column type after column name.\n");
                return .{ .val = null, .err = "Expected identifier." };
            }

            column.kind = tokens.items[i];
            i = i + 1;

            create_table.columns.append(column) catch return .{
                .val = null,
                .err = "Could not allocate for column.",
            };
        }

        // Skip past final paren.
        i = i + 1;

        if (i < tokens.items.len) {
            lex.debug(tokens, i, "Unexpected token.");
            return .{ .val = null, .err = "Did not complete parsing CREATE TABLE" };
        }

        return .{ .val = AST{
            .kind = Token.Kind.create_table_keyword,
            .select = undefined,
            .create_table = create_table,
            .insert = undefined,
        }, .err = null };
    }

    fn parseInsert(self: Parser, tokens: std.ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.insert_keyword)) {
            return .{ .val = null, .err = "Expected INSERT INTO keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            lex.debug(tokens, i, "Expected table name after INSERT INTO keyword.\n");
            return .{ .val = null, .err = "Expected INSERT INTO table name" };
        }

        var insert = self.allocator.create(InsertAST) catch return .{
            .val = null,
            .err = "Could not allocate InsertAST",
        };
        insert.values = std.ArrayList(ExpressionAST).init(self.allocator);
        insert.table = tokens.items[i];
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.values_keyword)) {
            lex.debug(tokens, i, "Expected VALUES keyword.\n");
            return .{ .val = null, .err = "Expected VALUES keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.left_paren_syntax)) {
            lex.debug(tokens, i, "Expected opening paren after CREATE TABLE name.\n");
            return .{ .val = null, .err = "Expected opening paren" };
        }
        i = i + 1;

        while (!expectTokenKind(tokens, i, Token.Kind.right_paren_syntax)) {
            if (insert.values.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    lex.debug(tokens, i, "Expected comma.\n");
                    return .{ .val = null, .err = "Expected comma." };
                }

                i = i + 1;
            }

            var expressionRes = self.parseExpression(tokens, i);
            if (expressionRes.err != null) {
                return .{ .val = null, .err = expressionRes.err };
            }

            insert.values.append(expressionRes.val.?) catch return .{
                .val = null,
                .err = "Could not allocate for expression.",
            };
            i = expressionRes.nextPosition;
        }

        // Skip past final paren.
        i = i + 1;

        if (i < tokens.items.len) {
            lex.debug(tokens, i, "Unexpected token.");
            return .{ .val = null, .err = "Did not complete parsing INSERT INTO" };
        }

        return .{ .val = AST{
            .kind = Token.Kind.insert_keyword,
            .select = undefined,
            .create_table = undefined,
            .insert = insert,
        }, .err = null };
    }

    pub fn parse(self: Parser, tokens: std.ArrayList(Token)) struct { val: ?AST, err: ?Error } {
        if (expectTokenKind(tokens, 0, Token.Kind.select_keyword)) {
            var res = self.parseSelect(tokens);
            return .{ .val = res.val, .err = res.err };
        }

        if (expectTokenKind(tokens, 0, Token.Kind.create_table_keyword)) {
            var res = self.parseCreateTable(tokens);
            return .{ .val = res.val, .err = res.err };
        }

        if (expectTokenKind(tokens, 0, Token.Kind.insert_keyword)) {
            var res = self.parseInsert(tokens);
            return .{ .val = res.val, .err = res.err };
        }

        return .{ .val = null, .err = "Unknown statement" };
    }
};
