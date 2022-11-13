const std = @import("std");

const lex = @import("lex.zig");
const Result = @import("types.zig").Result;
const Token = lex.Token;

pub const BinaryOperationAST = struct {
    operator: Token,
    left: *ExpressionAST,
    right: *ExpressionAST,

    fn print(self: BinaryOperationAST) void {
        self.left.print();
        std.debug.print(" {s} ", .{self.operator.string()});
        self.right.print();
    }
};

pub const ExpressionAST = union(enum) {
    literal: Token,
    binary_operation: BinaryOperationAST,

    fn print(self: ExpressionAST) void {
        switch (self) {
            .literal => |literal| switch (literal.kind) {
                .string => std.debug.print("'{s}'", .{literal.string()}),
                else => std.debug.print("{s}", .{literal.string()}),
            },
            .binary_operation => self.binary_operation.print(),
        }
    }
};

pub const SelectAST = struct {
    columns: []ExpressionAST,
    from: Token,
    where: ?ExpressionAST,

    fn print(self: SelectAST) void {
        std.debug.print("SELECT\n", .{});
        for (self.columns) |column, i| {
            std.debug.print("  ", .{});
            column.print();
            if (i < self.columns.len - 1) {
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
    values: []ExpressionAST,

    fn print(self: InsertAST) void {
        std.debug.print("INSERT INTO {s} VALUES (", .{self.table.string()});
        for (self.values) |value, i| {
            value.print();
            if (i < self.values.len - 1) {
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
    columns: []CreateTableColumnAST,

    fn print(self: CreateTableAST) void {
        std.debug.print("CREATE TABLE {s} (\n", .{self.table.string()});
        for (self.columns) |column, i| {
            std.debug.print(
                "  {s} {s}",
                .{ column.name.string(), column.kind.string() },
            );
            if (i < self.columns.len - 1) {
                std.debug.print(",", .{});
            }
            std.debug.print("\n", .{});
        }
        std.debug.print(")\n", .{});
    }
};

pub const AST = union(enum) {
    select: SelectAST,
    insert: InsertAST,
    create_table: CreateTableAST,

    pub fn print(self: AST) void {
        switch (self) {
            .select => |select| select.print(),
            .insert => |insert| insert.print(),
            .create_table => |create_table| create_table.print(),
        }
    }
};

pub const Parser = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Parser {
        return Parser{ .allocator = allocator };
    }

    fn expectTokenKind(tokens: []Token, index: usize, kind: Token.Kind) bool {
        if (index >= tokens.len) {
            return false;
        }

        return tokens[index].kind == kind;
    }

    fn parseExpression(self: Parser, tokens: []Token, index: usize) Result(struct {
        ast: ExpressionAST,
        nextPosition: usize,
    }) {
        var i = index;

        var e: ExpressionAST = undefined;

        if (expectTokenKind(tokens, i, Token.Kind.numeric) or
            expectTokenKind(tokens, i, Token.Kind.identifier) or
            expectTokenKind(tokens, i, Token.Kind.string))
        {
            e = ExpressionAST{ .literal = tokens[i] };
            i = i + 1;
        } else {
            return .{ .err = "No expression" };
        }

        if (expectTokenKind(tokens, i, Token.Kind.equal_operator) or
            expectTokenKind(tokens, i, Token.Kind.lt_operator) or
            expectTokenKind(tokens, i, Token.Kind.plus_operator) or
            expectTokenKind(tokens, i, Token.Kind.concat_operator))
        {
            var newE = ExpressionAST{
                .binary_operation = BinaryOperationAST{
                    .operator = tokens[i],
                    .left = self.allocator.create(ExpressionAST) catch return .{
                        .err = "Could not allocate for left expression.",
                    },
                    .right = self.allocator.create(ExpressionAST) catch return .{
                        .err = "Could not allocate for right expression.",
                    },
                },
            };
            newE.binary_operation.left.* = e;
            e = newE;

            switch (self.parseExpression(tokens, i + 1)) {
                .err => |err| return .{ .err = err },
                .val => |val| {
                    e.binary_operation.right.* = val.ast;
                    i = val.nextPosition;
                },
            }
        }

        return .{ .val = .{ .ast = e, .nextPosition = i } };
    }

    fn parseSelect(self: Parser, tokens: []Token) Result(AST) {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.select_keyword)) {
            return .{ .err = "Expected SELECT keyword" };
        }
        i = i + 1;

        var columns = std.ArrayList(ExpressionAST).init(self.allocator);
        var select = SelectAST{
            .columns = undefined,
            .from = undefined,
            .where = null,
        };

        // Parse columns
        while (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            if (columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    lex.debug(tokens, i, "Expected comma.\n");
                    return .{ .err = "Expected comma." };
                }

                i = i + 1;
            }

            switch (self.parseExpression(tokens, i)) {
                .err => |err| return .{ .err = err },
                .val => |val| {
                    i = val.nextPosition;

                    columns.append(val.ast) catch return .{
                        .err = "Could not allocate for token.",
                    };
                },
            }
        }

        if (!expectTokenKind(tokens, i, Token.Kind.from_keyword)) {
            lex.debug(tokens, i, "Expected FROM keyword after this.\n");
            return .{ .err = "Expected FROM keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            lex.debug(tokens, i, "Expected FROM table name after this.\n");
            return .{ .err = "Expected FROM keyword" };
        }
        select.from = tokens[i];
        i = i + 1;

        if (expectTokenKind(tokens, i, Token.Kind.where_keyword)) {
            // i + 1, skip past the where
            switch (self.parseExpression(tokens, i + 1)) {
                .err => |err| return .{ .err = err },
                .val => |val| {
                    select.where = val.ast;
                    i = val.nextPosition;
                },
            }
        }

        if (i < tokens.len) {
            lex.debug(tokens, i, "Unexpected token.");
            return .{ .err = "Did not complete parsing SELECT" };
        }

        select.columns = columns.items;
        return .{ .val = AST{ .select = select } };
    }

    fn parseCreateTable(self: Parser, tokens: []Token) Result(AST) {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.create_table_keyword)) {
            return .{ .err = "Expected CREATE TABLE keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            lex.debug(tokens, i, "Expected table name after CREATE TABLE keyword.\n");
            return .{ .err = "Expected CREATE TABLE name" };
        }

        var columns = std.ArrayList(CreateTableColumnAST).init(self.allocator);
        var create_table = CreateTableAST{
            .columns = undefined,
            .table = tokens[i],
        };
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.left_paren_syntax)) {
            lex.debug(tokens, i, "Expected opening paren after CREATE TABLE name.\n");
            return .{ .err = "Expected opening paren" };
        }
        i = i + 1;

        while (!expectTokenKind(tokens, i, Token.Kind.right_paren_syntax)) {
            if (columns.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    lex.debug(tokens, i, "Expected comma.\n");
                    return .{ .err = "Expected comma." };
                }

                i = i + 1;
            }

            var column = CreateTableColumnAST{ .name = undefined, .kind = undefined };
            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                lex.debug(tokens, i, "Expected column name after comma.\n");
                return .{ .err = "Expected identifier." };
            }

            column.name = tokens[i];
            i = i + 1;

            if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
                lex.debug(tokens, i, "Expected column type after column name.\n");
                return .{ .err = "Expected identifier." };
            }

            column.kind = tokens[i];
            i = i + 1;

            columns.append(column) catch return .{
                .err = "Could not allocate for column.",
            };
        }

        // Skip past final paren.
        i = i + 1;

        if (i < tokens.len) {
            lex.debug(tokens, i, "Unexpected token.");
            return .{ .err = "Did not complete parsing CREATE TABLE" };
        }

        create_table.columns = columns.items;
        return .{ .val = AST{ .create_table = create_table } };
    }

    fn parseInsert(self: Parser, tokens: []Token) Result(AST) {
        var i: usize = 0;
        if (!expectTokenKind(tokens, i, Token.Kind.insert_keyword)) {
            return .{ .err = "Expected INSERT INTO keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.identifier)) {
            lex.debug(tokens, i, "Expected table name after INSERT INTO keyword.\n");
            return .{ .err = "Expected INSERT INTO table name" };
        }

        var values = std.ArrayList(ExpressionAST).init(self.allocator);
        var insert = InsertAST{
            .values = undefined,
            .table = tokens[i],
        };
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.values_keyword)) {
            lex.debug(tokens, i, "Expected VALUES keyword.\n");
            return .{ .err = "Expected VALUES keyword" };
        }
        i = i + 1;

        if (!expectTokenKind(tokens, i, Token.Kind.left_paren_syntax)) {
            lex.debug(tokens, i, "Expected opening paren after CREATE TABLE name.\n");
            return .{ .err = "Expected opening paren" };
        }
        i = i + 1;

        while (!expectTokenKind(tokens, i, Token.Kind.right_paren_syntax)) {
            if (values.items.len > 0) {
                if (!expectTokenKind(tokens, i, Token.Kind.comma_syntax)) {
                    lex.debug(tokens, i, "Expected comma.\n");
                    return .{ .err = "Expected comma." };
                }

                i = i + 1;
            }

            switch (self.parseExpression(tokens, i)) {
                .err => |err| return .{ .err = err },
                .val => |val| {
                    values.append(val.ast) catch return .{
                        .err = "Could not allocate for expression.",
                    };
                    i = val.nextPosition;
                },
            }
        }

        // Skip past final paren.
        i = i + 1;

        if (i < tokens.len) {
            lex.debug(tokens, i, "Unexpected token.");
            return .{ .err = "Did not complete parsing INSERT INTO" };
        }

        insert.values = values.items;
        return .{ .val = AST{ .insert = insert } };
    }

    pub fn parse(self: Parser, tokens: []Token) Result(AST) {
        if (expectTokenKind(tokens, 0, Token.Kind.select_keyword)) {
            return switch (self.parseSelect(tokens)) {
                .err => |err| .{ .err = err },
                .val => |val| .{ .val = val },
            };
        }

        if (expectTokenKind(tokens, 0, Token.Kind.create_table_keyword)) {
            return switch (self.parseCreateTable(tokens)) {
                .err => |err| .{ .err = err },
                .val => |val| .{ .val = val },
            };
        }

        if (expectTokenKind(tokens, 0, Token.Kind.insert_keyword)) {
            return switch (self.parseInsert(tokens)) {
                .err => |err| .{ .err = err },
                .val => |val| .{ .val = val },
            };
        }

        return .{ .err = "Unknown statement" };
    }
};
