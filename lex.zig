const std = @import("std");

const Error = @import("result.zig").Error;

pub const Token = struct {
    start: u64,
    end: u64,
    kind: Kind,
    source: []const u8,

    pub const Kind = enum {
        select_keyword,
        create_table_keyword,
        insert_keyword,
        values_keyword,
        from_keyword,
        where_keyword,

        plus_operator,
        equal_operator,
        lt_operator,
        concat_operator,

        left_paren_syntax,
        right_paren_syntax,
        comma_syntax,

        identifier,
        numeric,
        string,
    };

    pub fn string(self: Token) []const u8 {
        return self.source[self.start..self.end];
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

        std.debug.print(
            "{s}\nNear line {}, column {}.\n{s}\n",
            .{ msg, line + 1, column, source[lineStartIndex..lineEndIndex] },
        );
        while (column - 1 > 0) {
            std.debug.print(" ", .{});
            column = column - 1;
        }
        std.debug.print("^ Near here\n\n", .{});
    }
};

pub fn debug(tokens: []Token, preferredIndex: usize, msg: []const u8) void {
    var i = preferredIndex;
    while (i >= tokens.len) {
        i = i - 0;
    }

    tokens[i].debug(msg);
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
    .{ .name = "CREATE TABLE", .kind = Token.Kind.create_table_keyword },
    .{ .name = "INSERT INTO", .kind = Token.Kind.insert_keyword },
    .{ .name = "SELECT", .kind = Token.Kind.select_keyword },
    .{ .name = "VALUES", .kind = Token.Kind.values_keyword },
    .{ .name = "WHERE", .kind = Token.Kind.where_keyword },
    .{ .name = "FROM", .kind = Token.Kind.from_keyword },
    .{ .name = "||", .kind = Token.Kind.concat_operator },
    .{ .name = "=", .kind = Token.Kind.equal_operator },
    .{ .name = "+", .kind = Token.Kind.plus_operator },
    .{ .name = "<", .kind = Token.Kind.lt_operator },
    .{ .name = "(", .kind = Token.Kind.left_paren_syntax },
    .{ .name = ")", .kind = Token.Kind.right_paren_syntax },
    .{ .name = ",", .kind = Token.Kind.comma_syntax },
};

//comptime {
//    std.debug.assert(BUILTINS.len == @typeInfo(Token.Kind).Enum.fields.len - 3);
//}

fn asciiCaseInsensitiveEqual(left: []const u8, right: []const u8) bool {
    var min = left;
    if (right.len < left.len) {
        min = right;
    }

    for (min) |_, i| {
        var l = left[i];
        if (l >= 97 and l <= 122) {
            l = l - 32;
        }

        var r = right[i];
        if (r >= 97 and r <= 122) {
            r = r - 32;
        }

        if (l != r) {
            return false;
        }
    }

    return true;
}

fn lexKeyword(source: []const u8, index: usize) struct { nextPosition: usize, token: ?Token } {
    var longestLen: usize = 0;
    var kind = Token.Kind.select_keyword;
    for (BUILTINS) |builtin| {
        if (index + builtin.name.len >= source.len) {
            continue;
        }

        if (asciiCaseInsensitiveEqual(source[index .. index + builtin.name.len], builtin.name)) {
            longestLen = builtin.name.len;
            kind = builtin.kind;
            // First match is the longest match
            break;
        }
    }

    if (longestLen == 0) {
        return .{ .nextPosition = 0, .token = null };
    }

    return .{
        .nextPosition = index + longestLen,
        .token = Token{
            .source = source,
            .start = index,
            .end = index + longestLen,
            .kind = kind,
        },
    };
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

    return .{
        .nextPosition = end,
        .token = Token{
            .source = source,
            .start = start,
            .end = end,
            .kind = Token.Kind.numeric,
        },
    };
}

fn lexString(source: []const u8, index: usize) struct { nextPosition: usize, token: ?Token } {
    var i = index;
    if (source[i] != '\'') {
        return .{ .nextPosition = 0, .token = null };
    }
    i = i + 1;

    var start = i;
    var end = i;
    while (source[i] != '\'') {
        end = end + 1;
        i = i + 1;
    }

    if (source[i] == '\'') {
        i = i + 1;
    }

    if (start == end) {
        return .{ .nextPosition = 0, .token = null };
    }

    return .{
        .nextPosition = i,
        .token = Token{
            .source = source,
            .start = start,
            .end = end,
            .kind = Token.Kind.string,
        },
    };
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

    return .{
        .nextPosition = end,
        .token = Token{
            .source = source,
            .start = start,
            .end = end,
            .kind = Token.Kind.identifier,
        },
    };
}

pub fn lex(source: []const u8, tokens: *std.ArrayList(Token)) ?Error {
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

        const stringRes = lexString(source, i);
        if (stringRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate space for string token";
            i = stringRes.nextPosition;
            continue;
        }

        const identifierRes = lexIdentifier(source, i);
        if (identifierRes.token) |token| {
            tokens.append(token) catch return "Failed to allocate space for identifier token";
            i = identifierRes.nextPosition;
            continue;
        }

        if (tokens.items.len > 0) {
            debug(tokens.items, tokens.items.len - 1, "Last good token.\n");
        }
        return "Bad token";
    }

    return null;
}
