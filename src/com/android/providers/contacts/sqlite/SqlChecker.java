/*
 * Copyright (C) 2016 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package com.android.providers.contacts.sqlite;

import android.annotation.Nullable;
import android.util.ArraySet;
import android.util.Log;

import com.android.providers.contacts.AbstractContactsProvider;

import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Simple SQL validator to detect uses of hidden tables / columns as well as invalid SQLs.
 */
public class SqlChecker {
    private static final String TAG = "SqlChecker";

    private static final String PRIVATE_PREFIX = "x_"; // MUST BE LOWERCASE.

    private static final boolean VERBOSE_LOGGING = AbstractContactsProvider.VERBOSE_LOGGING;

    private final ArraySet<String> mInvalidTokens;

    /**
     * Create a new instance with given invalid tokens.
     */
    public SqlChecker(List<String> invalidTokens) {
        mInvalidTokens = new ArraySet<>(invalidTokens.size());

        for (int i = invalidTokens.size() - 1; i >= 0; i--) {
            mInvalidTokens.add(invalidTokens.get(i).toLowerCase());
        }
        if (VERBOSE_LOGGING) {
            Log.d(TAG, "Initialized with invalid tokens: " + invalidTokens);
        }
    }

    private static boolean isAlpha(char ch) {
        return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || (ch == '_');
    }

    private static boolean isNum(char ch) {
        return ('0' <= ch && ch <= '9');
    }

    private static boolean isAlNum(char ch) {
        return isAlpha(ch) || isNum(ch);
    }

    private static boolean isAnyOf(char ch, String set) {
        return set.indexOf(ch) >= 0;
    }

    /**
     * Exception for invalid queries.
     */
    @VisibleForTesting
    public static final class InvalidSqlException extends IllegalArgumentException {
        public InvalidSqlException(String s) {
            super(s);
        }
    }

    private static InvalidSqlException genException(String message, String sql) {
        throw new InvalidSqlException(message + " in '" + sql + "'");
    }

    private void throwIfContainsToken(String token, String sql) {
        final String lower = token.toLowerCase();
        if (mInvalidTokens.contains(lower) || lower.startsWith(PRIVATE_PREFIX)) {
            throw genException("Detected disallowed token: " + token, sql);
        }
    }

    /**
     * Ensure {@code sql} is valid and doesn't contain invalid tokens.
     */
    public void ensureNoInvalidTokens(@Nullable String sql) {
        findTokens(sql, OPTION_NONE, token -> throwIfContainsToken(token, sql));
    }

    /**
     * Ensure {@code sql} only contains a single, valid token.  Use to validate column names
     * in {@link android.content.ContentValues}.
     */
    public void ensureSingleTokenOnly(@Nullable String sql) {
        final AtomicBoolean tokenFound = new AtomicBoolean();

        findTokens(sql, OPTION_TOKEN_ONLY, token -> {
            if (tokenFound.get()) {
                throw genException("Multiple tokens detected", sql);
            }
            tokenFound.set(true);
            throwIfContainsToken(token, sql);
        });
        if (!tokenFound.get()) {
            throw genException("Token not found", sql);
        }
    }

    @VisibleForTesting
    static final int OPTION_NONE = 0;

    @VisibleForTesting
    static final int OPTION_TOKEN_ONLY = 1 << 0;

    private static char peek(String s, int index) {
        return index < s.length() ? s.charAt(index) : '\0';
    }

    /**
     * SQL Tokenizer specialized to extract tokens from SQL (snippets).
     *
     * Based on sqlite3GetToken() in tokenzie.c in SQLite.
     *
     * Source for v3.8.6 (which android uses): http://www.sqlite.org/src/artifact/ae45399d6252b4d7
     * (Latest source as of now: http://www.sqlite.org/src/artifact/78c8085bc7af1922)
     *
     * Also draft spec: http://www.sqlite.org/draft/tokenreq.html
     */
    @VisibleForTesting
    static void findTokens(@Nullable String sql, int options, Consumer<String> checker) {
        if (sql == null) {
            return;
        }
        int pos = 0;
        final int len = sql.length();
        while (pos < len) {
            final char ch = peek(sql, pos);

            // Regular token.
            if (isAlpha(ch)) {
                final int start = pos;
                pos++;
                while (isAlNum(peek(sql, pos))) {
                    pos++;
                }
                final int end = pos;

                final String token = sql.substring(start, end);
                checker.accept(token);

                continue;
            }

            // Handle quoted tokens
            if (isAnyOf(ch, "'\"`")) {
                final int quoteStart = pos;
                pos++;

                for (;;) {
                    pos = sql.indexOf(ch, pos);
                    if (pos < 0) {
                        throw genException("Unterminated quote", sql);
                    }
                    if (peek(sql, pos + 1) != ch) {
                        break;
                    }
                    // Quoted quote char -- e.g. "abc""def" is a single string.
                    pos += 2;
                }
                final int quoteEnd = pos;
                pos++;

                if (ch != '\'') {
                    // Extract the token
                    final String tokenUnquoted = sql.substring(quoteStart + 1, quoteEnd);

                    final String token;

                    // Unquote if needed. i.e. "aa""bb" -> aa"bb
                    if (tokenUnquoted.indexOf(ch) >= 0) {
                        token = tokenUnquoted.replaceAll(
                                String.valueOf(ch) + ch, String.valueOf(ch));
                    } else {
                        token = tokenUnquoted;
                    }
                    checker.accept(token);
                } else {
                    if ((options &= OPTION_TOKEN_ONLY) != 0) {
                        throw genException("Non-token detected", sql);
                    }
                }
                continue;
            }
            // Handle tokens enclosed in [...]
            if (ch == '[') {
                final int quoteStart = pos;
                pos++;

                pos = sql.indexOf(']', pos);
                if (pos < 0) {
                    throw genException("Unterminated quote", sql);
                }
                final int quoteEnd = pos;
                pos++;

                final String token = sql.substring(quoteStart + 1, quoteEnd);

                checker.accept(token);
                continue;
            }
            if ((options &= OPTION_TOKEN_ONLY) != 0) {
                throw genException("Non-token detected", sql);
            }

            // Detect comments.
            if (ch == '-' && peek(sql, pos + 1) == '-') {
                pos += 2;
                pos = sql.indexOf('\n', pos);
                if (pos < 0) {
                    // We disallow strings ending in an inline comment.
                    throw genException("Unterminated comment", sql);
                }
                pos++;

                continue;
            }
            if (ch == '/' && peek(sql, pos + 1) == '*') {
                pos += 2;
                pos = sql.indexOf("*/", pos);
                if (pos < 0) {
                    throw genException("Unterminated comment", sql);
                }
                pos += 2;

                continue;
            }

            // Semicolon is never allowed.
            if (ch == ';') {
                throw genException("Semicolon is not allowed", sql);
            }

            // For this purpose, we can simply ignore other characters.
            // (Note it doesn't handle the X'' literal properly and reports this X as a token,
            // but that should be fine...)
            pos++;
        }
    }
}
