/*
 * Copyright (C) 2011 The Android Open Source Project
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

package com.android.providers.contacts;

import android.database.CrossProcessCursor;
import android.database.Cursor;
import android.database.CursorWindow;
import android.database.CursorWrapper;
import android.provider.ContactsContract.Contacts;
import android.provider.ContactsContract.SearchSnippetColumns;
import android.text.TextUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cursor wrapper for use when the results include snippets.  This wrapper does special processing
 * when the snippet field is retrieved, converting the data in that column from the raw form that is
 * retrieved from the database into a snippet before returning (all other columns are simply passed
 * through).
 *
 * Note that this wrapper implements {@link CrossProcessCursor}, but will only behave as such if the
 * cursor it is wrapping is itself a {@link CrossProcessCursor} or another wrapper around the same.
 */
public class SnippetizingCursorWrapper extends CursorWrapper implements CrossProcessCursor {

    // Pattern for splitting a line into tokens.  This matches e-mail addresses as a single token,
    // otherwise splitting on any group of non-alphanumeric characters.
    Pattern SPLIT_PATTERN = Pattern.compile("([\\w-\\.]+)@((?:[\\w]+\\.)+)([a-zA-Z]{2,4})|[\\w]+");

    // The cross process cursor.  Only non-null if the wrapped cursor was a cross-process cursor.
    private final CrossProcessCursor mCrossProcessCursor;

    // Index of the snippet field (if any).
    private final int mSnippetIndex;

    // Parameters for snippetization.
    private final String mQuery;
    private final String mStartMatch;
    private final String mEndMatch;
    private final String mEllipsis;
    private final int mMaxTokens;

    // Whether to invoke the snippeting logic.  If the query consisted of multiple tokens, the DB
    // snippet() function was already used, so we should just return the snippet value directly.
    private final boolean mDoSnippetizing;

    /**
     * Creates a cursor wrapper that does special handling on the snippet field (converting the
     * raw content retrieved from the database into a snippet).
     * @param cursor The cursor to wrap.
     * @param query Query string.
     * @param startMatch String to insert at the start of matches in the snippet.
     * @param endMatch String to insert at the end of matches in the snippet.
     * @param ellipsis Ellipsis characters to use at the start or end of the snippet if appropriate.
     * @param maxTokens Maximum number of tokens to include in the snippet.
     */
    SnippetizingCursorWrapper(Cursor cursor, String query, String startMatch,
            String endMatch, String ellipsis, int maxTokens) {
        super(cursor);
        mCrossProcessCursor = getCrossProcessCursor(cursor);
        mSnippetIndex = getColumnIndex(SearchSnippetColumns.SNIPPET);
        mQuery = query;
        mStartMatch = startMatch;
        mEndMatch = endMatch;
        mEllipsis = ellipsis;
        mMaxTokens = maxTokens;
        mDoSnippetizing = mQuery.split(ContactsProvider2.QUERY_TOKENIZER_REGEX).length == 1;
    }

    private CrossProcessCursor getCrossProcessCursor(Cursor cursor) {
        if (cursor instanceof CrossProcessCursor) {
            return (CrossProcessCursor) cursor;
        } else if (cursor instanceof CursorWrapper) {
            return getCrossProcessCursor(((CursorWrapper) cursor).getWrappedCursor());
        } else {
            return null;
        }
    }

    @Override
    public void fillWindow(int pos, CursorWindow window) {
        if (mCrossProcessCursor != null) {
            mCrossProcessCursor.fillWindow(pos, window);
        } else {
            throw new UnsupportedOperationException("Wrapped cursor is not a cross-process cursor");
        }
    }

    @Override
    public CursorWindow getWindow() {
        if (mCrossProcessCursor != null) {
            return mCrossProcessCursor.getWindow();
        } else {
            throw new UnsupportedOperationException("Wrapped cursor is not a cross-process cursor");
        }
    }

    @Override
    public boolean onMove(int oldPosition, int newPosition) {
        if (mCrossProcessCursor != null) {
            return mCrossProcessCursor.onMove(oldPosition, newPosition);
        } else {
            throw new UnsupportedOperationException("Wrapped cursor is not a cross-process cursor");
        }
    }

    @Override
    public String getString(int columnIndex) {
        String columnContent = super.getString(columnIndex);

        // If the snippet column is being retrieved, do our custom snippetization logic.
        if (mDoSnippetizing && columnIndex == mSnippetIndex) {
            // Retrieve the display name - if it includes the query term, the snippet should be
            // left empty.
            int displayNameIndex = super.getColumnIndex(Contacts.DISPLAY_NAME);
            String displayName = displayNameIndex < 0 ? null : super.getString(displayNameIndex);
            return snippetize(columnContent, displayName);
        } else {
            return columnContent;
        }
    }

    /**
     * Creates a snippet for the given content, with the following algorithm:
     * <ul>
     *   <li>Check for the query term as a prefix of any token in the display name; if any match is
     *   found, no snippet should be computed (return null).</li>
     *   <li>Check for empty content (return null if found).</li>
     *   <li>Check for a custom snippet (phone number or email matches generate a usable snippet
     *   via a subquery)</li>
     *   <li>Check for a non-match of the query to the content - technically should never happen,
     *   but if it comes through, we'll just return null.</li>
     *   <li>Break the content into multiple lines.  For each line that includes the query string:
     *     <ul>
     *       <li>Tokenize it into alphanumeric chunks.</li>
     *       <li>Do a prefix match for the query against each token.</li>
     *       <li>If there's a match, replace the token with a version with the query term surrounded
     *       by the start and end match strings.</li>
     *       <li>If this is the first token in the line that has a match, compute the start and end
     *       token positions to use for this line (which will be used to create the snippet).</li>
     *       <li>If the line just processed had a match, reassemble the tokens (with ellipses, as
     *       needed) to produce the snippet, and return it.</li>
     *     </ul>
     *   </li>
     * </ul>
     * @param content The content to snippetize.
     * @param displayName Display name for the contact.
     * @return The snippet for the content, or null if no snippet should be shown.
     */
    // TODO: Tokenization is done based on alphanumeric characters, which may not be appropriate for
    // some locales (but this matches what the item view does for display).
    private String snippetize(String content, String displayName) {
        // If the display name already contains the query term, return empty - snippets should
        // not be needed in that case.
        String lowerDisplayName = displayName != null ? displayName.toLowerCase() : "";
        String lowerQuery = mQuery.toLowerCase();
        List<String> nameTokens = new ArrayList<String>();
        List<Integer> nameTokenOffsets = new ArrayList<Integer>();
        split(lowerDisplayName.trim(), nameTokens, nameTokenOffsets);
        for (String nameToken : nameTokens) {
            if (nameToken.startsWith(lowerQuery)) {
                return null;
            }
        }

        // If the content to snippetize is empty, return null.
        if (TextUtils.isEmpty(content)) {
            return null;
        }

        // Check to see if a custom snippet was already returned (identified by the string having no
        // newlines and beginning and ending with the split delimiters).
        String[] contentLines = content.split("\n");
        if (contentLines.length == 1 && !TextUtils.isEmpty(contentLines[0])
                && contentLines[0].startsWith(mStartMatch) && contentLines[0].endsWith(mEndMatch)) {
            // Custom snippet was retrieved - just return it.
            return content;
        }

        // If the content isn't a custom snippet and doesn't contain the query term, return null.
        if (!content.toLowerCase().contains(lowerQuery)) {
            return null;
        }

        // Locate the lines of the content that contain the query term.
        for (String contentLine : contentLines) {
            if (contentLine.toLowerCase().contains(lowerQuery)) {

                // Line contains the query string - now search for it at the start of tokens.
                List<String> lineTokens = new ArrayList<String>();
                List<Integer> tokenOffsets = new ArrayList<Integer>();
                split(contentLine.trim(), lineTokens, tokenOffsets);

                // As we find matches against the query, we'll populate this list with the marked
                // (or unchanged) tokens.
                List<String> markedTokens = new ArrayList<String>();

                int firstToken = -1;
                int lastToken = -1;
                for (int i = 0; i < lineTokens.size(); i++) {
                    String token = lineTokens.get(i);
                    String lowerToken = token.toLowerCase();
                    if (lowerToken.startsWith(lowerQuery)) {

                        // Query term matched; surround the token with match markers.
                        markedTokens.add(mStartMatch + token + mEndMatch);

                        // If this is the first token found with a match, mark the token
                        // positions to use for assembling the snippet.
                        if (firstToken == -1) {
                            firstToken =
                                    Math.max(0, i - (int) Math.floor(Math.abs(mMaxTokens) / 2.0));
                            lastToken =
                                    Math.min(lineTokens.size(), firstToken + Math.abs(mMaxTokens));
                        }
                    } else {
                        markedTokens.add(token);
                    }
                }

                // Assemble the snippet by piecing the tokens back together.
                if (firstToken > -1) {
                    StringBuilder sb = new StringBuilder();
                    if (firstToken > 0) {
                        sb.append(mEllipsis);
                    }
                    for (int i = firstToken; i < lastToken; i++) {
                        String markedToken = markedTokens.get(i);
                        String originalToken = lineTokens.get(i);
                        sb.append(markedToken);
                        if (i < lastToken - 1) {
                            // Add the characters that appeared between this token and the next.
                            sb.append(contentLine.substring(
                                    tokenOffsets.get(i) + originalToken.length(),
                                    tokenOffsets.get(i + 1)));
                        }
                    }
                    if (lastToken < lineTokens.size()) {
                        sb.append(mEllipsis);
                    }
                    return sb.toString();
                }
            }
        }
        return null;
    }

    /**
     * Helper method for splitting a string into tokens.  The lists passed in are populated with the
     * tokens and offsets into the content of each token.  The tokenization function parses e-mail
     * addresses as a single token; otherwise it splits on any non-alphanumeric character.
     * @param content Content to split.
     * @param tokens List of token strings to populate.
     * @param offsets List of offsets into the content for each token returned.
     */
    private void split(String content, List<String> tokens, List<Integer> offsets) {
        Matcher matcher = SPLIT_PATTERN.matcher(content);
        while (matcher.find()) {
            tokens.add(matcher.group());
            offsets.add(matcher.start());
        }
    }
}