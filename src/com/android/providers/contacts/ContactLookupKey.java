/*
 * Copyright (C) 2009 The Android Open Source Project
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

import android.net.Uri;
import android.text.TextUtils;

import java.util.ArrayList;

/**
 * Contacts lookup key. Used for generation and parsing of contact lookup keys as well
 * as doing the actual lookup.
 */
public class ContactLookupKey {

    public static class LookupKeySegment implements Comparable<LookupKeySegment> {
        public int accountHashCode;
        public boolean sourceIdLookup;
        public String key;
        public long contactId;

        public int compareTo(LookupKeySegment another) {
            if (contactId > another.contactId) {
                return -1;
            }
            if (contactId < another.contactId) {
                return 1;
            }
            return 0;
        }
    }

    /**
     * Returns a short hash code that functions as an additional precaution against the exceedingly
     * improbable collision between sync IDs in different accounts.
     */
    public static int getAccountHashCode(String accountType, String accountName) {
        if (accountType == null || accountName == null) {
            return 0;
        }

        return (accountType.hashCode() ^ accountName.hashCode()) & 0xFFF;
    }

    public static void appendToLookupKey(StringBuilder lookupKey, String accountType,
            String accountName, String sourceId, String displayName) {
        if (displayName == null) {
            displayName = "";
        }

        if (lookupKey.length() != 0) {
            lookupKey.append(".");
        }

        lookupKey.append(getAccountHashCode(accountType, accountName));
        if (sourceId == null) {
            lookupKey.append('n').append(NameNormalizer.normalize(displayName));
        } else {
            int pos = lookupKey.length();
            lookupKey.append('i');
            if (appendEscapedSourceId(lookupKey, sourceId)) {
                lookupKey.setCharAt(pos, 'e');
            }
        }
    }

    private static boolean appendEscapedSourceId(StringBuilder sb, String sourceId) {
        boolean escaped = false;
        int start = 0;
        while (true) {
            int index = sourceId.indexOf('.', start);
            if (index == -1) {
                sb.append(sourceId, start, sourceId.length());
                break;
            }

            escaped = true;
            sb.append(sourceId, start, index);
            sb.append("..");
            start = index + 1;
        }
        return escaped;
    }

    public ArrayList<LookupKeySegment> parse(String lookupKey) {
        ArrayList<LookupKeySegment> list = new ArrayList<LookupKeySegment>();

        String string = Uri.decode(lookupKey);
        int offset = 0;
        int length = string.length();
        int hashCode = 0;
        boolean sourceIdLookup = false;
        boolean escaped;
        String key;

        while (offset < length) {
            char c = 0;

            // Parse account hash code
            hashCode = 0;
            while (offset < length) {
                c = string.charAt(offset++);
                if (c < '0' || c > '9') {
                    break;
                }
                hashCode = hashCode * 10 + (c - '0');
            }

            // Parse segment type
            if (c == 'n') {
                sourceIdLookup = false;
                escaped = false;
            } else if (c == 'i') {
                sourceIdLookup = true;
                escaped = false;
            } else if (c == 'e') {
                sourceIdLookup = true;
                escaped = true;
            } else {
                throw new IllegalArgumentException("Invalid lookup id: " + lookupKey);
            }

            // Parse the source ID or normalized display name
            if (escaped) {
                StringBuffer sb = new StringBuffer();
                while (offset < length) {
                    c = string.charAt(offset++);

                    if (c == '.') {
                        if (offset == length) {
                            throw new IllegalArgumentException("Invalid lookup id: " + lookupKey);
                        }
                        c = string.charAt(offset);

                        if (c == '.') {
                            sb.append('.');
                            offset++;
                        } else {
                            break;
                        }
                    } else {
                        sb.append(c);
                    }
                }
                key = sb.toString();
            } else {
                int start = offset;
                while (offset < length) {
                    c = string.charAt(offset++);

                    if (c == '.') {
                        break;
                    }
                }
                if (offset == length) {
                    key = string.substring(start);
                } else {
                    key = string.substring(start, offset - 1);
                }
            }

            LookupKeySegment segment = new LookupKeySegment();
            segment.accountHashCode = hashCode;
            segment.key = key;
            segment.sourceIdLookup = sourceIdLookup;
            segment.contactId = -1;
            list.add(segment);
        }

        return list;
    }
}
