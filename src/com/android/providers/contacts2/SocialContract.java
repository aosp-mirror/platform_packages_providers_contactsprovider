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

package com.android.providers.contacts2;

import com.android.providers.contacts2.ContactsContract.Contacts;

import android.graphics.BitmapFactory;
import android.net.Uri;
import android.provider.BaseColumns;

/**
 * The contract between the social provider and applications. Contains
 * definitions for the supported URIs and columns.
 * <p>
 * TODO: move to android.provider package
 */
public class SocialContract {
    /** The authority for the social provider */
    public static final String AUTHORITY = "com.android.social";

    /** A content:// style uri to the authority for the contacts provider */
    public static final Uri AUTHORITY_URI = Uri.parse("content://" + AUTHORITY);

    private interface ActivitiesColumns {
        /**
         * The package name that owns this social activity.
         * <p>
         * Type: TEXT
         */
        public static final String PACKAGE = "package";

        /**
         * The mime-type of this social activity.
         * <p>
         * Type: TEXT
         */
        public static final String MIMETYPE = "mimetype";

        /**
         * Internal raw identifier for this social activity. This field is
         * analogous to the <code>atom:id</code> element defined in RFC 4287.
         * <p>
         * Type: TEXT
         */
        public static final String RAW_ID = "raw_id";

        /**
         * Reference to another {@link Activities#RAW_ID} that this social activity
         * is replying to. This field is analogous to the
         * <code>thr:in-reply-to</code> element defined in RFC 4685.
         * <p>
         * Type: TEXT
         */
        public static final String IN_REPLY_TO = "in_reply_to";

        /**
         * Reference to the {@link Contacts#_ID} that authored this social
         * activity. This field is analogous to the <code>atom:author</code>
         * element defined in RFC 4287.
         * <p>
         * Type: INTEGER
         */
        public static final String AUTHOR_CONTACT_ID = "author_contact_id";

        /**
         * Optional reference to the {@link Contacts#_ID} this social activity
         * is targeted towards. If more than one direct target, this field may
         * be left undefined. This field is analogous to the
         * <code>activity:target</code> element defined in the Atom Activity
         * Extensions Internet-Draft.
         * <p>
         * Type: INTEGER
         */
        public static final String TARGET_CONTACT_ID = "target_contact_id";

        /**
         * Timestamp when this social activity was published, in a
         * {@link System#currentTimeMillis()} time base. This field is analogous
         * to the <code>atom:published</code> element defined in RFC 4287.
         * <p>
         * Type: INTEGER
         */
        public static final String PUBLISHED = "published";

        /**
         * Timestamp when the original message in a thread was published.  For activities with null
         * {@link Activities#IN_REPLY_TO} this field contains the same value as the same as
         * {@link Activities#PUBLISHED}. For activities with none null
         * {@link Activities#IN_REPLY_TO}, it contains the value of
         * {@link Activities#PUBLISHED} of the original message in the thread.
         * <p>
         * Type: INTEGER
         */
        public static final String THREAD_PUBLISHED = "thread_published";

        /**
         * Title of this social activity. This field is analogous to the
         * <code>atom:title</code> element defined in RFC 4287.
         * <p>
         * Type: TEXT
         */
        public static final String TITLE = "title";

        /**
         * Summary of this social activity. This field is analogous to the
         * <code>atom:summary</code> element defined in RFC 4287.
         * <p>
         * Type: TEXT
         */
        public static final String SUMMARY = "summary";

        /**
         * Optional thumbnail specific to this social activity. This is the raw
         * bytes of an image that could be inflated using {@link BitmapFactory}.
         * <p>
         * Type: BLOB
         */
        public static final String THUMBNAIL = "thumbnail";
    }

    public static final class Activities implements BaseColumns, ActivitiesColumns {
        /**
         * This utility class cannot be instantiated
         */
        private Activities() {
        }

        /**
         * The content:// style URI for this table
         */
        public static final Uri CONTENT_URI = Uri.withAppendedPath(AUTHORITY_URI, "activities");

        /**
         * The content:// style URI for this table filtered to the set of
         * social activities authored by a specific {@link Contact#_ID}.
         */
        public static final Uri CONTENT_AUTHORED_BY_URI =
            Uri.withAppendedPath(CONTENT_URI, "authored_by");

        /**
         * The MIME type of {@link #CONTENT_URI} providing a directory of social
         * activities.
         */
        public static final String CONTENT_TYPE = "vnd.android.cursor.dir/activity";

        /**
         * The MIME type of a {@link #CONTENT_URI} subdirectory of a single
         * social activity.
         */
        public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/activity";
    }

}
