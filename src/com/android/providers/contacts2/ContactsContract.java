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

import android.net.Uri;
import android.provider.BaseColumns;

/**
 * The contract between the contacts provider and applications. Contains definitions
 * for the supported URIs and columns.
 *
 * TODO: move to android.provider
 */
public final class ContactsContract {
    /** The authority for the contacts provider */
    public static final String AUTHORITY = "com.android.contacts";
    /** A content:// style uri to the authority for the contacts provider */
    public static final Uri AUTHORITY_URI = Uri.parse("content://" + AUTHORITY);

    private interface ContactsColumns {
        /**
         * The given name for the contact.
         * <P>Type: TEXT</P>
         */
        public static final String GIVEN_NAME = "given_name";

        /**
         * The phonetic version of the given name for the contact.
         * <P>Type: TEXT</P>
         */
        public static final String PHONETIC_GIVEN_NAME = "phonetic_given_name";

        /**
         * The family name for the contact.
         * <P>Type: TEXT</P>
         */
        public static final String FAMILY_NAME = "family_name";

        /**
         * The phonetic version of the family name for the contact.
         * <P>Type: TEXT</P>
         */
        public static final String PHONETIC_FAMILY_NAME = "phonetic_family_name";

        /**
         * The display name for the contact.
         * <P>Type: TEXT</P>
         */
        public static final String DISPLAY_NAME = "display_name";

        /**
         * The number of times a person has been contacted
         * <P>Type: INTEGER</P>
         */
        public static final String TIMES_CONTACTED = "times_contacted";

        /**
         * The last time a person was contacted.
         * <P>Type: INTEGER</P>
         */
        public static final String LAST_TIME_CONTACTED = "last_time_contacted";

        /**
         * A custom ringtone associated with a person. Not always present.
         * <P>Type: TEXT (URI to the ringtone)</P>
         */
        public static final String CUSTOM_RINGTONE = "custom_ringtone";

        /**
         * Whether the person should always be sent to voicemail. Not always
         * present.
         * <P>Type: INTEGER (0 for false, 1 for true)</P>
         */
        public static final String SEND_TO_VOICEMAIL = "send_to_voicemail";

        /**
         * Is the contact starred?
         * <P>Type: INTEGER (boolean)</P>
         */
        public static final String STARRED = "starred";
    }

    /**
     * Constants for the contacts table, which contains the base contact information.
     */
    public static final class Contacts implements BaseColumns, ContactsColumns {
        /**
         * This utility class cannot be instantiated
         */
        private Contacts()  {}

        /**
         * The content:// style URI for this table
         */
        public static final Uri CONTENT_URI = Uri.withAppendedPath(AUTHORITY_URI, "contacts");

        /**
         * The MIME type of {@link #CONTENT_URI} providing a directory of
         * people.
         */
        public static final String CONTENT_TYPE = "vnd.android.cursor.dir/person";

        /**
         * The MIME type of a {@link #CONTENT_URI} subdirectory of a single
         * person.
         */
        public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/person";

        /**
         * A sub-directory of a single contact that contains all of their {@link Data} rows.
         * To access this directory append
         */
        public static final class Data implements BaseColumns, DataColumns {
            /**
             * no public constructor since this is a utility class
             */
            private Data() {}

            /**
             * The directory twig for this sub-table
             */
            public static final String CONTENT_DIRECTORY = "data";
        }
    }

    private interface DataColumns {
        /**
         * The package name that defines this type of data.
         */
        public static final String PACKAGE = "package";

        /**
         * The kind of the data, scoped within the package stored in {@link #PACKAGE}.
         */
        public static final String KIND = "kind";

        /**
         * A reference to the {@link Contacts#_ID} that this data belongs to.
         */
        public static final String CONTACT_ID = "contact_id";

        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA1 = "data1";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA2 = "data2";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA3 = "data3";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA4 = "data4";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA5 = "data5";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA6 = "data6";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA7 = "data7";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA8 = "data8";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA9 = "data9";
        /** Generic data column, the meaning is {@link #KIND} specific */
        public static final String DATA10 = "data10";
    }

    /**
     * Constants for the data table, which contains data points tied to a contact.
     * For example, a phone number or email address. Each row in this table contains a type
     * definition and some generic columns. Each data type can define the meaning for each of
     * the generic columns.
     */
    public static final class Data implements BaseColumns, DataColumns {
        /**
         * This utility class cannot be instantiated
         */
        private Data() {}

        /**
         * The content:// style URI for this table
         */
        public static final Uri CONTENT_URI = Uri.withAppendedPath(AUTHORITY_URI, "data");
    }

    /**
     * A table that represents the result of looking up a phone number, for example for caller ID.
     * The table joins that data row for the phone number with the contact that owns the number.
     * To perform a lookup you must append the number you want to find to {@link #CONTENT_URI}.
     */
    public static final class PhoneLookup implements BaseColumns, DataColumns, ContactsColumns {
        /**
         * This utility class cannot be instantiated
         */
        private PhoneLookup() {}

        /**
         * The content:// style URI for this table. Append the phone number you want to lookup
         * to this URI and query it to perform a lookup. For example:
         *
         * {@code
         * Uri lookupUri = Uri.withAppendedPath(PhoneLookup.CONTENT_URI, phoneNumber);
         * }
         */
        public static final Uri CONTENT_URI = Uri.withAppendedPath(AUTHORITY_URI, "phone_lookup");
    }

    /**
     * Container for definitions of common data types stored in the {@link Data} table.
     */
    public static final class CommonDataKinds {
        /** The {@link Data#PACKAGE} value for the common data kinds */
        public static final String PACKAGE_COMMON = "common";

        /**
         * Columns common across the specific types.
         */
        private interface BaseCommonColumns {
            /**
             * The package name that defines this type of data.
             */
            public static final String PACKAGE = "package";

            /**
             * The kind of the data, scoped within the package stored in {@link #PACKAGE}.
             */
            public static final String KIND = "kind";

            /**
             * A reference to the {@link Contacts#_ID} that this data belongs to.
             */
            public static final String CONTACT_ID = "contact_id";
        }

        /**
         * Columns common across the specific types.
         */
        private interface CommonColumns {
            /**
             * The type of data, for example Home or Work.
             * <P>Type: INTEGER</P>
             */
            public static final String TYPE = "data1";

            /**
             * The user defined label for the the contact method.
             * <P>Type: TEXT</P>
             */
            public static final String LABEL = "data2";

            /**
             * The data for the contact method.
             * <P>Type: TEXT</P>
             */
            public static final String DATA = "data3";

            /**
             * Whether this is the primary entry of its kind for the contact it belongs to
             * <P>Type: INTEGER (if set, non-0 means true)</P>
             */
            public static final String ISPRIMARY = "data4";
        }

        /**
         * Common data definition for telephone numbers.
         */
        public static final class Phone implements BaseCommonColumns {
            private Phone() {}

            /** Signifies a phone number row that is stored in the data table */
            public static final int KIND_PHONE = 5;

            /**
             * The type of data, for example Home or Work.
             * <P>Type: INTEGER</P>
             */
            public static final String TYPE = "data1";

            public static final int TYPE_CUSTOM = 0;
            public static final int TYPE_HOME = 1;
            public static final int TYPE_MOBILE = 2;
            public static final int TYPE_WORK = 3;
            public static final int TYPE_FAX_WORK = 4;
            public static final int TYPE_FAX_HOME = 5;
            public static final int TYPE_PAGER = 6;
            public static final int TYPE_OTHER = 7;

            /**
             * The user provided label, only used if TYPE is {@link #TYPE_CUSTOM}.
             * <P>Type: TEXT</P>
             */
            public static final String LABEL = "data2";

            /**
             * The phone number as the user entered it.
             * <P>Type: TEXT</P>
             */
            public static final String NUMBER = "data3";

            /**
             * Whether this is the primary phone number
             * <P>Type: INTEGER (if set, non-0 means true)</P>
             */
            public static final String ISPRIMARY = "data4";
        }

        /**
         * Common data definition for email addresses.
         */
        public static final class Email implements BaseCommonColumns, CommonColumns {
            private Email() {}

            /** Signifies an email address row that is stored in the data table */
            public static final int KIND = 1;

            public static final int TYPE_CUSTOM = 0;
            public static final int TYPE_HOME = 1;
            public static final int TYPE_WORK = 2;
            public static final int TYPE_OTHER = 3;
        }

        /**
         * Common data definition for postal addresses.
         */
        public static final class Postal implements BaseCommonColumns{
            private Postal() {}

            /** Signifies a postal address row that is stored in the data table */
            public static final int KIND = 2;

            public static final int TYPE_CUSTOM = 0;
            public static final int TYPE_HOME = 1;
            public static final int TYPE_WORK = 2;
            public static final int TYPE_OTHER = 3;
        }

       /**
        * Common data definition for IM addresses.
        */
        public static final class Im implements BaseCommonColumns {
            private Im() {}

            /** Signifies an IM address row that is stored in the data table */
            public static final int KIND = 3;

            public static final int TYPE_CUSTOM = 0;
            public static final int TYPE_HOME = 1;
            public static final int TYPE_WORK = 2;
            public static final int TYPE_OTHER = 3;
        }

        /**
         * Common data definition for organizations.
         */
        public static final class Organization implements BaseCommonColumns {
            private Organization() {}

            /** Signifies an organization row that is stored in the data table */
            public static final int KIND = 4;

            public static final int TYPE_CUSTOM = 0;
            public static final int TYPE_HOME = 1;
            public static final int TYPE_WORK = 2;
            public static final int TYPE_OTHER = 3;

            /**
             * The user provided label, only used if TYPE is {@link #TYPE_CUSTOM}.
             * <P>Type: TEXT</P>
             */
            public static final String LABEL = "label";

            /**
             * The name of the company for this organization.
             * <P>Type: TEXT</P>
             */
            public static final String COMPANY = "company";

            /**
             * The title within this organization.
             * <P>Type: TEXT</P>
             */
            public static final String TITLE = "title";

            /**
             * Whether this is the primary organization
             * <P>Type: INTEGER (if set, non-0 means true)</P>
             */
            public static final String ISPRIMARY = "isprimary";
        }

        /**
         * Notes about the contact.
         */
        public static final class Note implements BaseCommonColumns {
            private Note() {}

            /** Signifies a free-form note row that is stored in the data table */
            public static final int KIND = 6;

            /**
             * The note text.
             * <P>Type: TEXT</P>
             */
            public static final String NOTE = "data1";
        }
    }
}
