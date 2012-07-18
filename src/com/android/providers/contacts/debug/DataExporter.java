/*
 * Copyright (C) 2012 The Android Open Source Project
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
 * limitations under the License.
 */

package com.android.providers.contacts.debug;

import android.content.Context;
import android.media.MediaScannerConnection;
import android.util.Log;

import com.google.common.io.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Compress all files under the app data dir into a single zip file.
 */
public class DataExporter {
    private static String TAG = "DataExporter";

    public static final String ZIP_MIME_TYPE = "application/zip";

    /**
     * Compress all files under the app data dir into a single zip file.
     */
    public static void exportData(Context context, File outFile) throws IOException {
        outFile.delete();
        Log.i(TAG, "Outfile=" + outFile.getAbsolutePath());

        final ZipOutputStream os = new ZipOutputStream(new FileOutputStream(outFile));
        try {
            addDirectory(os, context.getFilesDir().getParentFile(), "contacts-files");
        } finally {
            Closeables.closeQuietly(os);
        }
        // Tell the media scanner about the new file so that it is
        // immediately available to the user.
        MediaScannerConnection.scanFile(context,
                new String[] {outFile.toString()},
                new String[] {ZIP_MIME_TYPE}, null);
    }

    /**
     * Add all files under {@code current} to {@code os} zip stream
     */
    private static void addDirectory(ZipOutputStream os, File current, String storedPath)
            throws IOException {
        for (File child : current.listFiles()) {
            final String childStoredPath = storedPath + "/" + child.getName();

            if (child.isDirectory()) {
                addDirectory(os, child, childStoredPath);
            } else if (child.isFile()) {
                addFile(os, child, childStoredPath);
            } else {
                // Shouldn't happen; skip.
            }
        }
    }

    /**
     * Add a single file {@code current} to {@code os} zip stream using the file name
     * {@code storedPath}.
     */
    private static void addFile(ZipOutputStream os, File current, String storedPath)
            throws IOException {
        final InputStream is = new FileInputStream(current);
        os.putNextEntry(new ZipEntry(storedPath));

        final byte[] buf = new byte[32 * 1024];
        int totalLen = 0;
        while (true) {
            int len = is.read(buf);
            if (len <= 0) {
                break;
            }
            os.write(buf, 0, len);
            totalLen += len;
        }
        os.closeEntry();
        Log.i(TAG, "Added " + current.getAbsolutePath() + " as " + storedPath +
                " (" + totalLen + " bytes)");
    }
}
