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

/**
 * Cursor wrapper that implements {@link CrossProcessCursor}, but will only behave as such if the
 * cursor it is wrapping is itself a {@link CrossProcessCursor} or another wrapper around the same.
 */
public class CrossProcessCursorWrapper extends CursorWrapper implements CrossProcessCursor {

    // The cross process cursor.  Only non-null if the wrapped cursor was a cross-process cursor.
    private final CrossProcessCursor mCrossProcessCursor;

    public CrossProcessCursorWrapper(Cursor cursor) {
        super(cursor);
        mCrossProcessCursor = getCrossProcessCursor(cursor);
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

}
