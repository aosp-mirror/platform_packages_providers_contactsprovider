/*
 * Copyright (C) 2011 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License
 */
package com.android.providers.contacts;

import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.graphics.Matrix;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Class that converts a bitmap (or byte array representing a bitmap) into a display
 * photo and a thumbnail photo.
 */
/* package-protected */ final class PhotoProcessor {

    private final int mMaxDisplayPhotoDim;
    private final int mMaxThumbnailPhotoDim;
    private final Bitmap mOriginal;
    private Bitmap mDisplayPhoto;
    private Bitmap mThumbnailPhoto;

    /**
     * Initializes a photo processor for the given bitmap.
     * @param original The bitmap to process.
     * @param maxDisplayPhotoDim The maximum height and width for the display photo.
     * @param maxThumbnailPhotoDim The maximum height and width for the thumbnail photo.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    public PhotoProcessor(Bitmap original, int maxDisplayPhotoDim, int maxThumbnailPhotoDim)
            throws IOException {
        mOriginal = original;
        mMaxDisplayPhotoDim = maxDisplayPhotoDim;
        mMaxThumbnailPhotoDim = maxThumbnailPhotoDim;
        process();
    }

    /**
     * Initializes a photo processor for the given bitmap.
     * @param originalBytes A byte array to decode into a bitmap to process.
     * @param maxDisplayPhotoDim The maximum height and width for the display photo.
     * @param maxThumbnailPhotoDim The maximum height and width for the thumbnail photo.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    public PhotoProcessor(byte[] originalBytes, int maxDisplayPhotoDim, int maxThumbnailPhotoDim)
            throws IOException {
        this(BitmapFactory.decodeByteArray(originalBytes, 0, originalBytes.length),
                maxDisplayPhotoDim, maxThumbnailPhotoDim);
    }

    /**
     * Processes the original image, producing a scaled-down display photo and thumbnail photo.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    private void process() throws IOException {
        if (mOriginal == null) {
            throw new IOException("Invalid image file");
        }
        mDisplayPhoto = scale(mMaxDisplayPhotoDim);
        mThumbnailPhoto = scale(mMaxThumbnailPhotoDim);
    }

    /**
     * Scales down the original bitmap to fit within the given maximum width and height.
     * If the bitmap already fits in those dimensions, the original bitmap will be
     * returned unmodified.
     * @param maxDim Maximum width and height (in pixels) for the image.
     * @return A bitmap that fits the maximum dimensions.
     */
    private Bitmap scale(int maxDim) {
        Bitmap b = mOriginal;
        int width = mOriginal.getWidth();
        int height = mOriginal.getHeight();
        float scaleFactor = ((float) maxDim) / Math.max(width, height);
        if (scaleFactor < 1.0) {
            // Need to scale down the photo.
            Matrix matrix = new Matrix();
            matrix.setScale(scaleFactor, scaleFactor);
            b = Bitmap.createBitmap(mOriginal, 0, 0, width, height, matrix, false);
        }
        return b;
    }

    /**
     * Helper method to compress the given bitmap as a JPEG and return the resulting byte array.
     */
    private byte[] getCompressedBytes(Bitmap b) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        boolean compressed = b.compress(Bitmap.CompressFormat.JPEG, 95, baos);
        if (!compressed) {
            throw new IOException("Unable to compress image");
        }
        baos.flush();
        baos.close();
        return baos.toByteArray();
    }

    /**
     * Retrieves the uncompressed display photo.
     */
    public Bitmap getDisplayPhoto() {
        return mDisplayPhoto;
    }

    /**
     * Retrieves the uncompressed thumbnail photo.
     */
    public Bitmap getThumbnailPhoto() {
        return mThumbnailPhoto;
    }

    /**
     * Retrieves the compressed display photo as a byte array.
     */
    public byte[] getDisplayPhotoBytes() throws IOException {
        return getCompressedBytes(mDisplayPhoto);
    }

    /**
     * Retrieves the compressed thumbnail photo as a byte array.
     */
    public byte[] getThumbnailPhotoBytes() throws IOException {
        return getCompressedBytes(mThumbnailPhoto);
    }

    /**
     * Retrieves the maximum width or height (in pixels) of the display photo.
     */
    public int getMaxDisplayPhotoDim() {
        return mMaxDisplayPhotoDim;
    }

    /**
     * Retrieves the maximum width or height (in pixels) of the thumbnail.
     */
    public int getMaxThumbnailPhotoDim() {
        return mMaxThumbnailPhotoDim;
    }
}
