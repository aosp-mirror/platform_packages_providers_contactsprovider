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
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.graphics.Rect;
import android.graphics.RectF;
import android.sysprop.ContactsProperties;

import com.android.providers.contacts.util.MemoryUtils;
import com.google.common.annotations.VisibleForTesting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Class that converts a bitmap (or byte array representing a bitmap) into a display
 * photo and a thumbnail photo.
 */
/* package-protected */ final class PhotoProcessor {

    /** Compression for display photos. They are very big, so we can use a strong compression */
    private static final int COMPRESSION_DISPLAY_PHOTO = 75;

    /**
     * Compression for thumbnails that don't have a full size photo. Those can be blown up
     * full-screen, so we want to make sure we don't introduce JPEG artifacts here
     */
    private static final int COMPRESSION_THUMBNAIL_HIGH = 95;

    /** Compression for thumbnails that also have a display photo */
    private static final int COMPRESSION_THUMBNAIL_LOW = 90;

    private static final Paint WHITE_PAINT = new Paint();

    static {
        WHITE_PAINT.setColor(Color.WHITE);
    }

    private static int sMaxThumbnailDim;
    private static int sMaxDisplayPhotoDim;

    static {
        final boolean isExpensiveDevice =
                MemoryUtils.getTotalMemorySize() >= PhotoSizes.LARGE_RAM_THRESHOLD;

        sMaxThumbnailDim = ContactsProperties.thumbnail_size().orElse(
                PhotoSizes.DEFAULT_THUMBNAIL);

        sMaxDisplayPhotoDim = ContactsProperties.display_photo_size().orElse(
                isExpensiveDevice
                        ? PhotoSizes.DEFAULT_DISPLAY_PHOTO_LARGE_MEMORY
                        : PhotoSizes.DEFAULT_DISPLAY_PHOTO_MEMORY_CONSTRAINED);
    }

    /**
     * The default sizes of a thumbnail/display picture. This is used in {@link #initialize()}
     */
    private interface PhotoSizes {
        /** Size of a thumbnail */
        public static final int DEFAULT_THUMBNAIL = 96;

        /**
         * Size of a display photo on memory constrained devices (those are devices with less than
         * {@link #DEFAULT_LARGE_RAM_THRESHOLD} of reported RAM
         */
        public static final int DEFAULT_DISPLAY_PHOTO_MEMORY_CONSTRAINED = 480;

        /**
         * Size of a display photo on devices with enough ram (those are devices with at least
         * {@link #DEFAULT_LARGE_RAM_THRESHOLD} of reported RAM
         */
        public static final int DEFAULT_DISPLAY_PHOTO_LARGE_MEMORY = 720;

        /**
         * If the device has less than this amount of RAM, it is considered RAM constrained for
         * photos
         */
        public static final int LARGE_RAM_THRESHOLD = 640 * 1024 * 1024;
    }

    private final int mMaxDisplayPhotoDim;
    private final int mMaxThumbnailPhotoDim;
    private final boolean mForceCropToSquare;
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
        this(original, maxDisplayPhotoDim, maxThumbnailPhotoDim, false);
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
                maxDisplayPhotoDim, maxThumbnailPhotoDim, false);
    }

    /**
     * Initializes a photo processor for the given bitmap.
     * @param original The bitmap to process.
     * @param maxDisplayPhotoDim The maximum height and width for the display photo.
     * @param maxThumbnailPhotoDim The maximum height and width for the thumbnail photo.
     * @param forceCropToSquare Whether to force the processed images to be square.  If the source
     *     photo is not square, this will crop to the square at the center of the image's rectangle.
     *     If this is not set to true, the image will simply be downscaled to fit in the given
     *     dimensions, retaining its original aspect ratio.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    public PhotoProcessor(Bitmap original, int maxDisplayPhotoDim, int maxThumbnailPhotoDim,
            boolean forceCropToSquare) throws IOException {
        mOriginal = original;
        mMaxDisplayPhotoDim = maxDisplayPhotoDim;
        mMaxThumbnailPhotoDim = maxThumbnailPhotoDim;
        mForceCropToSquare = forceCropToSquare;
        process();
    }

    /**
     * Initializes a photo processor for the given bitmap.
     * @param originalBytes A byte array to decode into a bitmap to process.
     * @param maxDisplayPhotoDim The maximum height and width for the display photo.
     * @param maxThumbnailPhotoDim The maximum height and width for the thumbnail photo.
     * @param forceCropToSquare Whether to force the processed images to be square.  If the source
     *     photo is not square, this will crop to the square at the center of the image's rectangle.
     *     If this is not set to true, the image will simply be downscaled to fit in the given
     *     dimensions, retaining its original aspect ratio.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    public PhotoProcessor(byte[] originalBytes, int maxDisplayPhotoDim, int maxThumbnailPhotoDim,
            boolean forceCropToSquare) throws IOException {
        this(BitmapFactory.decodeByteArray(originalBytes, 0, originalBytes.length),
                maxDisplayPhotoDim, maxThumbnailPhotoDim, forceCropToSquare);
    }

    /**
     * Processes the original image, producing a scaled-down display photo and thumbnail photo.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    private void process() throws IOException {
        if (mOriginal == null) {
            throw new IOException("Invalid image file");
        }
        mDisplayPhoto = getNormalizedBitmap(mOriginal, mMaxDisplayPhotoDim, mForceCropToSquare);
        mThumbnailPhoto = getNormalizedBitmap(mOriginal,mMaxThumbnailPhotoDim, mForceCropToSquare);
    }

    /**
     * Scales down the original bitmap to fit within the given maximum width and height.
     * If the bitmap already fits in those dimensions, the original bitmap will be
     * returned unmodified unless the photo processor is set up to crop it to a square.
     *
     * Also, if the image has transparency, conevrt it to white.
     *
     * @param original Original bitmap
     * @param maxDim Maximum width and height (in pixels) for the image.
     * @param forceCropToSquare See {@link #PhotoProcessor(Bitmap, int, int, boolean)}
     * @return A bitmap that fits the maximum dimensions.
     * @throws IOException If bitmap decoding or scaling fails.
     */
    @SuppressWarnings({"SuspiciousNameCombination"})
    @VisibleForTesting
    static Bitmap getNormalizedBitmap(Bitmap original, int maxDim, boolean forceCropToSquare)
            throws IOException {
        final boolean originalHasAlpha = original.hasAlpha();

        // All cropXxx's are in the original coordinate.
        int cropWidth = original.getWidth();
        int cropHeight = original.getHeight();
        int cropLeft = 0;
        int cropTop = 0;
        if (forceCropToSquare && cropWidth != cropHeight) {
            // Crop the image to the square at its center.
            if (cropHeight > cropWidth) {
                cropTop = (cropHeight - cropWidth) / 2;
                cropHeight = cropWidth;
            } else {
                cropLeft = (cropWidth - cropHeight) / 2;
                cropWidth = cropHeight;
            }
        }
        // Calculate the scale factor.  We don't want to scale up, so the max scale is 1f.
        final float scaleFactor = Math.min(1f, ((float) maxDim) / Math.max(cropWidth, cropHeight));

        if (scaleFactor < 1.0f || cropLeft != 0 || cropTop != 0 || originalHasAlpha) {
            final int newWidth = (int) (cropWidth * scaleFactor);
            final int newHeight = (int) (cropHeight * scaleFactor);
            if (newWidth <= 0 || newHeight <= 0) {
                throw new IOException("Invalid bitmap dimensions");
            }
            final Bitmap scaledBitmap = Bitmap.createBitmap(newWidth, newHeight,
                    Bitmap.Config.ARGB_8888);
            final Canvas c = new Canvas(scaledBitmap);

            if (originalHasAlpha) {
                c.drawRect(0, 0, scaledBitmap.getWidth(), scaledBitmap.getHeight(), WHITE_PAINT);
            }

            final Rect src = new Rect(cropLeft, cropTop,
                    cropLeft + cropWidth, cropTop + cropHeight);
            final RectF dst = new RectF(0, 0, scaledBitmap.getWidth(), scaledBitmap.getHeight());

            c.drawBitmap(original, src, dst, null);
            return scaledBitmap;
        } else {
            return original;
        }
    }

    /**
     * Helper method to compress the given bitmap as a JPEG and return the resulting byte array.
     */
    private byte[] getCompressedBytes(Bitmap b, int quality) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final boolean compressed = b.compress(Bitmap.CompressFormat.JPEG, quality, baos);
        baos.flush();
        baos.close();
        byte[] result = baos.toByteArray();

        if (!compressed) {
            throw new IOException("Unable to compress image");
        }
        return result;
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
        return getCompressedBytes(mDisplayPhoto, COMPRESSION_DISPLAY_PHOTO);
    }

    /**
     * Retrieves the compressed thumbnail photo as a byte array.
     */
    public byte[] getThumbnailPhotoBytes() throws IOException {
        // If there is a higher-resolution picture, we can assume we won't need to upscale the
        // thumbnail often, so we can compress stronger
        final boolean hasDisplayPhoto = mDisplayPhoto != null &&
                (mDisplayPhoto.getWidth() > mThumbnailPhoto.getWidth() ||
                mDisplayPhoto.getHeight() > mThumbnailPhoto.getHeight());
        return getCompressedBytes(mThumbnailPhoto,
                hasDisplayPhoto ? COMPRESSION_THUMBNAIL_LOW : COMPRESSION_THUMBNAIL_HIGH);
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

    /**
     * Returns the maximum size in pixel of a thumbnail (which has a default that can be overriden
     * using a system-property)
     */
    public static int getMaxThumbnailSize() {
        return sMaxThumbnailDim;
    }

    /**
     * Returns the maximum size in pixel of a display photo (which is determined based
     * on available RAM or configured using a system-property)
     */
    public static int getMaxDisplayPhotoSize() {
        return sMaxDisplayPhotoDim;
    }
}
Hol da aus dem Kontakt blos noch dein Todt! Das ist blamierung ! Wir sind wegen den Geisteskranken in größter Lebens Gefahr! 
