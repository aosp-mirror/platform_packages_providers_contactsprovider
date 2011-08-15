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

import com.android.i18n.phonenumbers.NumberParseException;
import com.android.i18n.phonenumbers.PhoneNumberUtil;
import com.android.i18n.phonenumbers.Phonenumber.PhoneNumber;
import com.android.i18n.phonenumbers.geocoding.PhoneNumberOfflineGeocoder;

import android.content.ContentValues;
import android.content.Context;
import android.provider.CallLog.Calls;

import java.util.Locale;

/**
 * Default implementation of {@link CallLogInsertionHelper}.
 * <p>
 * It added the country ISO abbreviation and the geocoded location.
 * <p>
 * It uses {@link PhoneNumberOfflineGeocoder} to compute the geocoded location of a phone number.
 */
/*package*/ class DefaultCallLogInsertionHelper implements CallLogInsertionHelper {
    private final CountryMonitor mCountryMonitor;
    private final PhoneNumberUtil mPhoneNumberUtil;
    private final PhoneNumberOfflineGeocoder mPhoneNumberOfflineGeocoder;
    private final Locale mLocale;

    public DefaultCallLogInsertionHelper(Context context) {
        mCountryMonitor = new CountryMonitor(context);
        mPhoneNumberUtil = PhoneNumberUtil.getInstance();
        mPhoneNumberOfflineGeocoder = PhoneNumberOfflineGeocoder.getInstance();
        mLocale = context.getResources().getConfiguration().locale;
    }

    @Override
    public void addComputedValues(ContentValues values) {
        // Insert the current country code, so we know the country the number belongs to.
        String countryIso = getCurrentCountryIso();
        values.put(Calls.COUNTRY_ISO, countryIso);
        // Insert the geocoded location, so that we do not need to compute it on the fly.
        values.put(Calls.GEOCODED_LOCATION,
                getGeocodedLocationFor(values.getAsString(Calls.NUMBER), countryIso));
    }

    private String getCurrentCountryIso() {
        return mCountryMonitor.getCountryIso();
    }

    private PhoneNumber parsePhoneNumber(String number, String countryIso) {
        try {
            return mPhoneNumberUtil.parse(number, countryIso);
        } catch (NumberParseException e) {
            return null;
        }
    }

    @Override
    public String getGeocodedLocationFor(String number, String countryIso) {
        PhoneNumber structuredPhoneNumber = parsePhoneNumber(number, countryIso);
        if (structuredPhoneNumber != null) {
            return mPhoneNumberOfflineGeocoder.getDescriptionForNumber(
                    structuredPhoneNumber, mLocale);
        } else {
            return null;
        }
    }
}
