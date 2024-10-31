/*
 * Copyright (C) 2024 The Android Open Source Project
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

package com.android.providers.contacts;

import static android.app.Activity.RESULT_CANCELED;
import static android.app.Activity.RESULT_OK;
import static android.provider.ContactsContract.RawContacts.DefaultAccount;

import static androidx.test.platform.app.InstrumentationRegistry.getInstrumentation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

import android.accounts.Account;
import android.app.Instrumentation;
import android.content.ContentResolver;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.icu.text.MessageFormat;
import android.net.Uri;
import android.os.Bundle;
import android.platform.test.annotations.EnableFlags;
import android.platform.test.annotations.RequiresFlagsDisabled;
import android.platform.test.flag.junit.CheckFlagsRule;
import android.platform.test.flag.junit.DeviceFlagsValueProvider;
import android.platform.test.flag.junit.SetFlagsRule;
import android.provider.ContactsContract;
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;
import android.provider.ContactsContract.Settings;
import android.provider.Flags;
import android.test.mock.MockContentProvider;
import android.test.mock.MockContentResolver;

import androidx.test.InstrumentationRegistry;
import androidx.test.ext.junit.runners.AndroidJUnit4;
import androidx.test.uiautomator.By;
import androidx.test.uiautomator.BySelector;
import androidx.test.uiautomator.UiDevice;
import androidx.test.uiautomator.Until;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@RunWith(AndroidJUnit4.class)
public class MoveContactsToDefaultAccountActivityTest {

    private static final String TEST_MOVE_LOCAL_CONTACTS_METHOD = "test_move_local_contacts";

    private static final String TEST_MOVE_LOCAL_CONTACTS_BUNDLE_KEY = "move_local_contacts_key";

    private static final String TEST_MOVE_SIM_CONTACTS_BUNDLE_KEY = "move_sim_contacts_key";

    private static final Account TEST_ACCOUNT = new Account("test@gmail.com", "Google");

    @Rule
    public final MockitoRule mockito = MockitoJUnit.rule();

    @Rule
    public final CheckFlagsRule mCheckFlagsRule = DeviceFlagsValueProvider.createCheckFlagsRule();

    @Rule
    public final SetFlagsRule mSetFlagsRule = new SetFlagsRule();
    private final MockContentResolver mContentResolver = new MockContentResolver();
    private final Instrumentation mInstrumentation = getInstrumentation();
    @Spy
    private TestMoveContactsToDefaultAccountActivity mActivity;
    private UiDevice mDevice;

    @Before
    public void setUp() throws Exception {
        mInstrumentation.getUiAutomation()
                .adoptShellPermissionIdentity(
                        "android.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS");
        mDevice = UiDevice.getInstance(mInstrumentation);
        mContentResolver.addProvider("settings", new MockContentProvider() {
            @Override
            public Bundle call(String method, String arg, Bundle extras) {
                return null;
            }

            @Override
            public Cursor query(
                    Uri uri,
                    String[] projection,
                    String selection,
                    String[] selectionArgs,
                    String sortOrder) {
                return null;
            }
        });
    }

    @Test
    @EnableFlags(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testMoveLocalContactsDialog_eligibleAccount_clickConfirm() throws Exception {
        MockContentProvider mockContentProvider = setupMockContentProvider(
                DefaultAccountAndState.ofCloud(TEST_ACCOUNT), 1);
        mContentResolver.addProvider(ContactsContract.AUTHORITY_URI.getAuthority(),
                mockContentProvider);
        TestMoveContactsToDefaultAccountActivity.setupForTesting(mContentResolver);

        Intent intent = new Intent(InstrumentationRegistry.getContext(),
                TestMoveContactsToDefaultAccountActivity.class);
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        mActivity = spy(
                (TestMoveContactsToDefaultAccountActivity) mInstrumentation.startActivitySync(
                        intent));
        mDevice.waitForIdle();

        assertTrue(mDevice.hasObject(By.text(mActivity.getTitleText())));
        assertTrue(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getSyncButtonText())));
        assertTrue(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getCancelButtonText())));
        assertTrue(mDevice.hasObject(
                By.text(mActivity.getMessageText(2, "Google", "test@gmail.com"))));

        mDevice.findObject(getCaseInsensitiveSelector(mActivity.getSyncButtonText())).click();
        // Wait for action to be performed.
        Thread.sleep(1000);

        assertTrue(mContentResolver.call(ContactsContract.AUTHORITY_URI,
                TEST_MOVE_LOCAL_CONTACTS_METHOD, null, null).getBoolean(
                TEST_MOVE_LOCAL_CONTACTS_BUNDLE_KEY));
        assertTrue(mContentResolver.call(ContactsContract.AUTHORITY_URI,
                TEST_MOVE_LOCAL_CONTACTS_METHOD, null, null).getBoolean(
                TEST_MOVE_SIM_CONTACTS_BUNDLE_KEY));
        verify(mActivity).setResult(eq(RESULT_OK));
    }

    @Test
    @EnableFlags(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testMoveLocalContactsDialog_eligibleAccount_clickCancel() {
        MockContentProvider mockContentProvider = setupMockContentProvider(
                DefaultAccountAndState.ofCloud(TEST_ACCOUNT), 1);
        mContentResolver.addProvider(ContactsContract.AUTHORITY_URI.getAuthority(),
                mockContentProvider);
        TestMoveContactsToDefaultAccountActivity.setupForTesting(mContentResolver);

        Intent intent = new Intent(InstrumentationRegistry.getContext(),
                TestMoveContactsToDefaultAccountActivity.class);
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        mActivity = spy(
                (TestMoveContactsToDefaultAccountActivity) mInstrumentation.startActivitySync(
                        intent));
        mDevice.waitForIdle();

        assertTrue(mDevice.hasObject(By.text(mActivity.getTitleText())));
        assertTrue(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getSyncButtonText())));
        assertTrue(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getCancelButtonText())));
        assertTrue(mDevice.hasObject(
                By.text(mActivity.getMessageText(2, "Google", "test@gmail.com"))));

        mDevice.findObject(getCaseInsensitiveSelector(mActivity.getCancelButtonText())).click();

        assertFalse(mContentResolver.call(ContactsContract.AUTHORITY_URI,
                TEST_MOVE_LOCAL_CONTACTS_METHOD, null, null).getBoolean(
                TEST_MOVE_LOCAL_CONTACTS_BUNDLE_KEY));
        assertFalse(mContentResolver.call(ContactsContract.AUTHORITY_URI,
                TEST_MOVE_LOCAL_CONTACTS_METHOD, null, null).getBoolean(
                TEST_MOVE_SIM_CONTACTS_BUNDLE_KEY));
        verify(mActivity).setResult(eq(RESULT_CANCELED));
    }

    @Test
    @EnableFlags(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testMoveLocalContactsDialog_noDefaultAccount_dontShowDialog() {
        MockContentProvider mockContentProvider = setupMockContentProvider(
                DefaultAccountAndState.ofNotSet(), 1);
        mContentResolver.addProvider(ContactsContract.AUTHORITY_URI.getAuthority(),
                mockContentProvider);
        TestMoveContactsToDefaultAccountActivity.setupForTesting(mContentResolver);

        Intent intent = new Intent(InstrumentationRegistry.getContext(),
                TestMoveContactsToDefaultAccountActivity.class);
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);

        mActivity = spy(
                (TestMoveContactsToDefaultAccountActivity) mInstrumentation.startActivitySync(
                        intent));
        mDevice.waitForIdle();

        assertFalse(mDevice.hasObject(By.text(mActivity.getTitleText())));
        assertFalse(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getSyncButtonText())));
        assertFalse(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getCancelButtonText())));

        verify(mActivity).setResult(eq(RESULT_CANCELED));
    }

    @Test
    @EnableFlags(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testMoveLocalContactsDialog_noCloudAccount_dontShowDialog() {
        MockContentProvider mockContentProvider = setupMockContentProvider(
                DefaultAccountAndState.ofLocal(), 1);
        mContentResolver.addProvider(ContactsContract.AUTHORITY_URI.getAuthority(),
                mockContentProvider);
        TestMoveContactsToDefaultAccountActivity.setupForTesting(mContentResolver);

        Intent intent = new Intent(InstrumentationRegistry.getContext(),
                TestMoveContactsToDefaultAccountActivity.class);
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        mActivity = spy(
                (TestMoveContactsToDefaultAccountActivity) mInstrumentation.startActivitySync(
                        intent));
        mDevice.waitForIdle();

        assertFalse(mDevice.hasObject(By.text(mActivity.getTitleText())));
        assertFalse(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getSyncButtonText())));
        assertFalse(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getCancelButtonText())));

        verify(mActivity).setResult(eq(RESULT_CANCELED));
    }

    @Test
    @RequiresFlagsDisabled(Flags.FLAG_NEW_DEFAULT_ACCOUNT_API_ENABLED)
    public void testMoveLocalContactsDialog_flagOff_dontShowDialog() {
        MockContentProvider mockContentProvider = setupMockContentProvider(
                DefaultAccountAndState.ofCloud(TEST_ACCOUNT), 1);
        mContentResolver.addProvider(ContactsContract.AUTHORITY_URI.getAuthority(),
                mockContentProvider);
        TestMoveContactsToDefaultAccountActivity.setupForTesting(mContentResolver);

        Intent intent = new Intent(InstrumentationRegistry.getContext(),
                TestMoveContactsToDefaultAccountActivity.class);
        intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        mActivity = spy(
                (TestMoveContactsToDefaultAccountActivity) mInstrumentation.startActivitySync(
                        intent));
        mDevice.wait(Until.hasObject(By.text(mActivity.getTitleText())), /*timeout=*/2000L);

        assertFalse(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getSyncButtonText())));
        assertFalse(mDevice.hasObject(getCaseInsensitiveSelector(mActivity.getCancelButtonText())));

        verify(mActivity).setResult(eq(RESULT_CANCELED));
    }

    private BySelector getCaseInsensitiveSelector(String text) {
        return By.text(Pattern.compile(text, CASE_INSENSITIVE));
    }

    private MockContentProvider setupMockContentProvider(
            DefaultAccountAndState defaultAccountAndState, int movableContactsCount) {
        MockContentProvider mockContentProvider = new MockContentProvider() {
            final Bundle moveLocalContactsBundle = new Bundle();

            @Override
            public Bundle call(String method, String arg, Bundle extras) {
                Bundle bundle = new Bundle();
                if (DefaultAccount.QUERY_DEFAULT_ACCOUNT_FOR_NEW_CONTACTS_METHOD.equals(method)) {
                    bundle.putInt(DefaultAccount.KEY_DEFAULT_ACCOUNT_STATE,
                            defaultAccountAndState.getState());
                    Account defaultAccount = defaultAccountAndState.getAccount();
                    if (defaultAccount != null) {
                        bundle.putString(Settings.ACCOUNT_NAME, defaultAccount.name);
                        bundle.putString(Settings.ACCOUNT_TYPE, defaultAccount.type);
                    }
                    return bundle;
                } else if (
                        DefaultAccount.MOVE_LOCAL_CONTACTS_TO_CLOUD_DEFAULT_ACCOUNT_METHOD.equals(
                                method)) {
                    moveLocalContactsBundle.putBoolean(TEST_MOVE_LOCAL_CONTACTS_BUNDLE_KEY, true);
                    return moveLocalContactsBundle;
                } else if (DefaultAccount.MOVE_SIM_CONTACTS_TO_CLOUD_DEFAULT_ACCOUNT_METHOD.equals(
                        method)) {
                    moveLocalContactsBundle.putBoolean(TEST_MOVE_SIM_CONTACTS_BUNDLE_KEY, true);
                    return moveLocalContactsBundle;
                } else if (DefaultAccount.GET_NUMBER_OF_MOVABLE_LOCAL_CONTACTS_METHOD.equals(
                        method)) {
                    bundle.putInt(DefaultAccount.KEY_NUMBER_OF_MOVABLE_LOCAL_CONTACTS,
                            movableContactsCount);
                    return bundle;
                } else if (DefaultAccount.GET_NUMBER_OF_MOVABLE_SIM_CONTACTS_METHOD.equals(
                        method)) {
                    bundle.putInt(DefaultAccount.KEY_NUMBER_OF_MOVABLE_SIM_CONTACTS,
                            movableContactsCount);
                    return bundle;
                } else if (TEST_MOVE_LOCAL_CONTACTS_METHOD.equals(method)) {
                    // Created this action to verify the move local contacts call.
                    return moveLocalContactsBundle;
                }
                return bundle;
            }
        };
        return mockContentProvider;
    }

    public static class TestMoveContactsToDefaultAccountActivity extends
            MoveContactsToDefaultAccountActivity {
        private static ContentResolver mContentResolver;

        public static void setupForTesting(
                ContentResolver contentResolver) {
            mContentResolver = contentResolver;
        }

        @Override
        public ContentResolver getContentResolver() {
            return mContentResolver;
        }

        @Override
        public String getMessageText(int movableContactsCount, String accountLabel,
                String accountName) {
            MessageFormat msgFormat = new MessageFormat(
                    InstrumentationRegistry.getTargetContext().getString(
                            R.string.movable_contacts_count),
                    Locale.getDefault());
            Map<String, Object> msgArgs = new HashMap<>();
            msgArgs.put("contacts_count", movableContactsCount);
            String movableContactsCountText = msgFormat.format(msgArgs);
            return InstrumentationRegistry.getTargetContext().getString(
                    R.string.move_contacts_to_default_account_dialog_message,
                    movableContactsCountText, accountLabel, accountName);
        }

        @Override
        String getTitleText() {
            return InstrumentationRegistry.getTargetContext().getString(
                    R.string.move_contacts_to_default_account_dialog_title);
        }

        @Override
        String getSyncButtonText() {
            return InstrumentationRegistry.getTargetContext().getString(
                    R.string.move_contacts_to_default_account_dialog_sync_button_text);
        }

        @Override
        String getCancelButtonText() {
            return InstrumentationRegistry.getTargetContext().getString(
                    R.string.move_contacts_to_default_account_dialog_cancel_button_text);
        }

        @Override
        public CharSequence getLabelForType(Context context, final String accountType) {
            return accountType;
        }
    }
}
