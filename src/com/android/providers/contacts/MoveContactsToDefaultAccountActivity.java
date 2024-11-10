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

import static android.Manifest.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS;
import static android.Manifest.permission.WRITE_CONTACTS;
import static android.provider.ContactsContract.RawContacts.DefaultAccount;
import static android.provider.Flags.newDefaultAccountApiEnabled;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.AuthenticatorDescription;
import android.annotation.RequiresPermission;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.pm.PackageManager;
import android.content.res.Resources;
import android.icu.text.MessageFormat;
import android.os.Bundle;
import android.os.UserHandle;
import android.provider.ContactsContract.RawContacts.DefaultAccount.DefaultAccountAndState;
import android.util.Log;

import com.android.internal.annotations.VisibleForTesting;
import com.android.providers.contacts.util.ContactsPermissions;
import com.android.providers.contacts.util.NeededForTesting;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class MoveContactsToDefaultAccountActivity extends Activity {
    @VisibleForTesting
    static final String MOVABLE_CONTACTS_MESSAGE_KEY = "contacts_count";
    private static final String TAG = "MoveContactsToDefaultAccountActivity";
    private Map<String, AuthenticatorDescription> mTypeToAuthDescription;

    private UserHandle mUserHandle;

    private int movableLocalContactsCount;

    private int movableSimContactsCount;

    @RequiresPermission(android.Manifest.permission.SET_DEFAULT_ACCOUNT_FOR_CONTACTS)
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (!newDefaultAccountApiEnabled()) {
            Log.w(TAG, "Default Account API flag not enabled, bailing out.");
            setResultAndFinish(RESULT_CANCELED);
            return;
        }
        try {
            DefaultAccountAndState currentDefaultAccount =
                    DefaultAccount.getDefaultAccountForNewContacts(getContentResolver());
            if (currentDefaultAccount.getState()
                    == DefaultAccountAndState.DEFAULT_ACCOUNT_STATE_CLOUD) {
                mTypeToAuthDescription = new HashMap<>();
                mUserHandle = new UserHandle(UserHandle.myUserId());
                movableLocalContactsCount = DefaultAccount.getNumberOfMovableLocalContacts(
                        getContentResolver());
                movableSimContactsCount = DefaultAccount.getNumberOfMovableSimContacts(
                        getContentResolver());
                if (movableLocalContactsCount + movableSimContactsCount <= 0) {
                    Log.i(TAG, "There's no movable contacts.");
                    setResultAndFinish(RESULT_CANCELED);
                    return;
                } else if (!checkPermission(this)) {
                    Log.e(TAG, "There's no contacts permission.");
                    setResultAndFinish(RESULT_CANCELED);
                    return;
                }
                showMoveContactsToDefaultAccountDialog(this, currentDefaultAccount);
            } else {
                Log.w(TAG, "Account is not cloud account, not eligible for moving local contacts.");
                setResultAndFinish(RESULT_CANCELED);
            }
        } catch (IllegalStateException e) {
            Log.e(TAG, "The default account is in an invalid state: " + e);
            setResultAndFinish(RESULT_CANCELED);
        } catch (RuntimeException e) {
            Log.e(TAG, "Failed to look up the default account: " + e);
            setResultAndFinish(RESULT_CANCELED);
        }
    }

    private void showMoveContactsToDefaultAccountDialog(Context context,
            DefaultAccountAndState currentDefaultAccount) {
        Account account = currentDefaultAccount.getAccount();
        if (account == null) {
            Log.e(TAG, "The default account is null.");
            setResultAndFinish(RESULT_CANCELED);
            return;
        }
        String accountLabel = (String) getLabelForType(context, account.type);
        if (accountLabel == null) {
            Log.e(TAG, "Cannot get account label.");
            setResultAndFinish(RESULT_CANCELED);
            return;
        }
        AlertDialog.Builder builder = new AlertDialog.Builder(context);
        int totalMovableContactsCount = movableSimContactsCount + movableLocalContactsCount;
        builder.setTitle(getTitleText()).setMessage(
                        getMessageText(totalMovableContactsCount, accountLabel, account.name))
                .setPositiveButton(getSyncButtonText(), (dialog, which) -> {
                    try {
                        DefaultAccount.moveLocalContactsToCloudDefaultAccount(getContentResolver());
                        DefaultAccount.moveSimContactsToCloudDefaultAccount(getContentResolver());
                        Log.i(TAG,
                                "Successfully moved all local and sim contacts to cloud account.");
                        setResultAndFinish(RESULT_OK);
                    } catch (RuntimeException e) {
                        Log.e(TAG, "Failed to move contacts to cloud account.");
                        setResultAndFinish(RESULT_CANCELED);
                    }
                })
                .setNegativeButton(getCancelButtonText(),
                        (dialog, choice) -> setResultAndFinish(RESULT_CANCELED))
                .setOnDismissListener(dialog -> {
                    dialog.dismiss();
                    finish();
                });
        AlertDialog dialog = builder.create();
        dialog.show();
    }

    private void setResultAndFinish(int resultCode) {
        setResult(resultCode);
        finish();
    }

    private boolean checkPermission(Context context) {
        ContactsPermissions.enforceCallingOrSelfPermission(context, WRITE_CONTACTS);
        ContactsPermissions.enforceCallingOrSelfPermission(context,
                SET_DEFAULT_ACCOUNT_FOR_CONTACTS);
        return true;
    }

    @NeededForTesting
    String getMessageText(int movableContactsCount, String accountLabel,
            String accountName) {
        MessageFormat msgFormat = new MessageFormat(
                getString(R.string.movable_contacts_count),
                Locale.getDefault());
        Map<String, Object> msgArgs = new HashMap<>();
        msgArgs.put(MOVABLE_CONTACTS_MESSAGE_KEY, movableContactsCount);
        String movableContactsCountText = msgFormat.format(msgArgs);
        return getString(R.string.move_contacts_to_default_account_dialog_message,
                movableContactsCountText, accountLabel, accountName);
    }

    @NeededForTesting
    String getTitleText() {
        return getString(R.string.move_contacts_to_default_account_dialog_title);
    }

    @NeededForTesting
    String getSyncButtonText() {
        return getString(R.string.move_contacts_to_default_account_dialog_sync_button_text);
    }

    @NeededForTesting
    String getCancelButtonText() {
        return getString(R.string.move_contacts_to_default_account_dialog_cancel_button_text);
    }

    /**
     * Gets the label associated with a particular account type. If none found, return null.
     *
     * @param accountType the type of account
     * @return a CharSequence for the label or null if one cannot be found.
     */
    @NeededForTesting
    public CharSequence getLabelForType(Context context, final String accountType) {
        AuthenticatorDescription[] authDescs = AccountManager.get(context)
                .getAuthenticatorTypesAsUser(mUserHandle.getIdentifier());
        for (AuthenticatorDescription authDesc : authDescs) {
            mTypeToAuthDescription.put(authDesc.type, authDesc);
        }
        CharSequence label = null;
        if (mTypeToAuthDescription.containsKey(accountType)) {
            try {
                AuthenticatorDescription desc = mTypeToAuthDescription.get(accountType);
                Context authContext = context.createPackageContextAsUser(desc.packageName, 0,
                        mUserHandle);
                label = authContext.getResources().getText(desc.labelId);
            } catch (PackageManager.NameNotFoundException e) {
                Log.w(TAG, "No label name for account type " + accountType);
            } catch (Resources.NotFoundException e) {
                Log.w(TAG, "No label icon for account type " + accountType);
            }
        }
        return label;
    }
}
