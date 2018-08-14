package com.android.providers.contacts;

import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.pm.ActivityInfo;
import android.content.pm.ResolveInfo;
import android.net.Uri;
import android.os.Binder;
import android.provider.VoicemailContract;
import android.util.ArraySet;
import android.util.Log;

import com.google.android.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Aggregates voicemail broadcasts from multiple operations in to a single one. The URIs will be
 * {@link VoicemailContract.Voicemails#DIR_TYPE} instead of {@link
 * VoicemailContract.Voicemails#ITEM_TYPE} if multiple URIs is notified.
 */
public class VoicemailNotifier {

    private final String TAG = "VoicemailNotifier";

    private final Context mContext;
    private final Uri mBaseUri;

    private final VoicemailPermissions mVoicemailPermissions;

    private final Set<String> mIntentActions = new ArraySet<>();
    private final Set<String> mModifiedPackages = new ArraySet<>();
    private final Set<Uri> mUris = new ArraySet<>();

    public VoicemailNotifier(Context context, Uri baseUri) {
        mContext = context;
        mBaseUri = baseUri;
        mVoicemailPermissions = new VoicemailPermissions(mContext);
    }

    public void addIntentActions(String action) {
        mIntentActions.add(action);
    }

    public void addModifiedPackages(Collection<String> packages) {
        mModifiedPackages.addAll(packages);
    }

    public void addUri(Uri uri) {
        mUris.add(uri);
    }

    public void sendNotification() {
        Uri uri = mUris.size() == 1 ? mUris.iterator().next() : mBaseUri;
        mContext.getContentResolver().notifyChange(uri, null, true);
        Collection<String> callingPackages = getCallingPackages();
        // Now fire individual intents.
        for (String intentAction : mIntentActions) {
            // self_change extra should be included only for provider_changed events.
            boolean includeSelfChangeExtra = intentAction.equals(Intent.ACTION_PROVIDER_CHANGED);
            Log.i(TAG, "receivers for " + intentAction + " :" + getBroadcastReceiverComponents(
                    intentAction, uri));
            for (ComponentName component :
                    getBroadcastReceiverComponents(intentAction, uri)) {
                boolean hasFullReadAccess =
                        mVoicemailPermissions.packageHasReadAccess(component.getPackageName());
                boolean hasOwnAccess =
                        mVoicemailPermissions.packageHasOwnVoicemailAccess(
                                component.getPackageName());
                // If we don't have full access, ignore the broadcast if the package isn't affected
                // by the change or doesn't have access to its own messages.
                if (!hasFullReadAccess
                        && (!mModifiedPackages.contains(component.getPackageName())
                                || !hasOwnAccess)) {
                    continue;
                }

                Intent intent = new Intent(intentAction, uri);
                intent.setComponent(component);
                if (includeSelfChangeExtra && callingPackages != null) {
                    intent.putExtra(VoicemailContract.EXTRA_SELF_CHANGE,
                            callingPackages.contains(component.getPackageName()));
                }
                mContext.sendBroadcast(intent);
                Log.v(TAG, String.format("Sent intent. act:%s, url:%s, comp:%s," +
                                " self_change:%s", intent.getAction(), intent.getData(),
                        component.getClassName(),
                        intent.hasExtra(VoicemailContract.EXTRA_SELF_CHANGE) ?
                                intent.getBooleanExtra(VoicemailContract.EXTRA_SELF_CHANGE, false) :
                                null));
            }
        }
        mIntentActions.clear();
        mModifiedPackages.clear();
        mUris.clear();
    }

    /**
     * Returns the package names of the calling process. If the calling process has more than
     * one packages, this returns them all
     */
    private Collection<String> getCallingPackages() {
        int caller = Binder.getCallingUid();
        if (caller == 0) {
            return null;
        }
        return Lists.newArrayList(mContext.getPackageManager().getPackagesForUid(caller));
    }

    /**
     * Determines the components that can possibly receive the specified intent.
     */
    private List<ComponentName> getBroadcastReceiverComponents(String intentAction, Uri uri) {
        Intent intent = new Intent(intentAction, uri);
        List<ComponentName> receiverComponents = new ArrayList<ComponentName>();
        // For broadcast receivers ResolveInfo.activityInfo is the one that is populated.
        for (ResolveInfo resolveInfo :
                mContext.getPackageManager().queryBroadcastReceivers(intent, 0)) {
            ActivityInfo activityInfo = resolveInfo.activityInfo;
            receiverComponents.add(new ComponentName(activityInfo.packageName, activityInfo.name));
        }
        return receiverComponents;
    }
}
