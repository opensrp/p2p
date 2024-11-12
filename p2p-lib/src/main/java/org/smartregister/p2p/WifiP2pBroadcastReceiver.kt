/*
 * License text copyright (c) 2020 MariaDB Corporation Ab, All Rights Reserved.
 * “Business Source License” is a trademark of MariaDB Corporation Ab.
 *
 * Parameters
 *
 * Licensor:             Ona Systems, Inc.
 * Licensed Work:        android-p2p. The Licensed Work is (c) 2023 Ona Systems, Inc.
 * Additional Use Grant: You may make production use of the Licensed Work,
 *                       provided such use does not include offering the Licensed Work
 *                       to third parties on a hosted or embedded basis which is
 *                       competitive with Ona Systems' products.
 * Change Date:          Four years from the date the Licensed Work is published.
 * Change License:       MPL 2.0
 *
 * For information about alternative licensing arrangements for the Licensed Work,
 * please contact licensing@ona.io.
 *
 * Notice
 *
 * Business Source License 1.1
 *
 * Terms
 *
 * The Licensor hereby grants you the right to copy, modify, create derivative
 * works, redistribute, and make non-production use of the Licensed Work. The
 * Licensor may make an Additional Use Grant, above, permitting limited production use.
 *
 * Effective on the Change Date, or the fourth anniversary of the first publicly
 * available distribution of a specific version of the Licensed Work under this
 * License, whichever comes first, the Licensor hereby grants you rights under
 * the terms of the Change License, and the rights granted in the paragraph
 * above terminate.
 *
 * If your use of the Licensed Work does not comply with the requirements
 * currently in effect as described in this License, you must purchase a
 * commercial license from the Licensor, its affiliated entities, or authorized
 * resellers, or you must refrain from using the Licensed Work.
 *
 * All copies of the original and modified Licensed Work, and derivative works
 * of the Licensed Work, are subject to this License. This License applies
 * separately for each version of the Licensed Work and the Change Date may vary
 * for each version of the Licensed Work released by Licensor.
 *
 * You must conspicuously display this License on each original or modified copy
 * of the Licensed Work. If you receive the Licensed Work in original or
 * modified form from a third party, the terms and conditions set forth in this
 * License apply to your use of that work.
 *
 * Any use of the Licensed Work in violation of this License will automatically
 * terminate your rights under this License for the current and all other
 * versions of the Licensed Work.
 *
 * This License does not grant you any right in any trademark or logo of
 * Licensor or its affiliates (provided that you may use a trademark or logo of
 * Licensor as expressly required by this License).
 *
 * TO THE EXTENT PERMITTED BY APPLICABLE LAW, THE LICENSED WORK IS PROVIDED ON
 * AN “AS IS” BASIS. LICENSOR HEREBY DISCLAIMS ALL WARRANTIES AND CONDITIONS,
 * EXPRESS OR IMPLIED, INCLUDING (WITHOUT LIMITATION) WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NON-INFRINGEMENT, AND
 * TITLE.
 */
package org.smartregister.p2p

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Network
import android.net.NetworkInfo
import android.net.wifi.p2p.WifiP2pDevice
import android.net.wifi.p2p.WifiP2pGroup
import android.net.wifi.p2p.WifiP2pInfo
import android.net.wifi.p2p.WifiP2pManager
import androidx.core.app.ActivityCompat
import org.smartregister.p2p.search.contract.P2PManagerListener
import timber.log.Timber

class WifiP2pBroadcastReceiver(
  private val manager: WifiP2pManager,
  private val channel: WifiP2pManager.Channel,
  private val listener: P2PManagerListener,
  private val context: Context
) : BroadcastReceiver() {

  override fun onReceive(context: Context, intent: Intent) {
    when (intent.action) {
      WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION -> {
        Timber.e("Wifi p2p Connection changed action")

        val networkInfo: NetworkInfo? = intent.getParcelableExtra(WifiP2pManager.EXTRA_NETWORK_INFO) as? NetworkInfo
        val p2pGroupInfo = intent.getParcelableExtra(WifiP2pManager.EXTRA_WIFI_P2P_GROUP) as? WifiP2pGroup
        if (networkInfo?.isConnected == true){
          // We are connected with the other device, request connection
          // info to find group owner IP

          manager.requestConnectionInfo(channel) { wifiP2pInfo ->
            listener.onConnectionInfoAvailable(wifiP2pInfo, p2pGroupInfo)
          }
        }
      }
      WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION ->
        handleDiscoveryChanged(intent.getIntExtra(WifiP2pManager.EXTRA_DISCOVERY_STATE, -1))
      WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION -> handlePeersChanged()
      WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION ->
        handleStateChanged(intent.getIntExtra(WifiP2pManager.EXTRA_WIFI_STATE, -1))
      WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION -> {
        val device =
          intent.getParcelableExtra(WifiP2pManager.EXTRA_WIFI_P2P_DEVICE) as WifiP2pDevice?
        handleDeviceChanged(device = device)
      }
    }
  }

  /**
   * https://developer.android.com/reference/android/net/wifi/p2p/WifiP2pManager#WIFI_P2P_CONNECTION_CHANGED_ACTION
   */
  private fun handleConnectionChanged() {
    manager.requestConnectionInfo(channel, listener)
  }

  /**
   * https://developer.android.com/reference/android/net/wifi/p2p/WifiP2pManager#WIFI_P2P_DISCOVERY_CHANGED_ACTION
   */
  private fun handleDiscoveryChanged(discoveryState: Int) =
    when (discoveryState) {
      WifiP2pManager.WIFI_P2P_DISCOVERY_STARTED -> listener.handleP2pDiscoveryStarted()
      WifiP2pManager.WIFI_P2P_DISCOVERY_STOPPED -> listener.handleP2pDiscoveryStopped()
      else -> listener.handleUnexpectedWifiP2pDiscoveryState(discoveryState)
    }

  /**
   * https://developer.android.com/reference/android/net/wifi/p2p/WifiP2pManager#WIFI_P2P_PEERS_CHANGED_ACTION
   */
  private fun handlePeersChanged() {
    if (ActivityCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) !=
        PackageManager.PERMISSION_GRANTED
    ) {
      // TODO: Consider calling
      //    ActivityCompat#requestPermissions
      // here to request the missing permissions, and then overriding
      //   public void onRequestPermissionsResult(int requestCode, String[] permissions,
      //                                          int[] grantResults)
      // to handle the case where the user grants the permission. See the documentation
      // for ActivityCompat#requestPermissions for more details.
      listener.handleAccessFineLocationNotGranted()
      return
    }
    manager.requestPeers(channel) { listener.handleP2pPeersChanged(it) }
  }

  /**
   * https://developer.android.com/reference/android/net/wifi/p2p/WifiP2pManager#WIFI_P2P_STATE_CHANGED_ACTION
   */
  private fun handleStateChanged(wifiState: Int) =
    when (wifiState) {
      WifiP2pManager.WIFI_P2P_STATE_DISABLED -> listener.handleWifiP2pDisabled()
      WifiP2pManager.WIFI_P2P_STATE_ENABLED -> listener.handleWifiP2pEnabled()
      else -> listener.handleUnexpectedWifiP2pState(wifiState)
    }

  /**
   * https://developer.android.com/reference/android/net/wifi/p2p/WifiP2pManager#WIFI_P2P_THIS_DEVICE_CHANGED_ACTION
   */
  private fun handleDeviceChanged(device: WifiP2pDevice?) {
    if (device == null) {
      listener.handleWifiP2pDisabled()
    } else {
      listener.handleWifiP2pDevice(device)
    }
  }
}
