/*
 * Copyright 2022 Ona Systems, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartregister.p2p.data_sharing

import android.app.Activity
import android.content.BroadcastReceiver
import android.content.Context
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.net.wifi.p2p.WifiP2pDevice
import android.net.wifi.p2p.WifiP2pGroup
import android.net.wifi.p2p.WifiP2pInfo
import android.net.wifi.p2p.WifiP2pManager
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.robolectric.util.ReflectionHelpers
import org.smartregister.p2p.robolectric.RobolectricTest

class WifiDirectDataSharingStrategyTest : RobolectricTest() {

  private lateinit var wifiDirectDataSharingStrategy: WifiDirectDataSharingStrategy
  private lateinit var context: Activity
  private lateinit var wifiP2pManager: WifiP2pManager
  private lateinit var wifiP2pChannel: WifiP2pManager.Channel
  private lateinit var wifiP2pReceiver: BroadcastReceiver

  private lateinit var onDeviceFound: OnDeviceFound
  private lateinit var pairingListener: DataSharingStrategy.PairingListener

  private lateinit var wifiP2pInfo: WifiP2pInfo
  private lateinit var onConnectionInfo: (() -> Unit)
  private lateinit var wifiP2pGroup: WifiP2pGroup
  private lateinit var currentDevice: WifiP2pDevice

  private lateinit var socket: Socket
  private lateinit var dataInputStream: DataInputStream
  private lateinit var dataOutputStream: DataOutputStream

  @Before
  fun setUp() {
    wifiP2pManager = mockk()
    wifiP2pChannel = mockk()
    wifiP2pReceiver = mockk()
    context = spyk(Activity())
    onDeviceFound = mockk()
    pairingListener = mockk()
    wifiDirectDataSharingStrategy = spyk(recordPrivateCalls = true)
    wifiDirectDataSharingStrategy.setActivity(context)
    every { context.getSystemService(Context.WIFI_P2P_SERVICE) } returns wifiP2pManager

    every { wifiDirectDataSharingStrategy getProperty "wifiP2pManager" } returns wifiP2pManager
    ReflectionHelpers.setField(wifiDirectDataSharingStrategy, "wifiP2pReceiver", wifiP2pReceiver)
    ReflectionHelpers.setField(wifiDirectDataSharingStrategy, "wifiP2pChannel", wifiP2pChannel)
  }

  @Test
  fun `searchDevices() calls wifiP2pManager#initialize()`() {
    every { context.checkPermission(any(), any(), any()) } returns PackageManager.PERMISSION_GRANTED
    every { wifiP2pManager.initialize(context, context.mainLooper, null) } returns wifiP2pChannel
    every { context.registerReceiver(any(), any()) } returns null
    every { wifiP2pManager.discoverPeers(any(), any()) } just runs
    wifiDirectDataSharingStrategy.searchDevices(onDeviceFound, pairingListener)

    verify { wifiP2pManager.initialize(context, context.mainLooper, null) }
  }

  @Test
  fun `requestConnectionInfo() calls onConnectionInfoAvailable()`() {
    every { wifiP2pManager.requestConnectionInfo(any(), any()) } just runs
    ReflectionHelpers.callInstanceMethod<WifiDirectDataSharingStrategy>(
      wifiDirectDataSharingStrategy,
      "requestConnectionInfo"
    )
    verify { wifiP2pManager.requestConnectionInfo(wifiP2pChannel, any()) }
  }

  @Test
  fun `listenForWifiP2pIntents() calls context#registerReceiver() with correct intent filter actions`() {
    every { context.registerReceiver(any(), any()) } returns null
    ReflectionHelpers.callInstanceMethod<WifiDirectDataSharingStrategy>(
      wifiDirectDataSharingStrategy,
      "listenForWifiP2pIntents"
    )

    val broadcastReceiverSlot = slot<BroadcastReceiver>()
    val intentFilterSlot = slot<IntentFilter>()
    verify { context.registerReceiver(capture(broadcastReceiverSlot), capture(intentFilterSlot)) }
    Assert.assertTrue(
      intentFilterSlot.captured.hasAction(WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION)
    )
    Assert.assertTrue(
      intentFilterSlot.captured.hasAction(WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION)
    )
    Assert.assertTrue(
      intentFilterSlot.captured.hasAction(WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION)
    )
    Assert.assertTrue(
      intentFilterSlot.captured.hasAction(WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION)
    )
    Assert.assertTrue(
      intentFilterSlot.captured.hasAction(WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION)
    )
  }

  @Test
  fun `initiatePeerDiscovery() calls wifiP2pManager#discoverPeers()`() {
    every { context.checkPermission(any(), any(), any()) } returns PackageManager.PERMISSION_GRANTED
    every { wifiP2pManager.discoverPeers(any(), any()) } just runs
    ReflectionHelpers.callInstanceMethod<WifiDirectDataSharingStrategy>(
      wifiDirectDataSharingStrategy,
      "initiatePeerDiscovery",
      ReflectionHelpers.ClassParameter.from(OnDeviceFound::class.java, onDeviceFound)
    )

    val actionListenerSlot = slot<WifiP2pManager.ActionListener>()
    verify { wifiP2pManager.discoverPeers(wifiP2pChannel, capture(actionListenerSlot)) }
  }

  @Test
  fun `initiatePeerDiscovery() calls onSearchingFailed() and onDeviceFound#failed() when WifiP2pManager#ActionListener() returns failure`() {
    every { context.checkPermission(any(), any(), any()) } returns PackageManager.PERMISSION_GRANTED
    every { wifiP2pManager.discoverPeers(any(), any()) } just runs
    every { onDeviceFound.failed(any()) } just runs
    ReflectionHelpers.callInstanceMethod<WifiDirectDataSharingStrategy>(
      wifiDirectDataSharingStrategy,
      "initiatePeerDiscovery",
      ReflectionHelpers.ClassParameter.from(OnDeviceFound::class.java, onDeviceFound)
    )

    val actionListenerSlot = slot<WifiP2pManager.ActionListener>()
    verify { wifiP2pManager.discoverPeers(wifiP2pChannel, capture(actionListenerSlot)) }

    val exceptionSlot = slot<Exception>()
    actionListenerSlot.captured.onFailure(0)
    verify { onDeviceFound.failed(capture(exceptionSlot)) }
    Assert.assertEquals("0: Error", exceptionSlot.captured.message)
    verify { wifiDirectDataSharingStrategy.onSearchingFailed(capture(exceptionSlot)) }
    Assert.assertEquals("0: Error", exceptionSlot.captured.message)
  }

  @Test
  fun `initiatePeerDiscovery() calls logDebug() when WifiP2pManager#ActionListener() returns success`() {
    every { context.checkPermission(any(), any(), any()) } returns PackageManager.PERMISSION_GRANTED
    every { wifiP2pManager.discoverPeers(any(), any()) } just runs
    ReflectionHelpers.callInstanceMethod<WifiDirectDataSharingStrategy>(
      wifiDirectDataSharingStrategy,
      "initiatePeerDiscovery",
      ReflectionHelpers.ClassParameter.from(OnDeviceFound::class.java, onDeviceFound)
    )

    val actionListenerSlot = slot<WifiP2pManager.ActionListener>()
    verify { wifiP2pManager.discoverPeers(wifiP2pChannel, capture(actionListenerSlot)) }

    actionListenerSlot.captured.onSuccess()
    verify {
      wifiDirectDataSharingStrategy invoke
        "logDebug" withArguments
        listOf("Discovering peers successful")
    }
  }
}