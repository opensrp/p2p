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
package org.smartregister.p2p.search.ui

import android.net.wifi.p2p.WifiP2pDevice
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.Assert
import org.junit.Before
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.smartregister.p2p.CoroutineTestRule
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.data_sharing.OnDeviceFound
import org.smartregister.p2p.data_sharing.WifiDirectDataSharingStrategy
import org.smartregister.p2p.model.P2PState
import org.smartregister.p2p.robolectric.RobolectricTest
import org.smartregister.p2p.search.ui.p2p.P2PEvent
import org.smartregister.p2p.search.ui.p2p.P2PViewModel

class P2PViewModelTest : RobolectricTest() {
  @get:Rule var coroutinesTestRule = CoroutineTestRule()

  lateinit var p2PViewModel: P2PViewModel
  lateinit var view: P2PDeviceSearchActivity
  lateinit var dataSharingStrategy: DataSharingStrategy
  lateinit var deviceInfo: DeviceInfo

  @Before
  fun setUp() {
    view = mockk(relaxed = true)
    dataSharingStrategy = mockk()

    p2PViewModel =
      spyk(
        P2PViewModel(
          view = view,
          dataSharingStrategy = dataSharingStrategy,
          dispatcherProvider = coroutinesTestRule.testDispatcherProvider
        )
      )

    val wifiP2pDevice =
      WifiP2pDevice().apply {
        deviceName = "Google Pixel"
        deviceAddress = "00:00:5e:00:53:af"
      }
    deviceInfo = WifiDirectDataSharingStrategy.WifiDirectDevice(wifiP2pDevice)
  }

  @Test
  fun `onEvent() calls startScanning() when P2PEvent is StartScanningEvent`() {
    every { p2PViewModel.startScanning(dataSharingStrategy) } just runs
    p2PViewModel.onEvent(P2PEvent.StartScanning)
    verify { p2PViewModel.startScanning(dataSharingStrategy) }
  }

  @Test
  fun `onEvent() calls connectToDevice() when P2PEvent is PairWithDevice`() {
    every { p2PViewModel.connectToDevice(deviceInfo) } just runs
    p2PViewModel.onEvent(P2PEvent.PairWithDevice(device = deviceInfo))
    verify { p2PViewModel.connectToDevice(deviceInfo) }
  }

  @Test
  fun `onEvent() updates p2PUiState#showP2PDialog to true when P2PEvent is CancelDataTransfer`() {
    Assert.assertFalse(p2PViewModel.p2PUiState.value.showP2PDialog)
    p2PViewModel.onEvent(P2PEvent.CancelDataTransfer)
    Assert.assertTrue(p2PViewModel.p2PUiState.value.showP2PDialog)
  }

  @Test
  fun `onEvent() calls cancelTransfer() when P2PEvent is ConnectionBreakConfirmed`() {
    every { p2PViewModel.cancelTransfer(any()) } just runs
    p2PViewModel.onEvent(P2PEvent.ConnectionBreakConfirmed)
    verify { p2PViewModel.cancelTransfer(P2PState.INITIATE_DATA_TRANSFER) }
  }

  @Test
  fun `onEvent() updates p2PUiState#showP2PDialog to false when P2PEvent is DismissConnectionBreakDialog`() {
    p2PViewModel.p2PUiState.value = p2PViewModel.p2PUiState.value.copy(showP2PDialog = true)
    Assert.assertTrue(p2PViewModel.p2PUiState.value.showP2PDialog)
    p2PViewModel.onEvent(P2PEvent.DismissConnectionBreakDialog)
    Assert.assertFalse(p2PViewModel.p2PUiState.value.showP2PDialog)
  }

  // @Ignore("Fix failing livedata assertion")
  @Test
  fun `onEvent() updates p2PState to PROMPT_NEXT_TRANSFER when P2PEvent is DataTransferCompleteConfirmed`() {
    Assert.assertNull(p2PViewModel.p2PState.value)
    p2PViewModel.onEvent(P2PEvent.DataTransferCompleteConfirmed)
    Assert.assertEquals(P2PState.PROMPT_NEXT_TRANSFER, p2PViewModel.p2PState.value)
  }

  @Test
  fun `startScanning() should call view#keepScreenOn() and dataSharingStrategy#searchDevices()`() {
    every { view.keepScreenOn(true) } just runs
    every { dataSharingStrategy.searchDevices(any(), any()) } just runs

    p2PViewModel.startScanning(dataSharingStrategy = dataSharingStrategy)

    verify { view.keepScreenOn(true) }
    verify { dataSharingStrategy.searchDevices(any(), any()) }
  }

  @Ignore("Fix failing livedata assertion")
  @Test
  fun `startScanning() should update deviceList and p2pState to PAIR_DEVICES_FOUND when device role is SENDER when onDeviceFound#deviceFound is called`() {
    every { view.keepScreenOn(true) } just runs
    val onDeviceFoundSlot = slot<OnDeviceFound>()
    every { dataSharingStrategy.searchDevices(capture(onDeviceFoundSlot), any()) } just runs

    p2PViewModel.startScanning(dataSharingStrategy = dataSharingStrategy)

    val devicesList = listOf(deviceInfo)
    onDeviceFoundSlot.captured.deviceFound(devicesList)

    Assert.assertNotNull(p2PViewModel.deviceList.value)
  }

  @Test
  fun `startScanning() should update currentConnectedDevice when pairing#onSuccess is called`() {
    every { view.keepScreenOn(true) } just runs
    every { dataSharingStrategy.getCurrentDevice() } returns deviceInfo
    val pairingListenerSlot = slot<DataSharingStrategy.PairingListener>()
    every { dataSharingStrategy.searchDevices(any(), capture(pairingListenerSlot)) } just runs

    p2PViewModel.setCurrentConnectedDevice(null)
    Assert.assertNull(p2PViewModel.getCurrentConnectedDevice())

    p2PViewModel.startScanning(dataSharingStrategy = dataSharingStrategy)

    pairingListenerSlot.captured.onSuccess(deviceInfo)

    Assert.assertEquals(deviceInfo, p2PViewModel.getCurrentConnectedDevice())
  }

  @Test
  fun `startScanning() should call view#keepScreenOn() when pairing#onFailure is called`() {
    every { view.keepScreenOn(true) } just runs
    every { dataSharingStrategy.getCurrentDevice() } returns deviceInfo
    val pairingListenerSlot = slot<DataSharingStrategy.PairingListener>()
    every { dataSharingStrategy.searchDevices(any(), capture(pairingListenerSlot)) } just runs

    p2PViewModel.startScanning(dataSharingStrategy = dataSharingStrategy)

    pairingListenerSlot.captured.onFailure(deviceInfo, Exception(""))

    verify { view.keepScreenOn(false) }
  }
}
