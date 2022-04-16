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
package org.smartregister.p2p.search.contract

import androidx.annotation.NonNull
import org.smartregister.p2p.authentication.model.DeviceRole
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.data_sharing.Manifest
import org.smartregister.p2p.payload.StringPayload

/** Interface for functions used to make changes to the data transfer page UI */
interface P2pModeSelectContract {

  fun showP2PSelectPage(deviceRole: DeviceRole, deviceName: String)

  fun getDeviceRole(): DeviceRole

  interface SenderViewModel {
    fun sendManifest(@NonNull manifest: Manifest)
    fun getCurrentConnectedDevice(): DeviceInfo?
    fun processReceivedHistory(syncPayload: StringPayload)
    fun requestSyncParams(deviceInfo: DeviceInfo?)
  }

  interface ReceiverViewModel {
    fun getSendingDeviceId(): String
  }
}
