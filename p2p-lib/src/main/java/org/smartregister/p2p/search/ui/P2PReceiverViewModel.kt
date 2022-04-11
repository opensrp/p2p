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

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.google.gson.Gson
import kotlinx.coroutines.launch
import org.smartregister.p2p.P2PLibrary
import org.smartregister.p2p.SyncPayload
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.data_sharing.IReceiverSyncLifecycleCallback
import org.smartregister.p2p.data_sharing.WifiDirectDataSharingStrategy
import org.smartregister.p2p.model.P2PReceivedHistory
import org.smartregister.p2p.search.contract.P2pModeSelectContract
import org.smartregister.p2p.utils.Constants

class P2PReceiverViewModel : ViewModel(), IReceiverSyncLifecycleCallback, P2pModeSelectContract.ReceiverViewModel {

  private val connectionLevel: Constants.ConnectionLevel? = null

  fun checkIfDeviceKeyHasChanged(senderDeviceId: String) {
    viewModelScope.launch {
      var receivedHistory =
        P2PLibrary.getInstance()!!
          .getDb()
          ?.p2pReceivedHistoryDao()
          ?.getDeviceReceivedHistory(senderDeviceId)

      sendLastReceivedRecords(receivedHistory)
    }
  }

  override fun sendLastReceivedRecords(receivedHistory: List<P2PReceivedHistory?>?) {

    val deviceInfo: MutableMap<String, String?> = HashMap()
    deviceInfo[Constants.BasicDeviceDetails.KEY_APP_LIFETIME_KEY] =
      P2PLibrary.getInstance()!!.getHashKey()
    deviceInfo[Constants.BasicDeviceDetails.KEY_DEVICE_ID] =
      P2PLibrary.getInstance()!!.getDeviceUniqueIdentifier()

    WifiDirectDataSharingStrategy()
      .send(
        device = DeviceInfo(strategySpecificDevice = deviceInfo)
        /** Find out how to get this */
        ,
        syncPayload =
          SyncPayload(
            Gson().toJson(receivedHistory),
          ),
        object : DataSharingStrategy.OperationListener {
          override fun onSuccess(device: DeviceInfo) {
            TODO("Not yet implemented")
          }

          override fun onFailure(device: DeviceInfo, ex: Exception) {
            TODO("Not yet implemented")
          }
        }
      )
  }

  override fun getSendingDeviceId(): String {
    TODO("Not yet implemented")
  }
}