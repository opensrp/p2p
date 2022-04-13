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
import androidx.lifecycle.ViewModelProvider
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.smartregister.p2p.P2PLibrary
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.data_sharing.Manifest
import org.smartregister.p2p.data_sharing.WifiDirectDataSharingStrategy
import org.smartregister.p2p.data_sharing.SyncSenderHandler
import org.smartregister.p2p.model.P2PReceivedHistory
import org.smartregister.p2p.payload.StringPayload
import org.smartregister.p2p.search.contract.P2pModeSelectContract
import org.smartregister.p2p.utils.Constants

class P2PSenderViewModel(private val context: P2PDeviceSearchActivity,
                         private val dataSharingStrategy: DataSharingStrategy
) : ViewModel(), P2pModeSelectContract.SenderViewModel {

  private var connectionLevel: Constants.ConnectionLevel? = null

  private lateinit var syncSenderHandler: SyncSenderHandler

  override fun requestSyncParams(deviceInfo: DeviceInfo) {
    // write a message to the socket requesting the receiver for acceptable data types
    // and their last update times which can be sent using a simple string command,
    // 'SEND_SYNC_PARAMS', and the **app_lifetime_key**

 /*   val deviceInfo: MutableMap<String, String?> = HashMap()
    deviceInfo[Constants.BasicDeviceDetails.KEY_APP_LIFETIME_KEY] =
      P2PLibrary.getInstance()!!.getHashKey()
    deviceInfo[Constants.BasicDeviceDetails.KEY_DEVICE_ID] =
      P2PLibrary.getInstance()!!.getDeviceUniqueIdentifier()*/

    dataSharingStrategy
      .send(
        device = DeviceInfo(strategySpecificDevice = deviceInfo)
        /** Find out how to get this */
        ,
        syncPayload =
        StringPayload(
            Gson().toJson(Constants.SEND_SYNC_PARAMS),
          ),
        object : DataSharingStrategy.OperationListener {
          override fun onSuccess(device: DeviceInfo) {
          }

          override fun onFailure(device: DeviceInfo, ex: Exception) {
          }
        }
      )
  }

  override fun sendManifest(manifest: Manifest) {
    if (getCurrentConnectedDevice() != null) {
      WifiDirectDataSharingStrategy()
        .sendManifest(
          device = getCurrentConnectedDevice(),
          manifest = manifest,
          object : DataSharingStrategy.OperationListener {
            override fun onSuccess(device: DeviceInfo) {
            }

            override fun onFailure(device: DeviceInfo, ex: Exception) {
            }
          }
        )
    }
  }

  override fun getCurrentConnectedDevice(): DeviceInfo {
    return context.getCurrentConnectedDevice()
  }

  override fun processReceivedHistory(syncPayload: StringPayload) {
    connectionLevel = Constants.ConnectionLevel.RECEIPT_OF_RECEIVED_HISTORY

    // Handle non empty payload
    if (syncPayload != null) {
      val receivedHistoryListType = object : TypeToken<ArrayList<P2PReceivedHistory?>?>() {}.type
      val receivedHistory: List<P2PReceivedHistory> =
        Gson().fromJson(syncPayload.toString(), receivedHistoryListType)

      // TODO run this is background
      val jsonData = P2PLibrary.getInstance().getSenderTransferDao().getP2PDataTypes()

      syncSenderHandler = SyncSenderHandler(p2PSenderViewModel = this,
        dataSyncOrder = jsonData,
        receivedHistory = receivedHistory)


      if (jsonData != null) {
        syncSenderHandler.startSyncProcess()
      } else {
        // sendSyncComplete
      }
    }
  }

  class Factory(private val context: P2PDeviceSearchActivity,
                private val dataSharingStrategy: DataSharingStrategy)
    : ViewModelProvider.Factory {
    override fun <T : ViewModel?> create(modelClass: Class<T>): T {
      return P2PSenderViewModel(context, dataSharingStrategy) as T
    }
  }
}
