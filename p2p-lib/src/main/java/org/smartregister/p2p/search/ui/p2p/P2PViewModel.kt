/*
 * Copyright 2022-2023 Ona Systems, Inc
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
package org.smartregister.p2p.search.ui.p2p

import androidx.compose.runtime.mutableStateOf
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.launch
import org.smartregister.p2p.R
import org.smartregister.p2p.authentication.model.DeviceRole
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.data_sharing.OnDeviceFound
import org.smartregister.p2p.model.P2PState
import org.smartregister.p2p.model.TransferProgress
import org.smartregister.p2p.search.ui.BaseViewModel
import org.smartregister.p2p.search.ui.UIAction
import org.smartregister.p2p.utils.DispatcherProvider
import timber.log.Timber

class P2PViewModel(
  private val dataSharingStrategy: DataSharingStrategy,
  private val dispatcherProvider: DispatcherProvider
) : BaseViewModel(dataSharingStrategy) {
  val p2PUiState = mutableStateOf(P2PUiState())
  private val _deviceList = MutableLiveData<List<DeviceInfo>>()
  val deviceList: LiveData<List<DeviceInfo>>
    get() = _deviceList

  private val _p2PState = MutableLiveData<P2PState>()
  val p2PState: LiveData<P2PState>
    get() = _p2PState

  var deviceRole: DeviceRole = DeviceRole.SENDER
  private var currentConnectedDevice: DeviceInfo? = null
  private var requestDisconnection = false
  private var isSenderSyncComplete = false

  fun onEvent(event: P2PEvent) {
    when (event) {
      is P2PEvent.StartScanning -> {
        // check whether location services are enabled
        postUIAction(UIAction.REQUEST_LOCATION_PERMISSIONS_ENABLE_LOCATION)
      }
      is P2PEvent.PairWithDevice -> {
        // initiate pairing with device
        connectToDevice(event.device)
      }
      is P2PEvent.CancelDataTransfer -> {
        // show cancel transfer dialog
        showCancelTransferDialog()
      }
      P2PEvent.ConnectionBreakConfirmed -> {
        // cancel data transfer
        cancelTransfer(P2PState.INITIATE_DATA_TRANSFER)
        setRequestDisconnection(true)
      }
      P2PEvent.DismissConnectionBreakDialog -> {
        p2PUiState.value = p2PUiState.value.copy(showP2PDialog = false)
      }
      P2PEvent.DataTransferCompleteConfirmed -> {
        updateP2PState(P2PState.PROMPT_NEXT_TRANSFER)
      }
      P2PEvent.BottomSheetClosed -> {
        cancelTransfer(P2PState.PROMPT_NEXT_TRANSFER)
      }
    }
  }

  fun startScanning() {
    postUIAction(UIAction.KEEP_SCREEN_ON, true)
    dataSharingStrategy.searchDevices(onDeviceFound, pairingListener)
  }

  val onDeviceFound by lazy {
    object : OnDeviceFound {
      override fun deviceFound(devices: List<DeviceInfo>) {
        _deviceList.postValue(devices)
        if (deviceRole == DeviceRole.SENDER &&
            (_p2PState.value == null || _p2PState.value == P2PState.INITIATE_DATA_TRANSFER)
        ) {
          updateP2PState(P2PState.PAIR_DEVICES_FOUND)
        }

        Timber.i("onDeviceFound: State is ${p2PState.value?.name}")
        Timber.i("Devices searching succeeded. Found ${devices.size} devices")
      }

      override fun failed(ex: Exception) {
        postUIAction(UIAction.KEEP_SCREEN_ON, false)

        Timber.d("Devices searching failed")
        Timber.e(ex)
        updateP2PState(P2PState.PAIR_DEVICES_SEARCH_FAILED)
      }
    }
  }

  val pairingListener: DataSharingStrategy.PairingListener by lazy {
    object : DataSharingStrategy.PairingListener {

      override fun onSuccess(device: DeviceInfo?) {

        if (!dataSharingStrategy.isPairingInitiated() && deviceRole == DeviceRole.SENDER) {
          Timber.i("pairingListener#onSuccess() -> pairingInitiated = false ${deviceRole.name}")
          return
        }
        currentConnectedDevice = device
        Timber.i("Device role is ${deviceRole.name}")

        when (deviceRole) {
          DeviceRole.RECEIVER -> {
            updateP2PState(P2PState.WAITING_TO_RECEIVE_DATA)
            postUIAction(UIAction.PROCESS_SENDER_DEVICE_DETAILS)
          }
          DeviceRole.SENDER -> {
            postUIAction(UIAction.SEND_DEVICE_DETAILS)
          }
        }
      }

      override fun onFailure(device: DeviceInfo?, ex: Exception) {
        // view.keepScreenOn(false)
        postUIAction(UIAction.KEEP_SCREEN_ON, false)
        Timber.i("Devices pairing failed")
        Timber.e(ex)
        updateP2PState(P2PState.PROMPT_NEXT_TRANSFER)
      }

      override fun onDisconnected() {
        Timber.d("onDisconnected()")
        if (!requestDisconnection) {
          showToast(R.string.connection_was_disconnected)

          postUIAction(UIAction.KEEP_SCREEN_ON, false)

          Timber.i("Successful on disconnect")
          Timber.i("isSenderSyncComplete $isSenderSyncComplete")
          // But use a flag to determine if sync was completed
          updateP2PState(P2PState.DEVICE_DISCONNECTED)
        }
        if (isSenderSyncComplete) {
          updateP2PState(P2PState.TRANSFER_COMPLETE)

          // TODO: Remove this, this should have fixed the socket bind address exception
          dataSharingStrategy.cleanup()
        }
      }
    }
  }

  fun initChannel() {
    dataSharingStrategy.initChannel(onDeviceFound, pairingListener)
  }

  fun connectToDevice(device: DeviceInfo) {
    dataSharingStrategy.connect(
      device,
      object : DataSharingStrategy.OperationListener {
        override fun onSuccess(device: DeviceInfo?) {
          currentConnectedDevice = device
          Timber.d("Connecting to device %s success", device?.getDisplayName() ?: "Unknown")
          updateP2PState(P2PState.PREPARING_TO_SEND_DATA)
        }

        override fun onFailure(device: DeviceInfo?, ex: Exception) {
          Timber.d("Connecting to device %s failure", device?.getDisplayName() ?: "Unknown")
          Timber.e(ex)
          updateP2PState(P2PState.CONNECT_TO_DEVICE_FAILED)
        }
      }
    )
  }

  fun showTransferCompleteDialog(p2PState: P2PState) {
    updateP2PState(p2PState)
  }

  fun cancelTransfer(p2PState: P2PState = P2PState.TRANSFER_CANCELLED) {
    if (dataSharingStrategy.getCurrentDevice() == null) {
      updateP2PState(p2PState)
      return
    }
    viewModelScope.launch {
      dataSharingStrategy.disconnect(
        dataSharingStrategy.getCurrentDevice()!!,
        object : DataSharingStrategy.OperationListener {
          override fun onSuccess(device: DeviceInfo?) {
            Timber.d("Disconnection successful")
            updateP2PState(p2PState)
          }

          override fun onFailure(device: DeviceInfo?, ex: Exception) {
            Timber.e(ex, "P2P diconnection failed")
            updateP2PState(p2PState)
          }
        }
      )
    }
  }

  fun updateTransferProgress(transferProgress: TransferProgress) {
    p2PUiState.value = p2PUiState.value.copy(transferProgress = transferProgress)
  }

  fun getCurrentConnectedDevice(): DeviceInfo? {
    return currentConnectedDevice
  }

  override fun updateP2PState(state: P2PState) {
    Timber.i("P2P state updated to ${state.name}")
    _p2PState.postValue(state)
  }

  fun closeP2PScreen() {
    if (dataSharingStrategy.getCurrentDevice() == null) {
      postUIAction(UIAction.FINISH)
      return
    }

    viewModelScope.launch {
      dataSharingStrategy.disconnect(
        dataSharingStrategy.getCurrentDevice()!!,
        object : DataSharingStrategy.OperationListener {
          override fun onSuccess(device: DeviceInfo?) {
            Timber.d("Disconnection successful")
            postUIAction(UIAction.FINISH)
          }

          override fun onFailure(device: DeviceInfo?, ex: Exception) {
            postUIAction(UIAction.FINISH)
            Timber.e(ex, "P2P diconnection failed")
          }
        }
      )
    }
  }

  fun setCurrentConnectedDevice(device: DeviceInfo?) {
    currentConnectedDevice = device
  }

  fun setRequestDisconnection(requestDisconnection: Boolean) {
    this.requestDisconnection = requestDisconnection
  }
  fun getRequestDisconnection(): Boolean {
    return requestDisconnection
  }

  fun showCancelTransferDialog() {
    // show cancel transfer dialog
    p2PUiState.value = p2PUiState.value.copy(showP2PDialog = true)
  }

  fun updateSenderSyncComplete(complete: Boolean = false) {
    this.isSenderSyncComplete = complete
  }

  override fun onCleared() {
    Timber.d("P2PViewModel onCleared")
    super.onCleared()
  }

  class Factory(
    private val dataSharingStrategy: DataSharingStrategy,
    private val dispatcherProvider: DispatcherProvider
  ) : ViewModelProvider.Factory {
    override fun <T : ViewModel> create(modelClass: Class<T>): T {
      return P2PViewModel(dataSharingStrategy, dispatcherProvider).apply {
        dataSharingStrategy.setCoroutineScope(viewModelScope)
      } as
        T
    }
  }
}
