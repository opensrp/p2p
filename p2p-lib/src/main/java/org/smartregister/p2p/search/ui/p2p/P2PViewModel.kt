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
package org.smartregister.p2p.search.ui.p2p

import androidx.compose.runtime.mutableStateOf
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import java.util.Timer
import kotlin.concurrent.schedule
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.smartregister.p2p.R
import org.smartregister.p2p.authentication.model.DeviceRole
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.data_sharing.OnDeviceFound
import org.smartregister.p2p.model.P2PState
import org.smartregister.p2p.model.TransferProgress
import org.smartregister.p2p.search.ui.P2PDeviceSearchActivity
import org.smartregister.p2p.utils.DispatcherProvider
import timber.log.Timber

const val START_DATA_TRANSFER_DELAY: Long = 5000

class P2PViewModel(
  private val view: P2PDeviceSearchActivity,
  private val dataSharingStrategy: DataSharingStrategy,
  private val dispatcherProvider: DispatcherProvider
) : ViewModel() {
  val p2PUiState = mutableStateOf(P2PUiState())
  private val _deviceList = MutableLiveData<List<DeviceInfo>>()
  val deviceList: LiveData<List<DeviceInfo>>
    get() = _deviceList

  private val _p2PState = MutableLiveData<P2PState>()
  val p2PState: LiveData<P2PState>
    get() = _p2PState

  var deviceRole: DeviceRole = DeviceRole.SENDER
  private var currentConnectedDevice: DeviceInfo? = null

  fun onEvent(event: P2PEvent) {
    when (event) {
      is P2PEvent.StartScanning -> {
        // initiate scanning
        startScanning(dataSharingStrategy = dataSharingStrategy)
      }
      is P2PEvent.PairDevicesFound -> {
        // display list of pairing devices
      }
      is P2PEvent.PairWithDevice -> {
        // initiate pairing with device
        connectToDevice(event.device)
      }
      is P2PEvent.CancelDataTransfer -> {
        // show cancel transfer dialog
        p2PUiState.value = p2PUiState.value.copy(showP2PDialog = true)
      }
      P2PEvent.ConnectionBreakConfirmed -> {
        // cancel data transfer
        cancelTransfer(P2PState.INITIATE_DATA_TRANSFER)
      }
      P2PEvent.DismissConnectionBreakDialog -> {
        p2PUiState.value = p2PUiState.value.copy(showP2PDialog = false)
      }
      P2PEvent.DataTransferCompleteConfirmed -> {
        _p2PState.postValue(P2PState.PROMPT_NEXT_TRANSFER)
      }
    }
  }

  fun startScanning(dataSharingStrategy: DataSharingStrategy) {
    view.keepScreenOn(true)
    dataSharingStrategy.searchDevices(
      object : OnDeviceFound {
        override fun deviceFound(devices: List<DeviceInfo>) {
          _deviceList.postValue(devices)
          if (deviceRole == DeviceRole.SENDER &&
              (_p2PState.value == null || _p2PState.value == P2PState.INITIATE_DATA_TRANSFER)
          ) {
            _p2PState.postValue(P2PState.PAIR_DEVICES_FOUND)
          }

          Timber.e("Devices searching succeeded. Found ${devices.size} devices")
        }

        override fun failed(ex: Exception) {
          view.keepScreenOn(false)
          Timber.e("Devices searching failed")
          Timber.e(ex)
          // removeScanningDialog()

          view.showToast(view.getString(R.string.device_searching_failed))
        }
      },
      object : DataSharingStrategy.PairingListener {

        override fun onSuccess(device: DeviceInfo?) {

          if (currentConnectedDevice == null) {
            Timber.e("Devices paired with another: DeviceInfo is null")
          }

          Timber.e("Devices paired with another: DeviceInfo is +++++")

          currentConnectedDevice = device

          // find better way to track this
          if (deviceRole == DeviceRole.RECEIVER) {
            _p2PState.postValue(P2PState.WAITING_TO_RECEIVE_DATA)
            view.processSenderDeviceDetails()
          }
        }

        override fun onFailure(device: DeviceInfo?, ex: Exception) {
          view.keepScreenOn(false)
          Timber.e("Devices pairing failed")
          Timber.e(ex)
          // removeScanningDialog()
        }

        override fun onDisconnected() {
          if (!view.requestDisconnection) {
            // removeScanningDialog()
            view.showToast("Connection was disconnected")

            view.keepScreenOn(false)

            /*  if (isSenderSyncComplete) {
              showTransferCompleteDialog()
            }*/

            Timber.e("Successful on disconnect")
            Timber.e("isSenderSyncComplete $view.isSenderSyncComplete")
            // But use a flag to determine if sync was completed
          }
          _p2PState.postValue(P2PState.TRANSFER_COMPLETE)
        }
      }
    )
  }

  fun connectToDevice(device: DeviceInfo) {
    dataSharingStrategy.connect(
      device,
      object : DataSharingStrategy.OperationListener {
        override fun onSuccess(device: DeviceInfo?) {
          // scanning = false
          currentConnectedDevice = device
          Timber.e("Connecting to device %s success", device?.getDisplayName() ?: "Unknown")
          _p2PState.postValue(P2PState.PREPARING_TO_SEND_DATA)
          Timber.e("connect to device sets ${P2PState.PREPARING_TO_SEND_DATA.name} +++++++")

          Timber.e("calling sleep before sending")
          Timer().schedule(START_DATA_TRANSFER_DELAY) {
            view.sendDeviceDetails()
            Timber.e("sending called after sleep")
          }
        }

        override fun onFailure(device: DeviceInfo?, ex: Exception) {
          Timber.e("Connecting to device %s failure", device?.getDisplayName() ?: "Unknown")
          Timber.e(ex)

          view.showToast(view.getString(R.string.connecting_to_device_failed))
        }
      }
    )
  }

  fun showTransferCompleteDialog() {
    _p2PState.postValue(P2PState.TRANSFER_COMPLETE)

    /*while (keepScreenOnCounter > 0) {
      keepScreenOn(false)
    }

    initInteractiveDialog()
    interactiveDialog.setContentView(R.layout.data_transfer_bottom_sheet)

    interactiveDialog
      .findViewById<TextView>(R.id.data_transfer_title)
      ?.setText(getString(R.string.data_transfer_comlete))

    interactiveDialog
      .findViewById<TextView>(R.id.data_transfer_description)
      ?.setText(String.format(getString(R.string.device_data_successfully_sent)))

    interactiveDialog.findViewById<ImageButton>(R.id.data_transfer_dialog_close)
      ?.setOnClickListener { interactiveDialog.cancel() }

    interactiveDialog.findViewById<Button>(R.id.dataTransferBtn)?.apply {
      setOnClickListener {
        // close wifi direct connection
        finish()
      }
      setText(getString(R.string.okay))
    }

    interactiveDialog.setCancelable(false)
    interactiveDialog.show()*/
  }

  fun cancelTransfer(p2PState: P2PState = P2PState.TRANSFER_CANCELLED) {
    Timber.e("Connection terminated by user")
    viewModelScope.launch {
      withContext(dispatcherProvider.main()) { view.showTransferCompleteDialog() }
      dataSharingStrategy.disconnect(
        dataSharingStrategy.getCurrentDevice()!!,
        object : DataSharingStrategy.OperationListener {
          override fun onSuccess(device: DeviceInfo?) {
            Timber.i("Diconnection successful")
            _p2PState.postValue(p2PState)
          }

          override fun onFailure(device: DeviceInfo?, ex: Exception) {
            Timber.e(ex, "P2P diconnection failed")
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

  fun updateP2PState(p2PState: P2PState) {
    _p2PState.postValue(p2PState)
  }

  fun closeP2PScreen() {
    view.finish()
  }

  fun setCurrentConnectedDevice(device: DeviceInfo?) {
    currentConnectedDevice = device
  }

  class Factory(
    private val context: P2PDeviceSearchActivity,
    private val dataSharingStrategy: DataSharingStrategy,
    private val dispatcherProvider: DispatcherProvider
  ) : ViewModelProvider.Factory {
    override fun <T : ViewModel> create(modelClass: Class<T>): T {
      return P2PViewModel(context, dataSharingStrategy, dispatcherProvider).apply {
        dataSharingStrategy.setCoroutineScope(viewModelScope)
      } as
        T
    }
  }
}
