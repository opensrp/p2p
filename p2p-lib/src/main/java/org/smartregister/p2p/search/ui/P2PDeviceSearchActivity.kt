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
package org.smartregister.p2p.search.ui

import android.Manifest
import android.content.Context
import android.content.Intent
import android.content.IntentSender
import android.content.pm.PackageManager
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Bundle
import android.provider.Settings
import android.view.WindowManager
import android.widget.Toast
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.annotation.RequiresApi
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import com.google.android.gms.common.api.ResolvableApiException
import com.google.android.gms.location.LocationRequest
import com.google.android.gms.location.LocationServices
import com.google.android.gms.location.LocationSettingsRequest
import com.google.android.gms.location.LocationSettingsResponse
import com.google.android.gms.tasks.OnFailureListener
import com.google.android.gms.tasks.OnSuccessListener
import org.smartregister.p2p.P2PLibrary
import org.smartregister.p2p.R
import org.smartregister.p2p.authentication.model.DeviceRole
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.DeviceInfo
import org.smartregister.p2p.model.P2PState
import org.smartregister.p2p.model.TransferProgress
import org.smartregister.p2p.search.contract.P2pModeSelectContract
import org.smartregister.p2p.search.ui.p2p.P2PScreen
import org.smartregister.p2p.search.ui.p2p.P2PViewModel
import org.smartregister.p2p.search.ui.theme.AppTheme
import org.smartregister.p2p.utils.DefaultDispatcherProvider
import org.smartregister.p2p.utils.isAppDebuggable
import org.smartregister.p2p.utils.startP2PScreen
import timber.log.Timber

/**
 * This is the exposed activity that provides access to all P2P operations and steps. It can be
 * called from other apps via [startP2PScreen] function.
 */
class P2PDeviceSearchActivity : AppCompatActivity(), P2pModeSelectContract.View {

  private val accessFineLocationPermissionRequestInt: Int = 12345
  private val p2PReceiverViewModel by viewModels<P2PReceiverViewModel> {
    P2PReceiverViewModel.Factory(
      dataSharingStrategy = dataSharingStrategy,
      DefaultDispatcherProvider()
    )
  }
  private val p2PSenderViewModel by viewModels<P2PSenderViewModel> {
    P2PSenderViewModel.Factory(
      dataSharingStrategy = dataSharingStrategy,
      DefaultDispatcherProvider()
    )
  }
  private val p2PViewModel by viewModels<P2PViewModel> {
    P2PViewModel.Factory(dataSharingStrategy = dataSharingStrategy, DefaultDispatcherProvider())
  }
  private var scanning = false

  private lateinit var dataSharingStrategy: DataSharingStrategy

  private var keepScreenOnCounter = 0

  val REQUEST_CHECK_LOCATION_ENABLED = 2398
  private lateinit var androidWifiManager: WifiManager

  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    if (Timber.treeCount == 0 && isAppDebuggable(this)) {
      Timber.plant(Timber.DebugTree())
    }

    supportActionBar?.setHomeAsUpIndicator(android.R.drawable.ic_menu_close_clear_cancel)

    // Remaining setup for the DataSharingStrategy class
    dataSharingStrategy = P2PLibrary.getInstance().dataSharingStrategy!!
    dataSharingStrategy.setActivity(this)

    // use compose
    setContent {
      AppTheme {
        P2PScreen(
          p2PUiState = p2PViewModel.p2PUiState.value,
          onEvent = p2PViewModel::onEvent,
          p2PViewModel = p2PViewModel
        )
      }
    }

    if (Timber.treeCount == 0 && isAppDebuggable(this)) {
      Timber.plant(Timber.DebugTree())
    }

    title = getString(R.string.device_to_device_sync)

    androidWifiManager = getAndroidWifiManager()

    registerUIViewModelEvents()
    registerSenderViewModelEvents()
    registerReceiverViewModelEvents()
  }

  fun registerUIViewModelEvents() {
    registerViewModelEvents(p2PViewModel)
  }

  fun registerViewModelEvents(baseViewModel: BaseViewModel) {
    baseViewModel.displayMessages.observe(this) { stringResId -> showToast(getString(stringResId)) }
    baseViewModel.restartActivity.observe(this) {
      if (it) {
        restartActivity()
      }
    }

    baseViewModel.p2pState.observe(this) { p2pState -> updateP2PState(p2pState) }
    baseViewModel.p2pUiAction.observe(this) { pair ->
      val uiAction = pair.first
      val data = pair.second

      when (uiAction) {
        UIAction.SHOW_TRANSFER_COMPLETE_DIALOG -> {
          showTransferCompleteDialog(data as P2PState)
        }
        UIAction.NOTIFY_DATA_TRANSFER_STARTING -> {
          notifyDataTransferStarting(data as DeviceRole)
        }
        UIAction.SHOW_CANCEL_TRANSFER_DIALOG -> {
          showCancelTransferDialog()
        }
        UIAction.SENDER_SYNC_COMPLETE -> {
          senderSyncComplete(data as Boolean)
        }
        UIAction.UPDATE_TRANSFER_PROGRESS -> {
          updateTransferProgress(data as TransferProgress)
        }
        UIAction.REQUEST_LOCATION_PERMISSIONS_ENABLE_LOCATION -> {
          requestLocationPermissionsAndEnableLocation()
        }
        UIAction.FINISH -> {
          finish()
        }
        UIAction.KEEP_SCREEN_ON -> {
          keepScreenOn(data as Boolean)
        }
        UIAction.PROCESS_SENDER_DEVICE_DETAILS -> {
          processSenderDeviceDetails()
        }
        UIAction.SEND_DEVICE_DETAILS -> {
          sendDeviceDetails()
        }
      }
    }
  }

  fun registerSenderViewModelEvents() {
    registerViewModelEvents(p2PSenderViewModel)
  }

  fun registerReceiverViewModelEvents() {
    registerViewModelEvents(p2PReceiverViewModel)
  }

  override fun showToast(msg: String) {
    runOnUiThread { Toast.makeText(this@P2PDeviceSearchActivity, msg, Toast.LENGTH_LONG).show() }
  }

  override fun requestLocationPermissionsAndEnableLocation() {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
      requestAccessFineLocationIfNotGranted()
    }

    if (hasPermission(Manifest.permission.ACCESS_FINE_LOCATION)) {
      checkLocationEnabled()
    }
  }

  fun hasPermission(permission: String): Boolean =
    checkCallingOrSelfPermission(permission) == PackageManager.PERMISSION_GRANTED

  fun checkEnableWifi() {

    if (androidWifiManager.isWifiEnabled) {
      p2PViewModel.updateP2PState(P2PState.WIFI_AND_LOCATION_ENABLE)
      p2PViewModel.startScanning()
      return
    }

    showToast(getString(R.string.turn_on_wifi))
    if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) {
      val intent = Intent(Settings.ACTION_WIRELESS_SETTINGS)
      startForResult.launch(intent)
    } else {
      val intent = Intent(Settings.Panel.ACTION_WIFI)
      startForResult.launch(intent)
    }
  }

  private val startForResult =
    registerForActivityResult(ActivityResultContracts.StartActivityForResult()) {
      if (androidWifiManager.isWifiEnabled) {
        p2PViewModel.updateP2PState(P2PState.WIFI_AND_LOCATION_ENABLE)
        p2PViewModel.startScanning()
      }
    }

  fun getAndroidWifiManager(): WifiManager {
    return applicationContext.getSystemService(Context.WIFI_SERVICE) as (WifiManager)
  }

  /**
   * Checks if location is currently enabled
   *
   * @param activity
   */
  fun checkLocationEnabled() {
    val builder = LocationSettingsRequest.Builder().addLocationRequest(createLocationRequest())
    val result = LocationServices.getSettingsClient(this).checkLocationSettings(builder.build())
    result.addOnSuccessListener(
      this,
      OnSuccessListener<LocationSettingsResponse?> {
        // All location settings are satisfied. The client can initialize
        // location requests here.
        checkEnableWifi()
      }
    )
    result.addOnFailureListener(
      this,
      OnFailureListener { e ->
        if (e is ResolvableApiException) {
          // Location settings are not satisfied, but this can be fixed
          // by showing the user a dialog.
          try {
            // Show the dialog by calling startResolutionForResult(),
            // and check the result in onActivityResult().
            val resolvable = e as ResolvableApiException
            resolvable.startResolutionForResult(
              this@P2PDeviceSearchActivity,
              REQUEST_CHECK_LOCATION_ENABLED
            )
          } catch (sendEx: IntentSender.SendIntentException) {
            // Ignore the error.
            Timber.e(sendEx)
          }
        }
      }
    )
  }

  fun createLocationRequest(): LocationRequest {
    return LocationRequest.create().apply {
      interval = 3600000
      fastestInterval = 3600000
      priority = LocationRequest.PRIORITY_LOW_POWER
    }
  }

  override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
    super.onActivityResult(requestCode, resultCode, data)

    if (requestCode == REQUEST_CHECK_LOCATION_ENABLED && resultCode == RESULT_OK) {
      requestLocationPermissionsAndEnableLocation()
    }
  }

  /* DO NOT DELETE THIS ->> FOR USE LATER
  fun renameWifiDirectName() {
    val deviceName = getDeviceName(this)

    if (Build.VERSION.SDK_INT > 24) {
      // Copy the text to the Clipboard
      val clipboard: ClipboardManager = getSystemService(CLIPBOARD_SERVICE) as ClipboardManager
      val clip = ClipData.newPlainText(getString(R.string.wifi_direct_name), deviceName)
      clipboard.setPrimaryClip(clip)

      Toast.makeText(
          this,
          getString(R.string.wifi_direct_name_setup_instructions),
          Toast.LENGTH_LONG
        )
        .show()

      val turnWifiOn = Intent(Settings.ACTION_WIFI_SETTINGS)
      startActivity(turnWifiOn)
    } else {
      val setDeviceNameMethod =
        wifiP2pManager.javaClass.getMethod(
          "setDeviceName",
          wifiP2pChannel!!.javaClass,
          String::class.java,
          WifiP2pManager.ActionListener::class.java
        )
      setDeviceNameMethod.invoke(
        wifiP2pManager,
        wifiP2pChannel,
        deviceName,
        object : WifiP2pManager.ActionListener {
          override fun onSuccess() {
            Timber.e("Change device name worked")
          }

          override fun onFailure(reason: Int) {
            Timber.e("Change device name did not work")
          }
        }
      )
    }
  }*/

  override fun onResume() {
    super.onResume()

    p2PViewModel.initChannel()
    dataSharingStrategy.onResume(isScanning = scanning)
  }

  override fun onPause() {
    super.onPause()
    dataSharingStrategy.onPause()
  }

  @RequiresApi(Build.VERSION_CODES.M)
  internal fun requestAccessFineLocationIfNotGranted() {
    when (ActivityCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)) {
      PackageManager.PERMISSION_GRANTED -> logDebug("Wifi P2P: Access fine location granted")
      else -> {
        logDebug("Wifi P2P: Requesting access fine location permission")
        return requestPermissions(
          arrayOf(Manifest.permission.ACCESS_FINE_LOCATION),
          accessFineLocationPermissionRequestInt
        )
      }
    }
  }

  override fun sendDeviceDetails() {
    p2PSenderViewModel.sendDeviceDetails(getCurrentConnectedDevice())
  }

  override fun processSenderDeviceDetails() {
    p2PReceiverViewModel.processSenderDeviceDetails()
  }

  override fun showTransferCompleteDialog(p2PState: P2PState) {
    p2PViewModel.showTransferCompleteDialog(p2PState)
  }

  override fun showCancelTransferDialog() {
    p2PViewModel.showCancelTransferDialog()
  }

  private fun logDebug(message: String) {
    Timber.d(message)
  }

  override fun getCurrentConnectedDevice(): DeviceInfo? {
    return dataSharingStrategy.getCurrentDevice()
  }

  override fun senderSyncComplete(complete: Boolean) {
    p2PViewModel.updateSenderSyncComplete(complete)
    Timber.d("sender sync complete $complete")
  }

  override fun updateTransferProgress(transferProgress: TransferProgress) {
    p2PViewModel.updateTransferProgress(transferProgress = transferProgress)
  }

  override fun notifyDataTransferStarting(deviceRole: DeviceRole) {
    when (deviceRole) {
      DeviceRole.SENDER -> p2PViewModel.updateP2PState(P2PState.TRANSFERRING_DATA)
      DeviceRole.RECEIVER -> p2PViewModel.updateP2PState(P2PState.RECEIVING_DATA)
    }
  }

  override fun restartActivity() {
    startActivity(Intent(this, P2PDeviceSearchActivity::class.java))
    finish()
  }

  override fun updateP2PState(p2PState: P2PState) {
    p2PViewModel.updateP2PState(p2PState)
  }

  /**
   * Enables or disables the keep screen on flag to avoid the device going to sleep while there is a
   * sync happening
   *
   * @param enable `TRUE` to enable or `FALSE` disable
   */
  override fun keepScreenOn(enable: Boolean) {
    if (enable) {
      keepScreenOnCounter++
      if (keepScreenOnCounter == 1) {
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
      }
    } else {
      keepScreenOnCounter--
      if (keepScreenOnCounter == 0) {
        window.clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
      }
    }
  }

  override fun onRequestPermissionsResult(
    requestCode: Int,
    permissions: Array<out String>,
    grantResults: IntArray
  ) {
    super.onRequestPermissionsResult(requestCode, permissions, grantResults)

    if (accessFineLocationPermissionRequestInt == requestCode &&
        hasPermission(Manifest.permission.ACCESS_FINE_LOCATION)
    ) {
      checkLocationEnabled()
    }
  }

  override fun onStop() {
    dataSharingStrategy.onStop()
    super.onStop()
  }

  override fun onDestroy() {
    viewModelStore.clear()
    P2PLibrary.getInstance().clean()

    super.onDestroy()
  }
}
