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
package org.smartregister.p2p.data_sharing

import android.app.Activity
import android.content.BroadcastReceiver
import android.content.Context
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.net.wifi.p2p.WifiP2pConfig
import android.net.wifi.p2p.WifiP2pDevice
import android.net.wifi.p2p.WifiP2pDeviceList
import android.net.wifi.p2p.WifiP2pGroup
import android.net.wifi.p2p.WifiP2pInfo
import android.net.wifi.p2p.WifiP2pManager
import android.os.Build
import android.os.CountDownTimer
import androidx.annotation.RequiresApi
import androidx.core.app.ActivityCompat
import com.google.gson.Gson
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.text.NumberFormat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.smartregister.p2p.WifiP2pBroadcastReceiver
import org.smartregister.p2p.payload.BytePayload
import org.smartregister.p2p.payload.PayloadContract
import org.smartregister.p2p.payload.StringPayload
import org.smartregister.p2p.payload.SyncPayloadType
import org.smartregister.p2p.search.contract.P2PManagerListener
import org.smartregister.p2p.utils.DefaultDispatcherProvider
import org.smartregister.p2p.utils.DispatcherProvider
import timber.log.Timber

/** Created by Ephraim Kigamba - nek.eam@gmail.com on 21-03-2022. */
class WifiDirectDataSharingStrategy : DataSharingStrategy, P2PManagerListener {

  lateinit var context: Activity
  private val wifiP2pManager: WifiP2pManager by lazy(LazyThreadSafetyMode.NONE) {
    context.application.getSystemService(Context.WIFI_P2P_SERVICE) as WifiP2pManager
  }
  private val accessFineLocationPermissionRequestInt: Int = 12345
  private val nearbyWifiDevicesPermissionRequestInt: Int = 67890
  private var wifiP2pChannel: WifiP2pManager.Channel? = null
  private var wifiP2pReceiver: BroadcastReceiver? = null

  private var wifiP2pInfo: WifiP2pInfo? = null
  private var onConnectionInfo: (() -> Unit)? = null
  private var wifiP2pGroup: WifiP2pGroup? = null
  private var currentDevice: WifiP2pDevice? = null

  val PORT = 8988
  val SOCKET_TIMEOUT = 5_000

  private var socket: Socket? = null
  private var serverSocket: ServerSocket? = null
  private var dataInputStream: DataInputStream? = null
  private var dataOutputStream: DataOutputStream? = null

  private var requestedDisconnection = false
  private var isSearchingDevices = false
  private var paired = false
  private var dispatcherProvider: DispatcherProvider = DefaultDispatcherProvider()

  private lateinit var coroutineScope: CoroutineScope

  private val MANIFEST = "MANIFEST"

  var pairingTimeout: CountDownTimer? = null
  var pairingInitiated = false

  val successPairingListeners: MutableSet<() -> Unit> = mutableSetOf()
  val failedPairingListeners: MutableSet<(Exception) -> Unit> = mutableSetOf()

  override fun setDispatcherProvider(dispatcherProvider: DispatcherProvider) {
    this.dispatcherProvider = dispatcherProvider
  }

  override fun setActivity(context: Activity) {
    this.context = context
  }

  override fun setCoroutineScope(coroutineScope: CoroutineScope) {
    this.coroutineScope = coroutineScope
  }

  override fun searchDevices(
    onDeviceFound: OnDeviceFound,
    onConnected: DataSharingStrategy.PairingListener
  ) {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
      requestAccessFineLocationIfNotGranted()
    }

    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
      requestNearbyWifiDevicesNotGranted()
    }
    // Check if already connected and disconnect
    requestDeviceInfo(onDeviceFound = onDeviceFound, onConnected = onConnected)
  }

  private fun requestConnectionInfo() {
    wifiP2pManager.requestConnectionInfo(wifiP2pChannel) { onConnectionInfoAvailable(it, null) }
  }

  private fun listenForWifiP2pEventsIntents() {
    wifiP2pReceiver?.also {
      context.registerReceiver(
        it,
        IntentFilter().apply {
          addAction(WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION)
          addAction(WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION)
          addAction(WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION)
          addAction(WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION)
          addAction(WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION)
        }
      )
    }
  }

  private fun initiatePeerDiscoveryOnceAccessFineLocationGranted() {
    if ((ActivityCompat.checkSelfPermission(
        context,
        android.Manifest.permission.ACCESS_FINE_LOCATION
      )) != PackageManager.PERMISSION_GRANTED ||
        (ActivityCompat.checkSelfPermission(
          context,
          android.Manifest.permission.NEARBY_WIFI_DEVICES
        )) != PackageManager.PERMISSION_GRANTED
    ) {
      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M &&
          Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU
      ) {
        logDebug(
          "initiatePeerDiscoveryOnceAccessFineLocationGranted(): requesting  ACCESS_FINE_LOCATION"
        )
        requestAccessFineLocationIfNotGranted()
      } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        logDebug(
          "initiatePeerDiscoveryOnceAccessFineLocationGranted(): requesting  NEARBY_WIFI_DEVICES"
        )
        requestNearbyWifiDevicesNotGranted()
      } else {
        handleMinimumSDKVersionNotMet(Build.VERSION_CODES.M)
      }
    } else {
      initiatePeerDiscovery(null)
    }
  }

  @RequiresApi(Build.VERSION_CODES.M)
  private fun requestAccessFineLocationIfNotGranted() {
    when (ActivityCompat.checkSelfPermission(
        context,
        android.Manifest.permission.ACCESS_FINE_LOCATION
      )
    ) {
      PackageManager.PERMISSION_GRANTED -> logDebug("Wifi P2P: Access fine location granted")
      else -> {
        logDebug("Wifi P2P: Requesting access fine location permission")
        return context.requestPermissions(
          arrayOf(android.Manifest.permission.ACCESS_FINE_LOCATION),
          accessFineLocationPermissionRequestInt
        )
      }
    }
  }

  @RequiresApi(Build.VERSION_CODES.TIRAMISU)
  private fun requestNearbyWifiDevicesNotGranted() {
    when (ActivityCompat.checkSelfPermission(
        context,
        android.Manifest.permission.NEARBY_WIFI_DEVICES
      )
    ) {
      PackageManager.PERMISSION_GRANTED -> logDebug("Wifi P2P: Nearby wifi devices granted")
      else -> {
        logDebug("Wifi P2P: Requesting Nearby wifi devices granted permission")
        return context.requestPermissions(
          arrayOf(android.Manifest.permission.NEARBY_WIFI_DEVICES),
          nearbyWifiDevicesPermissionRequestInt
        )
      }
    }
  }

  private fun initiatePeerDiscovery(onDeviceFound: OnDeviceFound?) {
    if (ActivityCompat.checkSelfPermission(
        context,
        android.Manifest.permission.ACCESS_FINE_LOCATION
      ) != PackageManager.PERMISSION_GRANTED
    ) {
      return handleAccessFineLocationNotGranted()
    }

    isSearchingDevices = true
    wifiP2pManager.discoverPeers(
      wifiP2pChannel,
      object : WifiP2pManager.ActionListener {
        override fun onSuccess() {
          logDebug("Discovering peers successful")
        }

        override fun onFailure(reason: Int) {
          val exception = Exception("$reason: ${getWifiP2pReason(reason)}")
          onDeviceFound?.failed(exception)
          onSearchingFailed(exception)
        }
      }
    )
    logDebug("Peer discovery initiated")
  }

  private fun requestDeviceInfo(
    onDeviceFound: OnDeviceFound,
    onConnected: DataSharingStrategy.PairingListener
  ) {
    if (wifiP2pChannel == null) {
      initChannel(onDeviceFound = onDeviceFound, onConnected = onConnected)
    }
    wifiP2pChannel?.also { wifiP2pChannel ->
      if (ActivityCompat.checkSelfPermission(
          context,
          android.Manifest.permission.ACCESS_FINE_LOCATION
        ) != PackageManager.PERMISSION_GRANTED
      ) {
        return handleAccessFineLocationNotGranted()
      }

      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
        wifiP2pManager.requestDeviceInfo(wifiP2pChannel) {
          if (it != null && it.status == WifiP2pDevice.CONNECTED) {
            disconnect(
              WifiDirectDevice(it),
              object : DataSharingStrategy.OperationListener {
                override fun onSuccess(device: DeviceInfo?) {
                  Timber.e("Successfully disconnected from Wifi-Direct")
                  initChannelAndPeerDiscovery(
                    onDeviceFound = onDeviceFound,
                    onConnected = onConnected
                  )
                }

                override fun onFailure(device: DeviceInfo?, ex: Exception) {
                  Timber.e(ex, "Failed to disconnect from Wifi-Direct")
                }
              }
            )
          } else {
            logDebug("Wifi-Direct not connected")
            initChannelAndPeerDiscovery(onDeviceFound = onDeviceFound, onConnected = onConnected)
          }
        }
      } else {
        wifiP2pManager.requestConnectionInfo(wifiP2pChannel) { info ->
          if (info != null && info.groupFormed) {
            wifiP2pManager.removeGroup(
              wifiP2pChannel,
              object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                  logDebug("Successfully disconnected from Wifi-Direct")
                  wifiP2pManager.requestConnectionInfo(wifiP2pChannel) {
                    logDebug(
                      "groupformed : ${it.groupFormed} groupOwnerAddress : ${it.groupOwnerAddress}, isGroupOwner : ${it.isGroupOwner}"
                    )
                    initChannelAndPeerDiscovery(
                      onDeviceFound = onDeviceFound,
                      onConnected = onConnected
                    )
                  }
                }

                override fun onFailure(reason: Int) {
                  Timber.e(
                    Exception(getWifiP2pReason(reason)),
                    "Successfully disconnect from Wifi-Direct"
                  )
                }
              }
            )
          } else {
            logDebug("Wifi-Direct not connected")
            initChannelAndPeerDiscovery(onDeviceFound = onDeviceFound, onConnected = onConnected)
          }
        }
      }
    }
  }

  private fun initChannelAndPeerDiscovery(
    onDeviceFound: OnDeviceFound,
    onConnected: DataSharingStrategy.PairingListener
  ) {
    initChannel(onDeviceFound = onDeviceFound, onConnected = onConnected)
    listenForWifiP2pEventsIntents()
    initiatePeerDiscovery(onDeviceFound)
  }

  override fun connect(
    device: DeviceInfo,
    operationListener: DataSharingStrategy.OperationListener
  ) {
    val wifiDirectDevice = device.strategySpecificDevice as WifiP2pDevice

    logDebug("Wifi P2P: Initiating connection to device: ${wifiDirectDevice.deviceName}")
    val wifiP2pConfig = WifiP2pConfig().apply { deviceAddress = wifiDirectDevice.deviceAddress }
    pairingInitiated = true
    logDebug("connect() with pairing inititiated $pairingInitiated")
    wifiP2pChannel?.also { wifiP2pChannel ->
      if (ActivityCompat.checkSelfPermission(
          context,
          android.Manifest.permission.ACCESS_FINE_LOCATION
        ) != PackageManager.PERMISSION_GRANTED
      ) {
        return handleAccessFineLocationNotGranted()
      }

      // NB: This handles changing the last states of everything i.e. paired, pairingInitiated from
      // both onConnectionInfoAvailable implementations
      val successPairingListener = {
        disableConnectionCountdown(pairingTimeout!!)
        Timber.i("Device successfully paired")

        currentDevice = wifiDirectDevice
        paired = true
        pairingInitiated = false
        logDebug("connect() successfully paired with pairing inititiated $pairingInitiated")

        onConnectionSucceeded(device)
      }

      // NB: This handles changing the last states of everything i.e. paired, pairingInitiated from
      // both onConnectionInfoAvailable implementations
      val failedPairingListener = { exception: Exception ->
        paired = false
        pairingInitiated = false

        Timber.e("connect() pairing failure with pairing inititiated $pairingInitiated")

        disableConnectionCountdown(pairingTimeout!!)

        onConnectionFailed(device, exception)
        operationListener.onFailure(device, exception)
      }

      pairingTimeout =
        pairingCountdown {
          successPairingListeners.remove(successPairingListener)
          failedPairingListeners.remove(failedPairingListener)

          wifiP2pManager.cancelConnect(
            wifiP2pChannel,
            object : WifiP2pManager.ActionListener {
              override fun onSuccess() {
                Timber.e("Cancel connection successful")
                operationListener.onFailure(device, Exception("Pairing timeout"))
              }

              override fun onFailure(p0: Int) {
                val ex = Exception("Cancel connect failure $p0")
                Timber.e(ex)
                operationListener.onFailure(device, ex)
              }
            }
          )
        }

      wifiP2pManager.connect(
        wifiP2pChannel,
        wifiP2pConfig,
        object : WifiP2pManager.ActionListener {
          override fun onSuccess() {
            Timber.i("Sending connection request is successful")

            operationListener.onSuccess(device)

            successPairingListeners.add(successPairingListener)
            failedPairingListeners.add(failedPairingListener)
          }

          override fun onFailure(reason: Int) {
            val exception = Exception("Error #$reason: ${getWifiP2pReason(reason)}")
            logDebug("Failed to connect to the other device")
            Timber.e(exception)

            failedPairingListener(exception)
          }
        }
      )
    }
  }

  fun pairingCountdown(onTimeout: () -> Unit): CountDownTimer {
    val countDownTimer =
      object : CountDownTimer(connectionTimeout() * 1000L, 1000L) {
          // https://developer.android.com/reference/android/os/CountDownTimer.html
          override fun onTick(millisUntilFinished: Long) {
            val formattedMillis = NumberFormat.getIntegerInstance().format(millisUntilFinished)
            Timber.i("p2p wifi connection countdown until finished $formattedMillis ms")
          }

          override fun onFinish() {
            onTimeout()
          }
        }
        .start()

    return countDownTimer
  }

  fun disableConnectionCountdown(pairingCountDownTimer: CountDownTimer) {
    pairingCountDownTimer.cancel()
  }

  override fun disconnect(
    device: DeviceInfo,
    operationListener: DataSharingStrategy.OperationListener
  ) {
    requestedDisconnection = true

    if (wifiP2pChannel == null) {
      Timber.i("wifiP2pChannel is null when trying to disconnect")
      requestedDisconnection = false
      operationListener.onSuccess(device)
      return
    }

    wifiP2pManager.removeGroup(
      wifiP2pChannel,
      object : WifiP2pManager.ActionListener {
        override fun onSuccess() {
          logDebug("disconnect successfull")
          paired = false
          onDisconnectSucceeded(device)
          operationListener.onSuccess(device)
        }

        override fun onFailure(reason: Int) {
          logDebug("disconnect failed")
          val exception = Exception("Error #$reason: ${getWifiP2pReason(reason)}")
          onDisconnectFailed(device, exception)
          operationListener.onFailure(device, exception)
        }
      }
    )
  }

  override fun send(
    device: DeviceInfo?,
    syncPayload: PayloadContract<out Any>,
    operationListener: DataSharingStrategy.OperationListener
  ) {
    // Check if the socket is setup for sending
    // Check if this is the sender/receiver
    if (wifiP2pInfo == null) {
      val p2pDevice = (device?.strategySpecificDevice as WifiP2pDevice)
      val errorMsg = "WifiP2PInfo is not available"
      logError(errorMsg)
      operationListener.onFailure(
        device,
        Exception("Error sending to ${p2pDevice.deviceName}(${p2pDevice.deviceAddress}): $errorMsg")
      )
      return
    }
    coroutineScope.launch(dispatcherProvider.io()) {
      val groupOwnerAddress = getGroupOwnerAddress()

      if (groupOwnerAddress == null) {
        Timber.e("groupOwnerAddress is null")
        operationListener.onFailure(
          device,
          Exception("An exception occurred and the groupOwnerAddress is null")
        )

        return@launch
      }

      try {
        makeSocketConnections(groupOwnerAddress) { socket ->
          if (socket != null) {

            when (syncPayload.getDataType()) {
              SyncPayloadType.STRING -> {
                writeStringPayload(syncPayload, operationListener, device)
              }
              SyncPayloadType.BYTES -> {
                writeBytePayload(syncPayload, operationListener, device)
              }
            }
          } else {
            onConnectionInfo =
              fun() {
                send(device, syncPayload, operationListener)
              }

            operationListener.onFailure(
              device,
              Exception("An exception occurred and the socket is null")
            )
          }
        }
      } catch (ex: SocketException) {
        Timber.e(ex)
        operationListener.onFailure(device, ex)
      }
    }
  }

  private fun writeBytePayload(
    syncPayload: PayloadContract<out Any>,
    operationListener: DataSharingStrategy.OperationListener,
    device: DeviceInfo?
  ) {
    (dataOutputStream?.apply {
      val byteArray = syncPayload.getData() as ByteArray

      writeUTF(SyncPayloadType.BYTES.name)
      writeLong(byteArray.size.toLong())

      val len = byteArray.size
      var offset = 0
      var chunkSize = 1024

      while (offset < len) {
        if (chunkSize > len) {
          chunkSize = len
        }
        write(byteArray, offset, chunkSize)

        offset += chunkSize
        if ((len - offset) < chunkSize) {
          chunkSize = len - offset
        }
      }

      operationListener.onSuccess(device)
    }
      ?: run { operationListener.onFailure(device, Exception("DataOutputStream is null")) })
  }

  private fun writeStringPayload(
    syncPayload: PayloadContract<out Any>,
    operationListener: DataSharingStrategy.OperationListener,
    device: DeviceInfo?
  ) {
    if (dataOutputStream != null) {
      dataOutputStream?.apply {
        writeUTF(SyncPayloadType.STRING.name)
        flush()

        writeUTF(syncPayload.getData() as String)
        flush()

        operationListener.onSuccess(device)
      }
    } else {
      operationListener.onFailure(device, Exception("DataOutputStream is null"))
    }
  }

  private fun getGroupOwnerAddress(): String? {
    // This also causes a crash on the app
    return wifiP2pInfo?.groupOwnerAddress?.hostAddress
  }

  suspend fun makeSocketConnections(
    groupOwnerAddress: String,
    onSocketConnectionMade: (socket: Socket?) -> Unit
  ) {
    val socketResult: Socket?
    Timber.e("makeSocketConnections")
    if (socket != null) {
      socketResult = socket
    } else if (wifiP2pInfo == null) {
      logDebug("makeSocketConnections wifip2pInfo is null and socket is being set to null")
      // Request connections
      requestConnectionInfo()

      socketResult = null
      socket = socketResult
    } else if (wifiP2pInfo?.isGroupOwner == true) {
      // Start a server to accept connections.
      logDebug("Accepting connections")
      socketResult = acceptConnectionsToServerSocket()
      socket = socketResult
    } else {
      logDebug("Making connection to server")
      // Connect to the server running on the group owner device.
      socketResult = connectToServerSocket(groupOwnerAddress)
      socket = socketResult
    }

    onSocketConnectionMade.invoke(socketResult)
  }

  private suspend fun acceptConnectionsToServerSocket(): Socket? =
    withContext(dispatcherProvider.io()) {
      try {
        if (serverSocket == null) {
          logDebug("Server socket is already initialised")
          serverSocket = ServerSocket(PORT)
        }

        serverSocket!!.soTimeout = connectionTimeout() * 1000
        serverSocket!!.accept().apply { constructStreamsFromSocket(this) }
      } catch (e: Exception) {
        Timber.e(e)
        null
      }
    }

  private fun constructStreamsFromSocket(socket: Socket) {
    dataInputStream = DataInputStream(socket.getInputStream())
    dataOutputStream = DataOutputStream(socket.getOutputStream())
  }

  private suspend fun connectToServerSocket(groupOwnerAddress: String): Socket? =
    withContext(dispatcherProvider.io()) {
      var retryCount = CONNECT_TO_SERVER_RETRIES
      var clientSocket: Socket? = null

      while (retryCount > 0) {
        try {
          clientSocket =
            Socket().apply {
              bind(null)
              connect(InetSocketAddress(groupOwnerAddress, PORT), SOCKET_TIMEOUT)
              constructStreamsFromSocket(this)
            }
          break
        } catch (e: Exception) {
          Timber.e(e)
          clientSocket = null
          delay(CONNECT_TO_SERVER_TIMEOUT)
        }

        retryCount--
      }

      clientSocket
    }

  override fun sendManifest(
    device: DeviceInfo?,
    manifest: Manifest,
    operationListener: DataSharingStrategy.OperationListener
  ) {
    try {
      dataOutputStream?.apply {
        val manifestString = Gson().toJson(manifest)
        writeUTF(MANIFEST)
        writeUTF(manifestString)
        flush()
        operationListener.onSuccess(device = device)
      }
    } catch (ex: SocketException) {
      Timber.e("Sending the manifest failed", ex)
      operationListener.onFailure(device, ex)
    }
  }

  override fun receive(
    device: DeviceInfo?,
    payloadReceiptListener: DataSharingStrategy.PayloadReceiptListener,
    operationListener: DataSharingStrategy.OperationListener
  ) {
    // Check if the socket is setup for listening
    // Check if this is the receiver/sender

    if (wifiP2pInfo == null) {
      val p2pDevice = (device?.strategySpecificDevice as WifiP2pDevice)
      val errorMsg = "WifiP2PInfo is not available"
      logError(errorMsg)
      operationListener.onFailure(
        device,
        Exception(
          "Error receiving from ${p2pDevice.deviceName}(${p2pDevice.deviceAddress}): $errorMsg"
        )
      )
    }

    coroutineScope.launch(dispatcherProvider.io()) {
      val groupOwnerAddress = getGroupOwnerAddress()
      if (groupOwnerAddress == null) {
        operationListener.onFailure(device, Exception("Socket is null"))
        return@launch
      }

      try {
        makeSocketConnections(groupOwnerAddress) { socket ->
          if (socket != null) {
            dataInputStream?.run {
              val dataType = readUTF()

              if (dataType == SyncPayloadType.STRING.name) {
                val stringPayload = readUTF()
                payloadReceiptListener.onPayloadReceived(StringPayload(stringPayload))
              } else if (dataType == SyncPayloadType.BYTES.name) {
                var payloadLen = readLong()
                val payloadByteArray = ByteArray(payloadLen.toInt())
                var currentBufferPos = 0
                var n = 0

                while (payloadLen > 0 &&
                  read(payloadByteArray, currentBufferPos, Math.min(1024, payloadLen).toInt())
                    .also { n = it } != -1) {

                  currentBufferPos += n
                  payloadLen -= n.toLong()
                  logDebug("file size $payloadLen")
                }
                payloadReceiptListener.onPayloadReceived(BytePayload(payloadByteArray))
              } else {
                operationListener.onFailure(
                  getCurrentDevice(),
                  Exception("Unknown datatype: $dataType")
                )
              }
            }
          } else {
            operationListener.onFailure(device, Exception("Socket is null"))
          }
        }
      } catch (e: Exception) {
        operationListener.onFailure(device, e)
      }
    }
  }

  override fun receiveManifest(
    device: DeviceInfo,
    operationListener: DataSharingStrategy.OperationListener
  ): Manifest? {
    // Check if the socket is setup for listening
    // Check if this is the receiver/sender

    return dataInputStream?.run {
      try {
        val dataType = readUTF()

        if (dataType == MANIFEST) {
          val manifestString = readUTF()
          operationListener.onSuccess(device)
          Gson().fromJson(manifestString, Manifest::class.java)
        } else {
          operationListener.onFailure(device, Exception("dataType of manifest is $dataType"))
          null
        }
      } catch (ex: SocketException) {
        Timber.e("Receiving manifest failed", ex)
        operationListener.onFailure(device, ex)
        null
      }
    }
  }

  override fun onErrorOccurred(ex: Exception) {
    closeSocketAndStreams()
  }

  override fun onConnectionFailed(device: DeviceInfo, ex: Exception) {
    closeSocketAndStreams()
  }

  override fun onConnectionSucceeded(device: DeviceInfo) {
    // TODO: Return this to the device
  }

  override fun onDisconnectFailed(device: DeviceInfo, ex: Exception) {
    closeSocketAndStreams()
  }

  override fun onDisconnectSucceeded(device: DeviceInfo) {
    closeSocketAndStreams()
  }

  override fun onPairingFailed(ex: Exception) {
    // TODO: Return this to the device
  }

  override fun onPairingSucceeded() {
    for (successPairingListener in successPairingListeners) {
      successPairingListener.invoke()
    }
  }

  override fun onSendingFailed(ex: Exception) {
    // TODO: Return this to the device
    // Also show an error on the UI
  }

  override fun onSearchingFailed(ex: Exception) {
    // TODO: Return this to the device
  }

  override fun getCurrentDevice(): DeviceInfo? {
    return if (currentDevice != null) {
      WifiDirectDevice(currentDevice!!)
    } else {
      null
    }
  }

  override fun onResume(isScanning: Boolean) {
    if (isScanning) {
      listenForWifiP2pEventsIntents()
      initiatePeerDiscoveryOnceAccessFineLocationGranted()
      requestConnectionInfo()
    }
  }

  override fun onPause() {
    runCatching {
      wifiP2pReceiver?.also {
        context.unregisterReceiver(it)
        // TODO: Sample fix memory leak
        wifiP2pReceiver = null
      }
    }
      .onFailure {
        Timber.e(it)
        wifiP2pReceiver = null
      }

    closeSocketAndStreams()
  }

  override fun initChannel(
    onDeviceFound: OnDeviceFound,
    onConnected: DataSharingStrategy.PairingListener
  ) {
    if (wifiP2pChannel == null) {
      wifiP2pChannel = wifiP2pManager.initialize(context, context.mainLooper, null)
      wifiP2pChannel?.also { channel ->
        wifiP2pReceiver =
          WifiP2pBroadcastReceiver(
            wifiP2pManager,
            channel,
            object : P2PManagerListener {
              override fun handleWifiP2pDisabled() {
                this@WifiDirectDataSharingStrategy.handleWifiP2pDisabled()
              }

              override fun handleWifiP2pEnabled() {
                this@WifiDirectDataSharingStrategy.handleWifiP2pEnabled()
              }

              override fun handleUnexpectedWifiP2pState(wifiState: Int) {
                this@WifiDirectDataSharingStrategy.handleUnexpectedWifiP2pState(wifiState)
              }

              override fun handleWifiP2pDevice(device: WifiP2pDevice) {
                this@WifiDirectDataSharingStrategy.handleWifiP2pDevice(device)
              }

              override fun handleP2pDiscoveryStarted() {
                this@WifiDirectDataSharingStrategy.handleP2pDiscoveryStarted()
              }

              override fun handleP2pDiscoveryStopped() {
                this@WifiDirectDataSharingStrategy.handleP2pDiscoveryStopped()
              }

              override fun handleUnexpectedWifiP2pDiscoveryState(discoveryState: Int) {
                this@WifiDirectDataSharingStrategy.handleUnexpectedWifiP2pDiscoveryState(
                  discoveryState
                )
              }

              override fun handleP2pPeersChanged(peerDeviceList: WifiP2pDeviceList) {
                val devicesList = peerDeviceList.deviceList.map { WifiDirectDevice(it) }
                onDeviceFound.deviceFound(devicesList)

                this@WifiDirectDataSharingStrategy.handleP2pPeersChanged(peerDeviceList)
              }

              override fun handleAccessFineLocationNotGranted() {
                this@WifiDirectDataSharingStrategy.handleAccessFineLocationNotGranted()
              }

              override fun handleMinimumSDKVersionNotMet(minimumSdkVersion: Int) {
                this@WifiDirectDataSharingStrategy.handleMinimumSDKVersionNotMet(minimumSdkVersion)
              }

              override fun onConnectionInfoAvailable(
                info: WifiP2pInfo,
                wifiP2pGroup: WifiP2pGroup?
              ) {

                Timber.e("onConnectionInfoAvailable() WIFIp2p Group is $wifiP2pGroup")
                Timber.e("onConnectionInfoAvailable() WIFIp2p Info is $info")
                Timber.e("onConnectionInfoAvailable() Paired: $paired")
                Timber.e(
                  "onConnectionInfoAvailable() Requested disconnection: $requestedDisconnection"
                )

                if (info.groupFormed) {
                  // This is handled onConnectionInfoAvailable inside WifiDirectDataSharingStrategy
                  // paired = true

                  onConnected.onSuccess(null)
                } else {

                  if (paired) {
                    closeSocketAndStreams()
                    // TODO: Check whether this is disconnection or failed
                    if (!requestedDisconnection) {
                      onConnected.onDisconnected()
                    }

                    // This is handled onConnectionInfoAvailable inside
                    // WifiDirectDataSharingStrategy
                    // paired = false
                  }
                  requestedDisconnection = false
                }

                this@WifiDirectDataSharingStrategy.onConnectionInfoAvailable(info, wifiP2pGroup)
              }

              override fun onConnectionInfoAvailable(info: WifiP2pInfo) {
                this@WifiDirectDataSharingStrategy.onConnectionInfoAvailable(info, null)
              }
            },
            context
          )
      }
    }
  }

  fun logDebug(message: String) {
    Timber.d(message)
  }

  private fun logError(message: String) {
    Timber.e(message)
  }

  override fun handleWifiP2pDisabled() {
    // TODO: Handle the issue here
  }

  override fun handleWifiP2pEnabled() {
    // TODO: Handle the issue here
  }

  override fun handleUnexpectedWifiP2pState(wifiState: Int) {
    // TODO: Handle the issue here
    // Also show an error on the UI
  }

  override fun handleWifiP2pDevice(device: WifiP2pDevice) {
    if (device != null) {
      currentDevice = device
    }
  }

  override fun handleP2pDiscoveryStarted() {
    // TODO: Handle the issue here
  }

  override fun handleP2pDiscoveryStopped() {
    // TODO: Handle the issue here
  }

  override fun handleUnexpectedWifiP2pDiscoveryState(discoveryState: Int) {
    // TODO: Handle the issue here
  }

  override fun handleP2pPeersChanged(peerDeviceList: WifiP2pDeviceList) {
    // TODO: Handle the issue here
  }

  override fun handleAccessFineLocationNotGranted() {
    // TODO: Handle the issue here
  }

  override fun handleMinimumSDKVersionNotMet(minimumSdkVersion: Int) {
    // TODO: Handle the issue here
  }

  override fun onConnectionInfoAvailable(info: WifiP2pInfo, wifiP2pGroup: WifiP2pGroup?) {
    if (info == null) {
      logError("Connection info provided is NULL")
      return
    }

    val message =
      "Connection info available: groupFormed = ${info.groupFormed}, isGroupOwner = ${info.isGroupOwner}"
    logDebug(message)
    wifiP2pInfo = info

    if (info.groupFormed && wifiP2pGroup != null) {
      this.wifiP2pGroup = wifiP2pGroup

      if (info.isGroupOwner) {
        val isGroupOwner = info.isGroupOwner
        currentDevice = wifiP2pGroup.clientList.firstOrNull { it.isGroupOwner != isGroupOwner }
      }

      if (pairingInitiated) {
        onPairingSucceeded()
      }
    }

    if (onConnectionInfo != null) {
      onConnectionInfo?.invoke()
      onConnectionInfo = null
    }
  }

  override fun onConnectionInfoAvailable(info: WifiP2pInfo) {
    this.onConnectionInfoAvailable(info, null)
  }

  override fun stopSearchingDevices(operationListener: DataSharingStrategy.OperationListener?) {
    if (isSearchingDevices) {
      wifiP2pManager.stopPeerDiscovery(
        wifiP2pChannel,
        object : WifiP2pManager.ActionListener {
          override fun onSuccess() {
            logDebug("Successfully stopped peer discovery")
            operationListener?.onSuccess(null)

            if (!paired) {
              // Set this to null so that wifi direct is turned on during on resume
              // Fixes https://github.com/opensrp/fhircore/issues/1960#issuecomment-1427424971
              wifiP2pChannel = null
            }
          }

          override fun onFailure(reason: Int) {
            val ex =
              Exception("Error occurred trying to stop peer discovery ${getWifiP2pReason(reason)}")
            Timber.e(ex)
            operationListener?.onFailure(null, ex)

            if (!paired) {
              // Set this to null so that wifi direct is turned on during on resume
              // Fixes https://github.com/opensrp/fhircore/issues/1960#issuecomment-1427424971
              wifiP2pChannel = null
            }
          }
        }
      )
      isSearchingDevices = false
    } else {

      if (!paired) {
        // Set this to null so that wifi direct is turned on during on resume
        // Fixes https://github.com/opensrp/fhircore/issues/1960#issuecomment-1427424971
        wifiP2pChannel = null
      }
    }
  }

  fun closeSocketAndStreams() {
    stopSearchingDevices(null)

    dataInputStream?.run { close() }

    dataOutputStream?.run {
      flush()
      close()
    }

    if (socket != null) {
      try {
        socket!!.close()
      } catch (e: IOException) {
        Timber.e(e)
      }
      socket = null
    }

    if (serverSocket != null) {
      try {
        serverSocket!!.close()
      } catch (e: IOException) {
        Timber.e(e)
      }
      serverSocket = null
    }
  }

  fun getWifiP2pReason(reasonInt: Int): String =
    when (reasonInt) {
      0 -> "Error"
      1 -> "Unsupported"
      2 -> "Busy"
      else -> "Unknown"
    }

  override fun isPairingInitiated(): Boolean {
    return this.pairingInitiated
  }

  class WifiDirectDevice(var wifiP2pDevice: WifiP2pDevice) : DeviceInfo {

    override var strategySpecificDevice: Any
      get() = wifiP2pDevice
      set(value) {
        wifiP2pDevice = value as WifiP2pDevice
      }

    override fun getDisplayName(): String =
      "${wifiP2pDevice.deviceName} (${wifiP2pDevice.deviceAddress})"

    override fun name(): String {
      return wifiP2pDevice.deviceName
    }

    override fun address(): String {
      return wifiP2pDevice.deviceAddress
    }
  }

  override fun onStop() {
    if (paired) {
      requestedDisconnection = true
      wifiP2pManager.removeGroup(
        wifiP2pChannel,
        object : WifiP2pManager.ActionListener {
          override fun onSuccess() {
            Timber.i("Device successfully disconnected")
            paired = false

            // Set this to null so that wifi direct is turned on during on resume
            // Fixes https://github.com/opensrp/fhircore/issues/1960#issuecomment-1427424971
            wifiP2pChannel = null
          }

          override fun onFailure(reason: Int) {
            val exception = Exception("Error #$reason: ${getWifiP2pReason(reason)}")
            Timber.e(exception)

            // Set this to null so that wifi direct is turned on during on resume
            // Fixes https://github.com/opensrp/fhircore/issues/1960#issuecomment-1427424971
            wifiP2pChannel = null
          }
        }
      )
    }
  }

  override fun cleanup() {
    closeSocketAndStreams()
  }

  companion object {

    const val CONNECT_TO_SERVER_RETRIES = 3
    const val CONNECT_TO_SERVER_TIMEOUT = 5000L
  }
}
