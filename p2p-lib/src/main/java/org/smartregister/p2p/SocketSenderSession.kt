package org.smartregister.p2p

import android.util.Log
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import java.net.Socket

class SocketSenderSession(private val socket: Socket): SenderSession {
  override fun send() {
    val writer = socket.getOutputStream().bufferedWriter()
    val encoded = Json.encodeToString(SyncPayload("Hello"))
    writer.write(encoded)
    writer.flush()
    Log.d(this::class.simpleName, """Message sent: $encoded""")
    socket.close()
  }
}

interface SenderSession {
  fun send()
}