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
package org.smartregister.p2p

import android.util.Log
import java.net.Socket
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.smartregister.p2p.payload.StringPayload

class SocketSenderSession(private val socket: Socket) : SenderSession {
  override fun send() {
    val writer = socket.getOutputStream().bufferedWriter()
    val encoded = Json.encodeToString(StringPayload("Hello"))
    writer.write(encoded)
    writer.flush()
    Log.d(this::class.simpleName, """Message sent: $encoded""")
    socket.close()
  }
}

interface SenderSession {
  fun send()
}
