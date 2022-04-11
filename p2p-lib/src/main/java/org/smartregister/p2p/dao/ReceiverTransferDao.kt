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
package org.smartregister.p2p.dao

import androidx.annotation.NonNull
import androidx.annotation.WorkerThread
import java.util.TreeSet
import org.json.JSONArray
import org.smartregister.p2p.sync.DataType

interface ReceiverTransferDao {

  @WorkerThread fun getP2PDataTypes(): TreeSet<DataType>

  @WorkerThread fun receiveJson(@NonNull type: DataType, @NonNull jsonArray: JSONArray): Long
}