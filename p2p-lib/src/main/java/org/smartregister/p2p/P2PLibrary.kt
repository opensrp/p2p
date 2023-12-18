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
package org.smartregister.p2p

import android.content.Context
import androidx.annotation.NonNull
import java.util.UUID
import org.smartregister.p2p.dao.ReceiverTransferDao
import org.smartregister.p2p.dao.SenderTransferDao
import org.smartregister.p2p.data_sharing.DataSharingStrategy
import org.smartregister.p2p.data_sharing.WifiDirectDataSharingStrategy
import org.smartregister.p2p.model.AppDatabase
import org.smartregister.p2p.utils.Constants
import org.smartregister.p2p.utils.Settings
import org.smartregister.p2p.utils.isAppDebuggable
import timber.log.Timber

/** Created by Ephraim Kigamba - nek.eam@gmail.com on 14-03-2022. */
class P2PLibrary private constructor() {

  private lateinit var options: Options
  private var hashKey: String? = null
  private var deviceUniqueIdentifier: String? = null
  var dataSharingStrategy: DataSharingStrategy? = null

  companion object {
    private var instance: P2PLibrary? = null

    fun getInstance(): P2PLibrary {
      checkNotNull(instance) {
        ("Instance does not exist!!! Call P2PLibrary.init(P2PLibrary.Options) method " +
          "in the onCreate method of " +
          "your Application class ")
      }
      if (instance!!.dataSharingStrategy == null) {
        instance!!.dataSharingStrategy = WifiDirectDataSharingStrategy()
      }
      return instance!!
    }

    fun init(options: Options): P2PLibrary {
      instance = P2PLibrary(options)
      return instance!!
    }
  }

  private constructor(options: Options) : this() {
    this.options = options

    // We should not override the host applications Timber trees
    if (Timber.treeCount == 0 && isAppDebuggable(options.context)) {
      Timber.plant(Timber.DebugTree())
    }

    hashKey = getHashKey()

    // Start the DB
    AppDatabase.getInstance(getContext(), options.dbPassphrase)
  }

  fun getDb(): AppDatabase {
    return AppDatabase.getInstance(getContext(), options.dbPassphrase)
  }

  fun clean() {
    dataSharingStrategy = null
  }

  fun getHashKey(): String? {
    if (hashKey == null) {
      val settings = Settings(getContext())
      hashKey = settings.hashKey
      if (hashKey == null) {
        hashKey = generateHashKey()
        settings.saveHashKey(hashKey)
      }
    }
    return hashKey
  }

  private fun generateHashKey(): String {
    return UUID.randomUUID().toString()
  }

  fun setDeviceUniqueIdentifier(@NonNull deviceUniqueIdentifier: String?) {
    this.deviceUniqueIdentifier = deviceUniqueIdentifier
  }

  fun getDeviceUniqueIdentifier(): String? {
    return deviceUniqueIdentifier
  }

  fun getUsername(): String {
    return options.username
  }

  fun getContext(): Context {
    return options.context
  }

  fun getBatchSize(): Int {
    return options.batchSize
  }

  fun getSenderTransferDao(): SenderTransferDao {
    return options.senderTransferDao
  }

  fun getReceiverTransferDao(): ReceiverTransferDao {
    return options.receiverTransferDao
  }

  /** [P2PLibrary] configurability options an */
  class Options(
    val context: Context,
    val dbPassphrase: String,
    val username: String,
    val senderTransferDao: SenderTransferDao,
    val receiverTransferDao: ReceiverTransferDao
  ) {
    var batchSize: Int = Constants.DEFAULT_SHARE_BATCH_SIZE
  }
}
