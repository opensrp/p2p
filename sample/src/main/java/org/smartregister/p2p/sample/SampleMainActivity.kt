package org.smartregister.p2p.sample

import androidx.appcompat.app.AppCompatActivity
import android.os.Bundle
import android.widget.Button
import java.util.TreeSet
import org.json.JSONArray
import org.smartregister.p2p.P2PLibrary
import org.smartregister.p2p.dao.ReceiverTransferDao
import org.smartregister.p2p.dao.SenderTransferDao
import org.smartregister.p2p.model.RecordCount
import org.smartregister.p2p.search.data.JsonData
import org.smartregister.p2p.sync.DataType
import org.smartregister.p2p.utils.startP2PScreen

class SampleMainActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_sample_main)

        // Init P2PLibrary
        val p2POptions = P2PLibrary.Options(context = this,
            dbPassphrase = "demo",
            username = "demo",
            object: SenderTransferDao {
                override fun getP2PDataTypes(): TreeSet<DataType> {
                    return TreeSet<DataType>(listOf(DataType(name = "Patient", DataType.Filetype.JSON, 0)))
                }

                override fun getTotalRecordCount(highestRecordIdMap: HashMap<String, Long>): RecordCount {
                    return RecordCount()
                }

                override fun getJsonData(
                    dataType: DataType,
                    lastRecordId: Long,
                    batchSize: Int,
                    offset: Int
                ): JsonData? {
                    return null
                }
            }, object: ReceiverTransferDao {
                override fun getP2PDataTypes(): TreeSet<DataType> {
                    return TreeSet<DataType>(listOf(DataType(name = "Patient", DataType.Filetype.JSON, 0)))
                }

                override fun receiveJson(type: DataType, jsonArray: JSONArray): Long {
                    return 0L
                }
            }
        )
        P2PLibrary.init(p2POptions)

        findViewById<Button>(R.id.device_sync).setOnClickListener {
            startP2PScreen(this)
        }
    }
}