package it.kenn.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

public class HBaseSource extends RichSourceFunction<Tuple2<String,String>> {
    private Connection connection;
    private Table table;
    private Scan scan;

    public void run(SourceContext sourceContext) throws Exception {
        ResultScanner scanner = table.getScanner(new Scan());
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            for (Cell cell: result.listCells()){
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            Tuple2<String, String> tuple2 = new Tuple2<>();
            tuple2.setFields(rowkey, valueString);
            sourceContext.collect(tuple2);
        }
    }

    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(configuration);
        table = connection.getTable(TableName.valueOf("default:click_event"));
        scan = new Scan();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}
