package demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

/**
 * create by liuzhiwei on 2020/3/23
 */
public class SimpleClient {
    public static void main(String[] args) throws Exception {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("10.0.13.107", 11111), "example", "", "");

        connector.connect();
        // 开启订阅日志
        connector.subscribe(".*\\..*");
        connector.rollback();

        while (true) {
            // 每次读取 100 条
            Message message = connector.getWithoutAck(100);
            long batchId = message.getId();
            if (batchId == -1 || message.getEntries().isEmpty()) {
                System.out.println("sleep");
                Thread.sleep(1000);
                continue;
            }
            printEntries(message.getEntries());
            connector.ack(batchId);
        }
    }

    private static void printEntries(List<Entry> entries) throws Exception {
        for (Entry entry : entries) {
            if (entry.getEntryType() != EntryType.ROWDATA) {
                continue;
            }

            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            for (RowData rowData : rowChange.getRowDatasList()) {
                switch (rowChange.getEventType()) {
                    case INSERT:
                    case UPDATE:
                        System.out.print("UPSERT ");
                        printColumns(rowData.getAfterColumnsList());

                        if ("retl_buffer".equals(entry.getHeader().getTableName())) {
                            String tableName = rowData.getAfterColumns(1).getValue();
                            String pkValue = rowData.getAfterColumns(2).getValue();
                            System.out.println("SELECT * FROM " + tableName + " WHERE id = " + pkValue);
                        }
                        break;

                    case DELETE:
                        System.out.print("DELETE ");
                        printColumns(rowData.getBeforeColumnsList());
                        break;

                    default:
                        break;
                }
            }
        }
    }

    private static void printColumns(List<Column> columns) {
        String line = columns.stream()
                .map(column -> column.getName() + "=" + column.getValue())
                .collect(Collectors.joining(","));
        System.out.println(line);
    }
}
