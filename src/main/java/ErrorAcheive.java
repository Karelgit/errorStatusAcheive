import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
/**
 * Created by root on 16-4-19.
 */
public class ErrorAcheive {
    private static Configuration config=null;
    static {
        config=HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","90.90.90.5");
    }

    public static void rowfilter(String tablename) {
        try {
            HTable table = new HTable(config, tablename);

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("9456eb982623472c87f96125268127af"));

            Scan s = new Scan();
            s.setFilter(filter1);
            ResultScanner rs = table.getScanner(s);

            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列族:" + new String(keyValue.getFamily())
                            + " 列:" + new String(keyValue.getQualifier()) + ":"
                            + new String(keyValue.getValue()));
                }
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }

    }

    public static void query(){
        Scan scan = new Scan();
        ResultScanner rs = null;
        try {
            HTable table = new HTable(config, Bytes.toBytes("gengyun.sparkcrawl.errorstatuscode"));
            rs = table.getScanner(scan);
            int i = 0;
            for (Result r : rs) {
                i++;
                for(Cell cell:r.rawCells()){
                    String s = Bytes.toString(CellUtil.cloneRow(cell));
                    String s1 = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String s2 = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("row:" + s);
                    System.out.println("row:" + s1);
                    System.out.println("row:" + s2);
                }
                /*for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }*/
                System.out.println(i);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
    }

    public static void main(String[] args) {
//        query();
        rowfilter("gengyun.sparkcrawl.errorstatuscode");
    }
}
