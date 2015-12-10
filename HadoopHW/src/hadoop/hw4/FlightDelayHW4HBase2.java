
package hadoop.hw4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDelayHW4HBase2 {
    private static final String HTABLE_NAME = "Flight";
    private static final String HTABLE_FAMILY = "content";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf);
        job.setJarByClass(FlightDelayHW4HBase2.class);
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * A mapper class to load data from HBase.
     */
    public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputVal = new Text();
        private int num = 0;
        private double sum = 0;
        private Configuration config;
        private HTable table;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            config = HBaseConfiguration.create();
            table = new HTable(config, HTABLE_NAME);
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // scans the table with a prefix, the prefixes are store in files
            // the prefixes are something like '200801', '200802'...
            byte[] prefix = Bytes.toBytes(value.toString());
            Scan scan = new Scan(prefix);
            PrefixFilter prefixFilter = new PrefixFilter(prefix);
            scan.setFilter(prefixFilter);
            ResultScanner scanner = table.getScanner(scan);
            String carrier = null;
            String month = null;
            try {
                for (Result r = scanner.next(); r != null; r = scanner.next()) {
                    byte[] delayBytes = r.getValue(Bytes.toBytes(HTABLE_FAMILY), Bytes
                            .toBytes("ArrDelayMinutes"));
                    byte[] carrierBytes = r.getValue(Bytes.toBytes(HTABLE_FAMILY),
                            Bytes.toBytes("UniqueCarrier"));
                    if (carrierBytes == null) {
                        continue;
                    }
                    if (carrier != null
                            && !carrier.equals(Bytes.toString(carrierBytes))) {
                        outputKey.set(carrier);
                        outputVal.set("(" + month + ", " + Math.ceil(sum / num) + ")");
                        context.write(outputKey, outputVal);
                        sum = 0;
                        num = 0;
                    }
                    num++;
                    if (delayBytes != null) {
                        sum += Double.parseDouble(Bytes.toString(delayBytes));
                    }
                    carrier = Bytes.toString(r.getValue(Bytes.toBytes(HTABLE_FAMILY),
                            Bytes.toBytes("UniqueCarrier")));
                    month = Bytes.toString(r.getValue(Bytes.toBytes(HTABLE_FAMILY),
                            Bytes.toBytes("Month")));
                }
                if (carrier != null) {
                    outputKey.set(carrier);
                    outputVal.set("(" + month + ", " + Math.ceil(sum / num) + ")");
                    context.write(outputKey, outputVal);
                    sum = 0;
                    num = 0;
                }
            } finally {
                scanner.close();
            }
        }
    }

    /**
     * The reducer class to concatenate the values of each key.
     */
    public static class FlightReducer extends
            Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            StringBuilder sb = new StringBuilder();
            for (Text t : values) {
                list.add(t.toString());
            }
            Collections.sort(list, new Comparator<String>() {

                @Override
                public int compare(String s1, String s2) {
                    int a = Integer.parseInt(s1.split(",")[0].substring(1));
                    int b = Integer.parseInt(s2.split(",")[0].substring(1));
                    return a - b;
                }
            });
            for (String str : list) {
                sb.append(", ").append(str);
            }
            if (sb.length() > 2) {
                result.set(sb.toString().substring(2));
            }
            context.write(key, result);
        }
    }
}
