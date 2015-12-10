
package hadoop.hw4;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVReader;

public class FlightDelayHW4 {

    public static void main(String[] args) throws Exception {
        // configures job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flight delay");
        job.setJarByClass(FlightDelayHW4.class);
        job.setMapperClass(FlightMapper.class);
        job.setPartitionerClass(FlightPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(CarrierGroupingComparator.class);
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputVal = new Text();
        private String year = null;
        private String month = null;
        private String carrier = null;
        private String arrDelayMinutes = null;
        private String outputKeyStr = null;
        private String outputValStr = null;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                year = nextLine[0];
                // skips the first line
                if (year.equals("Year")) {
                    continue;
                }
                // checks year
                if (!year.equals("2008")) {
                    continue;
                }
                month = nextLine[2];
                carrier = nextLine[6];
                arrDelayMinutes = nextLine[37];
                // checks if arrDelayMinutes is not missing
                if (arrDelayMinutes.equals("")) {
                    continue;
                }
                // constructs key and value
                outputKeyStr = carrier + "," + month;
                outputValStr = arrDelayMinutes;
                outputKey.set(outputKeyStr);
                outputVal.set(outputValStr);
                context.write(outputKey, outputVal);
            }
        }
    }

    /**
     * A partitioner for carrier names.
     */
    public static class FlightPartitioner extends Partitioner<Text, Text> {
        
        @Override
        public int getPartition(Text key, Text val, int numPartitions) {
            return key.toString().split(",")[0].hashCode() % numPartitions;
        }
    }

    /**
     * A comparator for sorting in Mappers.
     */
    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            int compare = t1.toString().split(",")[0].compareTo(t2.toString().split(",")[0]);
            if (compare == 0) {
                compare = Integer.parseInt(t1.toString().split(",")[1])
                        - Integer.parseInt(t2.toString().split(",")[1]);
            }
            return compare;
        }
    }

    /**
     * A grouping comparator for grouping by carriers. 
     */
    public static class CarrierGroupingComparator extends WritableComparator {
        public CarrierGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            return t1.toString().split(",")[0].compareTo(t2.toString().split(",")[0]);
        }
    }

    public static class FlightReducer extends
            Reducer<Text, Text, Text, Text> {

        private Text resultKey = new Text();
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int num = 0;
            double sum = 0;
            StringBuilder sb = new StringBuilder();
            String preMonth = null;
            // accumulates the delay time for each month.
            for (Text val : values) {
                if (preMonth != null && !preMonth.equals(key.toString().split(",")[1])) {
                    sb.append("(").append(preMonth).append(",").append(Math.ceil(sum / num))
                            .append(")").append(", ");
                    sum = 0;
                    num = 0;
                }
                num++;
                sum += Double.parseDouble(val.toString());
                preMonth = key.toString().split(",")[1];
            }
            if (preMonth != null) {
                sb.append("(").append(preMonth).append(",").append(Math.ceil(sum / num))
                        .append(")");
                sum = 0;
                num = 0;
            }
            String carrier = key.toString().split(",")[0];
            resultKey.set(carrier);
            result.set(sb.toString());
            context.write(resultKey, result);
        }
    }
}
