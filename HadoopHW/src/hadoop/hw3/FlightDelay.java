package hadoop.hw3;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVReader;

public class FlightDelay {

    public static void main(String[] args) throws Exception {
        // job1
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flight delay");
        job.setJarByClass(FlightDelay.class);
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "_intermediate"));
//        FileInputFormat
//                .addInputPath(
//                        job,
//                        new Path(
//                                "/Users/Sam/Downloads/data.csv"));
//        FileOutputFormat.setOutputPath(job, new Path("intermediateOutput"));
        job.waitForCompletion(true);

        // job2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "calculate avg");
        job2.setJarByClass(FlightDelay.class);
        job2.setMapperClass(AvgMapper.class);
        job2.setReducerClass(AvgReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job2, new Path("intermediateOutput"));
//        FileOutputFormat.setOutputPath(job2, new Path("output"));
        FileInputFormat.addInputPath(job2, new Path(args[1] + "_intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    public static class FlightMapper extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputVal = new Text();

        private String year = null;
        private String month = null;
        private String flightDate = null;
        private String origin = null;
        private String dest = null;
        private String depTime = null;
        private String arrTime = null;
        private String arrDelayMinutes = null;
        private String isCancelled = null;
        private String isDiverted = null;
        private String outputKeyStr = null;
        private String outputValStr = null;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // System.out.println("year: " + nextLine[0] + ". month: " +
                // nextLine[2]
                // + ". origin: " + nextLine[11] + ". dest: " + nextLine[17]);
                // for (String s : nextLine) {
                // System.out.print(s + " ");
                // }
                year = nextLine[0];
                // skips the first line
                if (year.equals("Year")) {
                    continue;
                }
                month = nextLine[2];
                // filters by year and month
//                if ((Integer.parseInt(year) != 2007 || Integer.parseInt(month) != 12)
//                        && (Integer.parseInt(year) != 2008 || Integer
//                                .parseInt(month) != 1)) {
//                    continue;
//                }
                if (!(Integer.parseInt(year) == 2007 && Integer.parseInt(month) >= 6)
                        && !(Integer.parseInt(year) == 2008 && Integer.parseInt(month) <= 5)) {
                    continue;
                }
                flightDate = nextLine[5];
                origin = nextLine[11];
                dest = nextLine[17];
                // filters by origin and dest
                if (!origin.equals("ORD") && !dest.equals("JFK")) {
                    continue;
                }
                if (origin.equals("ORD") && dest.equals("JFK")) {
                    continue;
                }
                depTime = nextLine[24];
                arrTime = nextLine[35];
                arrDelayMinutes = nextLine[37];
                isCancelled = nextLine[41];
                isDiverted = nextLine[43];
                // filters by isCancelled and isDiverted
                if (Double.parseDouble(isCancelled) == 1
                        || Double.parseDouble(isDiverted) == 1) {
                    continue;
                }
                // constructs key and value
                outputKeyStr = flightDate;
                if (origin.equals("ORD")) {
                    outputKeyStr += dest;
                    outputValStr = "from";
                } else {
                    outputKeyStr += origin;
                    outputValStr = "to";
                }
                outputValStr = outputValStr + "," + depTime + "," + arrTime
                        + "," + arrDelayMinutes;
                outputKey.set(outputKeyStr);
                outputVal.set(outputValStr);
                context.write(outputKey, outputVal);
            }
        }
    }

    public static class JoinReducer extends
            Reducer<Text, Text, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private List<Text> fromList = new ArrayList<Text>();
        private List<Text> toList = new ArrayList<Text>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            fromList.clear();
            toList.clear();
            for (Text val : values) {
                if (val.toString().split(",")[0].equals("from")) {
                    fromList.add(new Text(val));
                } else {
                    toList.add(new Text(val));
                }
            }
            // join the two tables
            for (Text from : fromList) {
                for (Text to : toList) {
                    if (Integer.parseInt(from.toString().split(",")[2]) < Integer
                            .parseInt(to.toString().split(",")[1])) {
                        result.set((int) (Double.parseDouble(from.toString()
                                .split(",")[3]) + Double.parseDouble(to
                                .toString().split(",")[3])));
                        context.write(key, result);
                    }
                }
            }
        }
    }

    /**
     * Mapper class for calculating AVG in job2
     */
    public static class AvgMapper extends
            Mapper<Object, Text, Text, Text> {

        private Text keyText = new Text();
        private Text valText = new Text();
        private double sum = 0;
        private int num = 0;

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            num++;
            sum += Double.parseDouble(value.toString().split("\\s+")[1]);
        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            keyText.set("1");
            valText.set(sum + "," + num);
            context.write(keyText, valText);
            super.cleanup(context);
        }
        
    }

    /**
     * Reducer class for calculating AVG in job2
     */
    public static class AvgReducer extends
            Reducer<Text, Text, IntWritable, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            double sum = 0;
            int num = 0;
            for (Text val : values) {
                sum += Double.parseDouble(val.toString().split(",")[0]);
                num += Integer.parseInt(val.toString().split(",")[1]);
            }
            result.set(sum / num);
            IntWritable keyText = new IntWritable();
            keyText.set(num);
            context.write(keyText, result);
        }
    }
}
