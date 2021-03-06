
package hadoop.project;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.opencsv.CSVReader;

public class CopyOfHBaseInserterForMovies {
    private static final String HTABLE_MOVIE = "Movie";
    private static final String HTABLE_FAMILY = "content";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(CopyOfHBaseInserterForMovies.class);
        job.setMapperClass(MovieInsertToHBaseMapper.class);
        // No need for reducer.
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job,
                new Path("/Users/Sam/Downloads/ml-latest-small\\ 2/movies-small.csv"));
        FileOutputFormat.setOutputPath(job, new Path("outputCal"));
        // Create tables if not exist.
        Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);
        if (!admin.tableExists(HTABLE_MOVIE)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(HTABLE_MOVIE);
            tableDescriptor.addFamily(new HColumnDescriptor(HTABLE_FAMILY));
            admin.createTable(tableDescriptor);
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The mapper class to insert into HBase.
     */
    public static class MovieInsertToHBaseMapper extends Mapper<Object, Text, Text, Text> {

        private String movieID = null;
        private String movieName = null;
        private Configuration config;
        private HTable movieTable;
        private Text outputKey = new Text();
        private Text outputVal = new Text();

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                movieName = nextLine[1];
                if (movieName.equals("title")) {
                    continue;
                }
                int i = movieName.indexOf("(20");
                if (i < 0) {
                    continue;
                }
                String year = movieName.substring(i + 1, i + 5);
                outputKey.set(Bytes.toBytes(">2000"));
                outputVal.set(nextLine[0] + "," + nextLine[1] + "," + nextLine[2]);
                context.write(outputKey, outputVal);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        private Text mKey = new Text();
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(t, result);
            }
        }
    }
}
