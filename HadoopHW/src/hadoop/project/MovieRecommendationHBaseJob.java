
package hadoop.project;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
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

import com.opencsv.CSVReader;

public class MovieRecommendationHBaseJob {
    private static final String HTABLE_MOVIE = "Movie";
    private static final String HTABLE_RATING = "Rating";
    private static final String HTABLE_FAMILY = "content";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MovieRecommendationHBaseJob.class);
        job.setMapperClass(MovieHBaseMapper.class);
        job.setReducerClass(MovieSimilarityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // FileInputFormat.addInputPath(job, new Path(
        // "/Users/Sam/Downloads/ml-latest-small/movies.csv"));
        // FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The mapper class to insert into HBase.
     */
    public static class MovieHBaseMapper extends Mapper<Object, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputVal = new Text();
        private String movieID1 = null;
        private String movieID2 = null;
        private String movieName1 = null;
        private String movieName2 = null;
        private String userID = null;
        private Configuration config;
        private HTable movieTable;
        private HTable ratingTable;
        private Scan movieScanner;
        private Scan ratingScanner;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            config = HBaseConfiguration.create();
            movieTable = new HTable(config, HTABLE_MOVIE);
            ratingTable = new HTable(config, HTABLE_RATING);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
            movieScanner = new Scan();
            ratingScanner = new Scan();
            while ((nextLine = reader.readNext()) != null) {
                if (nextLine.length < 2) {
                    continue;
                }
                movieID1 = nextLine[0];
                movieName1 = nextLine[1];
                int i = movieName1.indexOf("(20");
                if (i < 0) {
                    continue;
                }
                System.out.println(movieID1 + " " + movieName1);
                PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(movieID1 + "-"));
                ratingScanner.setFilter(prefixFilter);
                ResultScanner movieResult = movieTable.getScanner(movieScanner);
                try {
                    for (Result mr = movieResult.next(); mr != null; mr = movieResult.next()) {
                        movieID2 = Bytes.toString(mr.getRow());
                        if (movieID2.equals("movieId")
                                || Integer.parseInt(movieID2) <= Integer.parseInt(movieID1)) {
                            continue;
                        }
                        ResultScanner ratingResult = ratingTable.getScanner(ratingScanner);
                        try {
                            for (Result rr = ratingResult.next(); rr != null; rr = ratingResult
                                    .next()) {
                                userID = Bytes.toString(rr.getRow()).split("-")[1];
                                Get g = new Get(Bytes.toBytes(movieID2 + "-" + userID));
                                Result r = ratingTable.get(g);
                                String rating2 = Bytes.toString(r.getValue(
                                        Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Rating")));
                                if (rating2 != null) {
                                    String rating1 = Bytes.toString(rr.getValue(
                                            Bytes.toBytes(HTABLE_FAMILY), Bytes.toBytes("Rating")));
                                    outputKey.set(movieID1 + "-" + movieID2);
                                    outputVal.set(rating1 + "," + rating2);
                                    context.write(outputKey, outputVal);
                                }
                            }
                        } finally {
                            ratingResult.close();
                        }
                    }
                } finally {
                    movieResult.close();
                }
            }
        }
    }

    public static class MovieSimilarityReducer extends Reducer<Text, Text, Text, Text> {

        private Text resultKey = new Text();
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long numerator = 0;
            long cosineDenominator1 = 0;
            long cosineDenominator2 = 0;
            double rating1 = 0;
            double rating2 = 0;
            int count = 0;
            for (Text t : values) {
                count++;
                rating1 = Double.parseDouble(t.toString().split(",")[0]);
                rating2 = Double.parseDouble(t.toString().split(",")[1]);
                numerator += (rating1 * rating2);
                cosineDenominator1 += (rating1 * rating1);
                cosineDenominator2 += (rating2 * rating2);
            }
            if (count < 20) {
                return;
            }
            double similarity = numerator
                    / (Math.sqrt(cosineDenominator1) * Math.sqrt(cosineDenominator2));
            resultKey.set(key.toString().split("-")[0] + " " + key.toString().split("-")[1]);
            result.set(String.valueOf(similarity) + " " + count);
            context.write(resultKey, result);
        }
    }
}
