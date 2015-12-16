
package hadoop.project;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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

import au.com.bytecode.opencsv.CSVReader;

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
        private String movieName1 = null;
        private String userID = null;
        private Configuration config;
        private HTable movieTable;
        private HTable ratingTable;
        private Scan movieScanner;
        private Scan ratingScanner;
        private List<String> movieIdList;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException,
                InterruptedException {
            super.setup(context);
            config = HBaseConfiguration.create();
            movieTable = new HTable(config, HTABLE_MOVIE);
            ratingTable = new HTable(config, HTABLE_RATING);
            movieScanner = new Scan();
            movieIdList = new ArrayList<String>();
            ResultScanner movieResult = movieTable.getScanner(movieScanner);
            String mid = null;
            for (Result mr = movieResult.next(); mr != null; mr = movieResult.next()) {
                mid = Bytes.toString(mr.getRow());
                movieIdList.add(mid);
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            CSVReader reader = new CSVReader(new StringReader(value.toString()));
            String[] nextLine;
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

                for (String movieID2 : movieIdList) {
                    if (movieID2.equals("movieId")
                            || Integer.parseInt(movieID2) <= Integer.parseInt(movieID1)) {
                        continue;
                    }
                    ResultScanner ratingResult = ratingTable.getScanner(ratingScanner);
                    List<Get> getList = new ArrayList<Get>();
                    List<String> ratingsForMovieID1 = new ArrayList<String>();
                    try {
                        for (Result rr = ratingResult.next(); rr != null; rr = ratingResult
                                .next()) {
                            userID = Bytes.toString(rr.getRow()).split("-")[1];
                            Get g = new Get(Bytes.toBytes(movieID2 + "-" + userID));
                            getList.add(g);
                            ratingsForMovieID1.add(Bytes.toString(rr.getValue(
                                    Bytes.toBytes(HTABLE_FAMILY), Bytes
                                            .toBytes("Rating"))));
                            // Result r = ratingTable.get(g);
                        }
                    } finally {
                        ratingResult.close();
                    }
                    Result[] ratingResults = ratingTable.get(getList);
                    for (int index = 0; index < ratingResults.length; index++) {
                        String rating2 = Bytes.toString(ratingResults[index].getValue(
                                Bytes.toBytes(HTABLE_FAMILY), Bytes
                                        .toBytes("Rating")));
                        if (rating2 != null) {
                            String rating1 = ratingsForMovieID1.get(index);
                            outputKey.set(movieID1 + "-" + movieID2);
                            outputVal.set(rating1 + "," + rating2);
                            context.write(outputKey, outputVal);
                        }
                    }
                }
            }
        }
    }

    public static class MovieSimilarityReducer extends
            Reducer<Text, Text, Text, Text> {

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
