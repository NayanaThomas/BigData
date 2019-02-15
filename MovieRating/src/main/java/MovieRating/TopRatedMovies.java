package MovieRating;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TopRatedMovies {


    public static class TopRatedMoviesMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text movieId = new Text();
        private final static DoubleWritable rating = new DoubleWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String csvLine = value.toString();
            String[] values = csvLine.split(",");
            if(values[0].equals("userId"))
                return;
            movieId.set(values[1]);
            rating.set(Double.parseDouble(values[2]));
            context.write(movieId, rating);
        }
    }

    public static class TopRatedMoviesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private Map<Text, DoubleWritable> countMap = new HashMap<Text, DoubleWritable>();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            countMap.put(new Text(key), new DoubleWritable(sum/count));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, DoubleWritable> sortedMap = DescSortHelper.sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 20) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Top20 <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top Rated Movies");
        job.setJarByClass(TopRatedMovies.class);
        job.setMapperClass(TopRatedMoviesMapper.class);
        job.setReducerClass(TopRatedMoviesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}