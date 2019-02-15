package MovieRating;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageRating {
    public static class AverageMovieRatingMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        private Text movieId = new Text();
        private DoubleWritable rating = new DoubleWritable();

        @Override
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

    public static class AverageMovieRatingReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
        conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
        conf.set("mapreduce.framework.name", "yarn");
        Job job = Job.getInstance(conf, "Average Movie Rating");
        job.setJarByClass(AverageRating.class);
        job.setMapperClass(AverageMovieRatingMapper.class);
        job.setReducerClass(AverageMovieRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
