import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCounter {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private List<String> words = new ArrayList<String>();
        private int n;

        // Method that sets up to the program the required length of a ngram
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            n = Integer.parseInt(conf.get("N"));
        }

        // Method that edits each line and adds all words to a list
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String lineWithoutPunctuation = value.toString().replaceAll("[^a-zA-Z ]", " ");
            String inCaseSensitiveLine = lineWithoutPunctuation.toLowerCase();
            StringTokenizer iterator = new StringTokenizer(inCaseSensitiveLine);

            while (iterator.hasMoreTokens()) {
                String nextWord = iterator.nextToken().trim();
                words.add(nextWord);
            }
        }

        // Method which builds ngrams and adds them to the context for reducing
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (int i = 0; i < words.size() - n + 1; i++) {

                // Builds ngram
                StringBuilder nGram = new StringBuilder(words.get(i));

                // Appends the following n-1 words to the current ngram
                for (int j = i + 1; j < n + i; j++) {
                    nGram.append("_").append(words.get(j));
                }

                context.write(new Text(nGram.toString()), one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int k;

        // Method that sets up to the program the required occurrences of each ngram so it can be added to the context
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("K"));
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;

            // Sums the occurrences of each ngram
            for (IntWritable val : values) {
                sum += val.get();
            }

            // If the sum of ngram's occurrences is greater than the user's input then it is being added to the context
            if (sum >= k) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 4) {
            throw new IllegalArgumentException("Usage <in> <out> <ngram's length> <required occurrences>");
        }

        Configuration conf = new Configuration();
        conf.setInt("N", Integer.parseInt(args[2]));
        conf.setInt("K", Integer.parseInt(args[3]));
        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(Ypoergasia1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}