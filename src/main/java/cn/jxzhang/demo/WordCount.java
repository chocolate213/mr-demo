package cn.jxzhang.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    /**
     * KEYIN：   输入 key
     * VALUEIN： 输入 value
     * KEYOUT：  输出 key
     * VALUEOUT：输出 value
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /**
         * map 方法，每行记录处理时将会调用该方法
         *
         * @param key     key，HDFS 文件的 key 为 字符偏移量，其中换行符在 linux 平台占用 1 个字符
         * @param value   每行内容
         * @param context 上下文对象，可以将结果输出
         * @throws IOException          IOException
         * @throws InterruptedException InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 该类可以将 string 分割成 token，默认情况下将会使用 " \t\n\r\f" 作为分词标记
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                // Generate an output key/value pair.
                context.write(word, one);
            }
        }

        /**
         * 默认情况下，该方法将在 map 方法执行之前执行一次
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("on setup...");
            Configuration configuration = context.getConfiguration();
            configuration.forEach(System.out::println);
            super.setup(context);
        }

        /**
         * 默认情况下，该方法将在map 方法执行之后执行一次
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("on cleanup...");
            super.cleanup(context);
        }

        /**
         * 该方法可以根据需要调整 mapper 的运行顺序
         * <p>
         * 该方法只运行一次
         */
        @Override
        public void run(Context context) throws IOException, InterruptedException {
            System.out.println("on run...");
            System.out.println("context impl in map: " + context.getClass().getName());
            System.out.println("context hash in map: " + context.getClass().hashCode());

            setup(context);
            try {
                while (context.nextKeyValue()) {
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                }
            } finally {
                cleanup(context);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         *
         * @param key     key
         * @param values  key 对应的 value 集合
         * @param context 上下文对象
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            // 与 map 一样，输出结果
            context.write(key, result);

            System.out.println("on run...");
            System.out.println("context impl in reduce: " + context.getClass().getName());
            System.out.println("context hash in reduce: " + context.getClass().hashCode());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        // 通过查找给定 jar 的来源来设置 jar
        job.setJarByClass(WordCount.class);

        // 设置 mapper 类
        job.setMapperClass(TokenizerMapper.class);

        // 设置 combiner 类
        job.setCombinerClass(IntSumReducer.class);

        // 设置 reduce 类
        job.setReducerClass(IntSumReducer.class);

        // 设置 output key 类
        job.setOutputKeyClass(Text.class);

        // 设置 output value 类
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 退出执行主程序，如果 job.waitForCompletion(true) 返回 true，则退出码为 0，表示成功，否则为 1，表示失败
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}