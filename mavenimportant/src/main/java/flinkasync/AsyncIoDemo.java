package flinkasync;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class AsyncIoDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile("data/test/ab.txt");

        /**
         * public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(
         * 			DataStream<IN> in,
         * 			AsyncFunction<IN, OUT> func,
         * 			long timeout,
         * 			TimeUnit timeUnit,
         * 			int capacity)
         * @param in Input {@link DataStream}
         * @param func {@link AsyncFunction}
         * @param timeout for the asynchronous operation to complete
         * @param timeUnit of the given timeout
         * @param capacity The max number of async i/o operation that can be triggered
         * @return A new {@link SingleOutputStreamOperator}.
         */
        SingleOutputStreamOperator<String> result =
                AsyncDataStream.orderedWait(
                source,
                new AsyncFunDemo(),
                60000,
                TimeUnit.SECONDS,
                1
        );

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            System.out.println("异常");
            e.printStackTrace();
        }
    }
}
