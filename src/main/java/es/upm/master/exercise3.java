package es.upm.master;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;


/*
1) Calculate the average speed of the cars in a given segment. More
concretely, the output events are tuples with (VID, xway, average speed of the car). 2) Spot the
cars with the highest average speed in a given segment every hour. The information to be output
is (VID, xway and average speed of the vehicle with the highest average speed).
 */


public class exercise3 {

    private static SingleOutputStreamOperator<Tuple3<Integer, Byte, Integer>> outputMapper(SingleOutputStreamOperator<Tuple4<Integer, Integer, Byte, Integer>> inputStream) {
        // drop the first element (time) for the output format
        return inputStream.map(new MapFunction<Tuple4<Integer, Integer, Byte, Integer>, Tuple3<Integer, Byte, Integer>>() {
            public Tuple3<Integer, Byte, Integer> map(Tuple4<Integer, Integer, Byte, Integer> in) throws Exception {
                return new Tuple3<Integer, Byte, Integer>(in.f1, in.f2, in.f3);
            }
        });
    }

    public static DataStream<Tuple5<Integer, Integer, Integer, Byte, Byte>> inputConverter(DataStream<String> inputText) {
        return inputText
                .map(new MapFunction<String, Tuple5<Integer, Integer, Integer, Byte, Byte>>() {
                    public Tuple5<Integer, Integer, Integer, Byte, Byte> map(String in)
                            throws Exception {

                        // split input text
                        String[] fieldArray = in.split(",");

                        Tuple5<Integer, Integer, Integer, Byte, Byte> out = new Tuple5(
                                Integer.parseInt(fieldArray[0]), // time
                                Integer.parseInt(fieldArray[1]), // vid
                                Integer.parseInt(fieldArray[2]), // spd
                                Byte.parseByte(fieldArray[3]),   // xway
                                Byte.parseByte(fieldArray[6]));  // segment
                        return out;
                    }
                });
    }

    public static void main(String[] args) throws Exception {
        // take inputs from the command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final byte segment = params.getByte("segment");

        // get input data
        DataStream<String> text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // time, vid, xway, spd, seg
        DataStream<Tuple5<Integer, Integer, Integer, Byte, Byte>> dataStream = inputConverter(text)
                .filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Byte, Byte>>() {
            public boolean filter(Tuple5<Integer, Integer, Integer, Byte, Byte> in) throws Exception {
                return (in.f4 == segment);
            }
        });

        KeyedStream<Tuple5<Integer, Integer, Integer, Byte, Byte>, Tuple> timestampedKeystream = dataStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple5<Integer, Integer, Integer, Byte, Byte>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple5<Integer, Integer, Integer, Byte, Byte> element) {
                        return element.f0*1000;
                    }
                }
        ).keyBy(1);

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Byte, Integer>> AverageStream = timestampedKeystream
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .apply(new Average());

        // output mapper to drop the time
        if (params.has("output1")) {
            String output_path=params.get("output1");
            SingleOutputStreamOperator<Tuple3<Integer, Byte, Integer>> output1 = outputMapper(AverageStream);
            output1.writeAsCsv(output_path, WriteMode.OVERWRITE);
        }

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Byte, Integer>> hourlyAverageStream = timestampedKeystream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new Average());

        // for output 2, with tumbling window (1 hour), get max(spd) from the keyed stream
        SingleOutputStreamOperator<Tuple4<Integer, Integer, Byte, Integer>> hourlyMaxAverageStream = hourlyAverageStream
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce(new ReduceFunction<Tuple4<Integer, Integer, Byte, Integer>>() {
                    public Tuple4<Integer, Integer, Byte, Integer> reduce(Tuple4<Integer, Integer, Byte, Integer> val1, Tuple4<Integer, Integer, Byte, Integer> val2) throws Exception {
                        if(val1.f3 < val2.f3){ return val2; }
                        return val1;
                    }
                });

        if (params.has("output2")) {
            String output_path=params.get("output2");
            SingleOutputStreamOperator<Tuple3<Integer, Byte, Integer>> output2 = outputMapper(hourlyMaxAverageStream);
            output2.writeAsCsv(output_path, WriteMode.OVERWRITE);
        }

        // execute program
        env.execute("Exercise3");
    }

    // input: time, vid, xway, speed, segment
    // output: time, VID, xway, avgSpeed
    public static class Average implements WindowFunction<Tuple5<Integer, Integer, Integer, Byte, Byte>, Tuple4<Integer, Integer, Byte, Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Byte, Byte>> input, Collector<Tuple4<Integer, Integer, Byte, Integer>> out) throws Exception {
            Iterator<Tuple5<Integer, Integer, Integer, Byte, Byte>> iterator = input.iterator();
            Tuple5<Integer, Integer, Integer, Byte, Byte> first = iterator.next();

            Integer time = 0;
            Integer vid = 0;
            Byte xway = 0;
            Integer sumSpeed = 0;
            Integer count = 1;

            if (first != null) {
                time = first.f0;
                vid = first.f1;
                xway = first.f3;
                sumSpeed = first.f2;
            }
            while (iterator.hasNext()) {
                Tuple5<Integer, Integer, Integer, Byte, Byte> next = iterator.next();
                time = next.f0;
                vid = next.f1;
                xway = next.f3;
                sumSpeed += next.f2;
                count += 1;
            }
            out.collect(new Tuple4<Integer, Integer, Byte, Integer>(time, vid, xway, sumSpeed/count));
        }
    }
}