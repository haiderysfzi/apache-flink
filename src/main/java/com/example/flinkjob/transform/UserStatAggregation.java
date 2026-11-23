package com.example.flinkjob.transform;

import com.example.flinkjob.model.Event;
import com.example.flinkjob.model.UserStat;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Incremental aggregation: Flink keeps ONE small accumulator per (key, window) in state instead of
 * buffering every event. The ProcessWindowFunction only decorates the final result with key + bounds.
 */
public final class UserStatAggregation {
    private UserStatAggregation() {}

    /** accumulator = [count, sum] */
    public static class Agg implements AggregateFunction<Event, double[], double[]> {
        private static final long serialVersionUID = 1L;

        @Override
        public double[] createAccumulator() {
            return new double[] {0, 0};
        }

        @Override
        public double[] add(Event e, double[] acc) {
            acc[0] += 1;
            acc[1] += e.amount;
            return acc;
        }

        @Override
        public double[] getResult(double[] acc) {
            return acc;
        }

        @Override
        public double[] merge(double[] a, double[] b) {
            return new double[] {a[0] + b[0], a[1] + b[1]};
        }
    }

    public static class Emit extends ProcessWindowFunction<double[], UserStat, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(String userId, Context ctx, Iterable<double[]> results, Collector<UserStat> out) {
            double[] r = results.iterator().next(); // exactly one element with an AggregateFunction
            TimeWindow w = ctx.window();
            out.collect(new UserStat(userId, w.getStart(), w.getEnd(), (long) r[0], Math.round(r[1] * 100) / 100.0));
        }
    }
}
