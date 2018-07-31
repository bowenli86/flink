package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *
 */
@PublicEvolving
public abstract class JoinWindowFunction<IN1, IN2, K, OUT, W extends Window> extends AbstractRichFunction {
	private static final long serialVersionUID = -6633780630831675656L;

	/**
	 *
	 */
	public abstract OUT join(IN1 first, IN2 second, Context ctx);

	/**
	 * The context that is available during an invocation of
	 * {@link #processElement(Object, Object, ProcessJoinFunction.Context, Collector)}. It gives access to the timestamps of the
	 * left element in the joined pair, the right one, and that of the joined pair. In addition, this context
	 * allows to emit elements on a side output.
	 */
	public abstract class Context {

		/**
		 * Returns the window that is being evaluated.
		 */
		public abstract W window();

		/**
		 * Get key of the element being processed.
		 */
		public abstract K getCurrentKey();

		/**
		 * @return The timestamp of the left element of a joined pair
		 */
		public abstract long getLeftTimestamp();

		/**
		 * @return The timestamp of the right element of a joined pair
		 */
		public abstract long getRightTimestamp();

		/**
		 * @return The timestamp of the joined pair.
		 */
		public abstract long getTimestamp();

		/**
		 * Emits a record to the side output identified by the {@link OutputTag}.
		 *
		 * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
		 * @param value The record to emit.
		 */
		public abstract <X> void output(OutputTag<X> outputTag, X value);

	}
}
