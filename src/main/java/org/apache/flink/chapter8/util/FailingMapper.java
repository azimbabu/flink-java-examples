package org.apache.flink.chapter8.util;

import org.apache.flink.api.common.functions.MapFunction;

import lombok.RequiredArgsConstructor;

/**
 * An MapFunction that forwards all records.
 *
 * Any instance of the function will fail after forwarding a configured number of records by
 * throwing an exception.
 *
 * NOTE: This function is only used to demonstrate Flink's failure recovery capabilities.
 *
 * @tparam IN The type of input and output records.
 */
public class FailingMapper<IN> implements MapFunction<IN, IN>
{
	private final int failInterval;
	private int count = 0;

	/**
	 *
	 * @param failInterval The number of records that are forwarded before throwing an exception.
	 */
	public FailingMapper(int failInterval)
	{
		this.failInterval = failInterval;
	}

	@Override
	public IN map(IN value) throws Exception
	{
		count++;
		// check failure condition
		if (count > failInterval) {
			throw new RuntimeException("Fail application to demonstrate output consistency.");
		}
		// forward value
		return value;
	}
}
