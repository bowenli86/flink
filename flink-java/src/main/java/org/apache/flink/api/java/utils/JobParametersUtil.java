/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.utils;

import org.apache.flink.api.common.ExecutionConfig;

/**
 *
 */
public class JobParametersUtil {
	private JobParametersUtil() {}

	/**
	 * Returns the String value for the given key.
	 * If the key does not exist it will return null.
	 */
	public static String get(ExecutionConfig.GlobalJobParameters params, String key) {
		return params.toMap().get(key);
	}

	/**
	 * Returns the String value for the given key.
	 * If the key does not exist it will throw a {@link RuntimeException}.
	 */
	public static String getRequired(ExecutionConfig.GlobalJobParameters params, String key) {
		String value = get(params, key);
		if (value == null) {
			throw new RuntimeException("No data for required key '" + key + "'");
		}
		return value;
	}

	/**
	 * Returns the String value for the given key.
	 * If the key does not exist it will return the given default value.
	 */
	public static String get(ExecutionConfig.GlobalJobParameters params, String key, String defaultValue) {
		String value = get(params, key);
		if (value == null) {
			return defaultValue;
		} else {
			return value;
		}
	}

	/**
	 * Check if value is set.
	 */
	public static boolean has(ExecutionConfig.GlobalJobParameters params, String value) {
		return params.toMap().containsKey(value);
	}

	/**
	 * Returns the Double value for the given key.
	 * The method fails if the key does not exist.
	 */
	public static double getDouble(ExecutionConfig.GlobalJobParameters params, String key) {
		String value = getRequired(params, key);
		return Double.valueOf(value);
	}

	/**
	 * Returns the Double value for the given key. If the key does not exists it will return the default value given.
	 * The method fails if the value is not a Double.
	 */
	public static double getDouble(ExecutionConfig.GlobalJobParameters params, String key, double defaultValue) {
		String value = get(params, key);
		if (value == null) {
			return defaultValue;
		} else {
			return Double.valueOf(value);
		}
	}

	/**
	 * Returns the Long value for the given key.
	 * The method fails if the key does not exist.
	 */
	public static long getLong(ExecutionConfig.GlobalJobParameters params, String key) {
		String value = getRequired(params, key);
		return Long.parseLong(value);
	}

	/**
	 * Returns the Long value for the given key. If the key does not exists it will return the default value given.
	 * The method fails if the value is not a Long.
	 */
	public static long getLong(ExecutionConfig.GlobalJobParameters params, String key, long defaultValue) {
		String value = get(params, key);
		if (value == null) {
			return defaultValue;
		}
		return Long.parseLong(value);
	}

	/**
	 * Returns the Integer value for the given key.
	 * The method fails if the key does not exist or the value is not an Integer.
	 */
	public static int getInt(ExecutionConfig.GlobalJobParameters params, String key) {
		String value = getRequired(params, key);
		return Integer.parseInt(value);
	}

	/**
	 * Returns the Integer value for the given key. If the key does not exists it will return the default value given.
	 * The method fails if the value is not an Integer.
	 */
	public static int getInt(ExecutionConfig.GlobalJobParameters params, String key, int defaultValue) {
		String value = get(params, key);
		if (value == null) {
			return defaultValue;
		}
		return Integer.parseInt(value);
	}
}
