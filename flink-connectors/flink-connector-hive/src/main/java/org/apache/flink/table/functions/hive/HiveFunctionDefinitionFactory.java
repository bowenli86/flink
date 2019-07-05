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

package org.apache.flink.table.functions.hive;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Factory to create {@link FunctionDefinition} for Hive user defined functions.
 */
public class HiveFunctionDefinitionFactory implements FunctionDefinitionFactory {
	private static final Logger LOG = LoggerFactory.getLogger(HiveFunctionDefinitionFactory.class);

	@Override
	public FunctionDefinition createFunctionDefinition(
			String name,
			CatalogFunction catalogFunction,
			Object[] constantArguments,
			DataType[] argTypes) {

		String functionClassName = catalogFunction.getClassName();

		if (Boolean.valueOf(catalogFunction.getProperties().get(CatalogConfig.IS_GENERIC))) {
			throw new TableException(
				String.format("HiveFunctionDefinitionFactory does not support generic functions %s yet", name));
		}

		Class clazz;
		try {
			clazz = Thread.currentThread().getContextClassLoader().loadClass(functionClassName);

			LOG.info("Successfully loaded Hive udf '{}' with class '{}'", name, functionClassName);
		} catch (ClassNotFoundException e) {
			throw new TableException(
				String.format("Failed to initiate an instance of class %s.", functionClassName), e);
		}

		if (UDF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveSimpleUDF", name);

			return new ScalarFunctionDefinition(
				name,
				new HiveSimpleUDF(new HiveFunctionWrapper<>(functionClassName))
			);
		} else if (GenericUDF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveGenericUDF", name);

			return new ScalarFunctionDefinition(
				name,
				new HiveGenericUDF(new HiveFunctionWrapper<>(functionClassName))
			);
		} else if (GenericUDTF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveGenericUDTF", name);

			HiveGenericUDTF udtf = new HiveGenericUDTF(new HiveFunctionWrapper<>(functionClassName));

			udtf.setArgumentTypesAndConstants(constantArguments, argTypes);

			return new TableFunctionDefinition(
				name,
				udtf,
				udtf.getResultType()
			);
		} else if (GenericUDAFResolver2.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveGenericUDAF with no UDAF bridging", name);

			HiveGenericUDAF udaf = new HiveGenericUDAF(new HiveFunctionWrapper<>(functionClassName), false);

			udaf.setArgumentTypesAndConstants(constantArguments, argTypes);

			return new AggregateFunctionDefinition(
				name,
				udaf,
				udaf.getResultType(),
				udaf.getAccumulatorType()
			);
		} else if (UDAF.class.isAssignableFrom(clazz)) {
			LOG.info("Transforming Hive function '{}' into a HiveGenericUDAF with UDAF bridging", name);

			HiveGenericUDAF udaf = new HiveGenericUDAF(new HiveFunctionWrapper<>(functionClassName), true);

			udaf.setArgumentTypesAndConstants(constantArguments, argTypes);

			return new AggregateFunctionDefinition(
				name,
				udaf,
				udaf.getResultType(),
				udaf.getAccumulatorType()
			);
		} else {
			throw new IllegalArgumentException(
				String.format("HiveFunctionDefinitionFactory cannot initiate FunctionDefinition for class %s", functionClassName));
		}
	}

	@Override
	public Map<String, String> requiredContext() {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<String> supportedProperties() {
		throw new UnsupportedOperationException();
	}
}
