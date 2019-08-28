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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunctionDefinition;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.functions.UserDefinedAggregateFunction;
import org.apache.flink.table.functions.UserFunctionsTypeHelper;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple function catalog to store {@link FunctionDefinition}s in catalogs.
 */
@Internal
public class FunctionCatalog implements FunctionLookup {

	private final CatalogManager catalogManager;

	// For simplicity, currently hold registered Flink functions in memory here
	// TODO: should move to catalog
	private final Map<String, FunctionDefinition> tempFunctions = new LinkedHashMap<>();

	/**
	 * Temporary utility until the new type inference is fully functional. It needs to be set by the planner.
	 */
	private PlannerTypeInferenceUtil plannerTypeInferenceUtil;

	public FunctionCatalog(CatalogManager catalogManager) {
		this.catalogManager = checkNotNull(catalogManager);
	}

	public void setPlannerTypeInferenceUtil(PlannerTypeInferenceUtil plannerTypeInferenceUtil) {
		this.plannerTypeInferenceUtil = plannerTypeInferenceUtil;
	}

	public void registerScalarFunction(String name, ScalarFunction function) {
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());
		registerFunction(
			name,
			new ScalarFunctionDefinition(name, function)
		);
	}

	public <T> void registerTableFunction(
			String name,
			TableFunction<T> function,
			TypeInformation<T> resultType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		registerFunction(
			name,
			new TableFunctionDefinition(
				name,
				function,
				resultType)
		);
	}

	public <T, ACC> void registerAggregateFunction(
			String name,
			UserDefinedAggregateFunction<T, ACC> function,
			TypeInformation<T> resultType,
			TypeInformation<ACC> accType) {
		// check if class not Scala object
		UserFunctionsTypeHelper.validateNotSingleton(function.getClass());
		// check if class could be instantiated
		UserFunctionsTypeHelper.validateInstantiation(function.getClass());

		final FunctionDefinition definition;
		if (function instanceof AggregateFunction) {
			definition = new AggregateFunctionDefinition(
				name,
				(AggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else if (function instanceof TableAggregateFunction) {
			definition = new TableAggregateFunctionDefinition(
				name,
				(TableAggregateFunction<?, ?>) function,
				resultType,
				accType);
		} else {
			throw new TableException("Unknown function class: " + function.getClass());
		}

		registerFunction(
			name,
			definition
		);
	}

	public String[] getUserDefinedFunctions() {
		return getUserDefinedFunctionNames().toArray(new String[0]);
	}

	public String[] getFunctions() {
		Set<String> result = getUserDefinedFunctionNames();

		// Get built-in functions
		result.addAll(
			BuiltInFunctionDefinitions.getDefinitions()
				.stream()
				.map(f -> normalizeName(f.getName()))
				.collect(Collectors.toSet())
		);

		return result.toArray(new String[0]);
	}

	private Set<String> getUserDefinedFunctionNames() {
		Set<String> result = new HashSet<>();

		// Get functions in catalog
		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();
		try {
			result.addAll(catalog.listFunctions(catalogManager.getCurrentDatabase()));
		} catch (DatabaseNotExistException e) {
			// Ignore since there will always be a current database of the current catalog
		}

		// Get functions registered in memory
		result.addAll(
			tempFunctions.values().stream()
				.map(FunctionDefinition::toString)
				.collect(Collectors.toSet()));

		return result;
	}

	@Override
	public Optional<FunctionLookup.Result> lookupFunction(String name) {
		String normalizedName = normalizeName(name);

		if (tempFunctions.containsKey(normalizedName)) {
			return Optional.of(
				new FunctionLookup.Result(
					ObjectIdentifier.of("", "", name), tempFunctions.get(normalizedName)));
		}

		Optional<FunctionDefinition> flinkBuiltIn = BuiltInFunctionDefinitions.getDefinitions()
				.stream()
				.filter(f -> normalizedName.equals(normalizeName(f.getName())))
				.findFirst()
				.map(Function.identity());

		if (flinkBuiltIn.isPresent()) {
			return flinkBuiltIn.map(definition -> new FunctionLookup.Result(
				ObjectIdentifier.of(
					"",
					"",
					name),
				definition)
			);
		}

		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();

		Optional<CatalogFunction> external = catalog.getExternalBuiltInFunction(normalizedName);
		if (external.isPresent()) {
			FunctionDefinition fd = createFunctionDefinition(normalizedName, external.get());
			return Optional.of(
				new FunctionLookup.Result(
					ObjectIdentifier.of(
						"",
						"",
						name),
					fd)
			);
		}

		ObjectPath path = new ObjectPath(catalogManager.getCurrentDatabase(), normalizedName);
		try {
			FunctionDefinition fd = createFunctionDefinition(normalizedName, catalog.getFunction(path));
			return Optional.of(
				new FunctionLookup.Result(
					ObjectIdentifier.of(
						catalogManager.getCurrentCatalog(),
						catalogManager.getCurrentDatabase(),
						name),
					fd)
			);
		} catch (FunctionNotExistException e) {
			// ignore
		}

		return Optional.empty();
	}

	private FunctionDefinition createFunctionDefinition(String name, CatalogFunction catalogFunction) {
		Catalog catalog = catalogManager.getCatalog(catalogManager.getCurrentCatalog()).get();

		if (catalog.getTableFactory().isPresent() && catalog.getTableFactory().get() instanceof FunctionDefinitionFactory) {
			FunctionDefinitionFactory factory = (FunctionDefinitionFactory) catalog.getTableFactory().get();

			return factory.createFunctionDefinition(name, catalogFunction);
		} else {
			// go thru function definition discovery service
			// out of scope of this FLIP
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public PlannerTypeInferenceUtil getPlannerTypeInferenceUtil() {
		Preconditions.checkNotNull(
			plannerTypeInferenceUtil,
			"A planner should have set the type inference utility.");
		return plannerTypeInferenceUtil;
	}

	private void registerFunction(String name, FunctionDefinition functionDefinition) {
		// TODO: should register to catalog
		tempFunctions.put(normalizeName(name), functionDefinition);
	}

	@VisibleForTesting
	static String normalizeName(String name) {
		return name.toUpperCase();
	}
}
