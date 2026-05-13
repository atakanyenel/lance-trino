/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.lance;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.spi.procedure.Procedure;
import org.lance.Dataset;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class LanceProcedures
        implements Provider<Set<Procedure>>
{
    private static final Logger log = Logger.get(LanceProcedures.class);

    private final LanceRuntime runtime;

    @Inject
    public LanceProcedures(LanceRuntime runtime)
    {
        this.runtime = requireNonNull(runtime, "runtime is null");
    }

    @Override
    public Set<Procedure> get()
    {
        return Set.of(createScalarIndexProcedure());
    }

    private Procedure createScalarIndexProcedure()
    {
        MethodHandle methodHandle;
        try {
            methodHandle = lookup().unreflect(
                    LanceProcedures.class.getMethod("createScalarIndex", String.class, String.class, String.class, String.class));
            methodHandle = methodHandle.bindTo(this);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }

        return new Procedure(
                "system",
                "create_scalar_index",
                List.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("COLUMN_NAME", VARCHAR),
                        new Procedure.Argument("INDEX_TYPE", VARCHAR, false, "BTREE")),
                methodHandle);
    }

    public void createScalarIndex(String schemaName, String tableName, String columnName, String indexType)
    {
        log.info("Creating scalar index on %s.%s column=%s type=%s", schemaName, tableName, columnName, indexType);

        List<String> tableId = runtime.getTableId(schemaName, tableName);
        IndexType idxType = IndexType.valueOf(indexType.toUpperCase());

        // Resolve table path and storage options via namespace
        DescribeTableRequest describeRequest = new DescribeTableRequest().id(tableId);
        DescribeTableResponse describeResponse = runtime.getNamespace().describeTable(describeRequest);
        String tablePath = describeResponse.getLocation();
        Map<String, String> storageOptions = describeResponse.getStorageOptions();
        if (storageOptions == null) {
            storageOptions = new HashMap<>();
        }
        if (storageOptions.isEmpty()) {
            storageOptions = runtime.getNamespaceStorageOptions();
        }

        IndexParams params = IndexParams.builder()
                .setScalarIndexParams(ScalarIndexParams.create(indexType.toUpperCase()))
                .build();

        // Open dataset directly (not cached) since index creation modifies the dataset
        try (Dataset dataset = runtime.openDatasetDirect("system", tablePath, null, storageOptions)) {
            dataset.createIndex(List.of(columnName), idxType, Optional.of(tableName + "_" + columnName + "_idx"), params, false);
        }

        log.info("Successfully created %s scalar index on %s.%s.%s", indexType, schemaName, tableName, columnName);
    }
}
