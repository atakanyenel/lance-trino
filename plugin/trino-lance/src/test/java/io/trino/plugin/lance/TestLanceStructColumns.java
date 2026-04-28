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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLanceStructColumns
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return LanceQueryRunner.builder()
                .setUseTempDirectory(true)
                .build();
    }

    @Test
    public void testCreateTableWithStructColumn()
    {
        String tableName = "test_struct_create_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, metadata ROW(name VARCHAR, value BIGINT))");
            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertAndReadStructColumn()
    {
        String tableName = "test_struct_insert_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " AS SELECT CAST(1 AS BIGINT) as id, CAST(ROW('hello', 42) AS ROW(name VARCHAR, value BIGINT)) as metadata", 1);

            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT metadata.name FROM " + tableName)).isEqualTo("hello");
            assertThat(computeScalar("SELECT metadata.value FROM " + tableName)).isEqualTo(42L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testStructWithBinaryField()
    {
        String tableName = "test_struct_binary_" + System.currentTimeMillis();
        try {
            // This mirrors the VLM precooked schema: struct<data: binary, checksum: integer>
            assertUpdate("CREATE TABLE " + tableName +
                    " AS SELECT CAST(1 AS BIGINT) as id, " +
                    "CAST(ROW(X'DEADBEEF', 12345) AS ROW(data VARBINARY, checksum INTEGER)) as chunked_example", 1);

            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT chunked_example.checksum FROM " + tableName)).isEqualTo(12345);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testNestedStruct()
    {
        String tableName = "test_struct_nested_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " AS SELECT CAST(1 AS BIGINT) as id, " +
                    "CAST(ROW('outer', ROW('inner', 99)) AS ROW(label VARCHAR, nested ROW(label VARCHAR, num BIGINT))) as payload", 1);

            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT payload.label FROM " + tableName)).isEqualTo("outer");
            assertThat(computeScalar("SELECT payload.nested.label FROM " + tableName)).isEqualTo("inner");
            assertThat(computeScalar("SELECT payload.nested.num FROM " + tableName)).isEqualTo(99L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testMultipleStructColumns()
    {
        String tableName = "test_struct_multi_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " AS SELECT CAST(1 AS BIGINT) as id, " +
                    "CAST(ROW('a', 1) AS ROW(name VARCHAR, val BIGINT)) as col_a, " +
                    "CAST(ROW('b', 2) AS ROW(name VARCHAR, val BIGINT)) as col_b", 1);

            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(1L);
            assertThat(computeScalar("SELECT col_a.name FROM " + tableName)).isEqualTo("a");
            assertThat(computeScalar("SELECT col_b.val FROM " + tableName)).isEqualTo(2L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testStructWithMultipleRows()
    {
        String tableName = "test_struct_rows_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " AS SELECT * FROM (VALUES " +
                    "(CAST(1 AS BIGINT), CAST(ROW('first', 10) AS ROW(name VARCHAR, value BIGINT))), " +
                    "(CAST(2 AS BIGINT), CAST(ROW('second', 20) AS ROW(name VARCHAR, value BIGINT))), " +
                    "(CAST(3 AS BIGINT), CAST(ROW('third', 30) AS ROW(name VARCHAR, value BIGINT)))" +
                    ") AS t(id, metadata)", 3);

            assertThat(computeScalar("SELECT COUNT(*) FROM " + tableName)).isEqualTo(3L);
            assertThat(computeScalar("SELECT SUM(metadata.value) FROM " + tableName)).isEqualTo(60L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testDescribeTableWithStructColumn()
    {
        String tableName = "test_struct_describe_" + System.currentTimeMillis();
        try {
            assertUpdate("CREATE TABLE " + tableName +
                    " (id BIGINT, metadata ROW(name VARCHAR, value BIGINT))");

            assertQuery("SELECT column_name, data_type FROM information_schema.columns " +
                            "WHERE table_name = '" + tableName + "' ORDER BY ordinal_position",
                    "VALUES ('id', 'bigint'), ('metadata', 'row(name varchar, value bigint)')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
