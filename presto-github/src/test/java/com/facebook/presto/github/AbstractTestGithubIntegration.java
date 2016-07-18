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
package com.facebook.presto.github;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestGithubIntegration
{
    private QueryRunner queryRunner;

    protected AbstractTestGithubIntegration(QueryRunner queryRunner)
    {
        this.queryRunner = queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        queryRunner.close();
    }

    @Test
    public void testShowSchemas()
            throws Exception
    {
        MaterializedResult expectedSubset = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), createUnboundedVarcharType())
                .row("presto")
                .build();
        MaterializedResult actual = queryRunner.execute("SHOW SCHEMAS");
        assertContains(actual, expectedSubset);
    }

    @Test
    public void testShowTables()
            throws Exception
    {
        List<MaterializedRow> expected = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), createUnboundedVarcharType())
                .row("stargazers")
                .build()
                .getMaterializedRows();
        MaterializedResult actual = queryRunner.execute("SHOW TABLES");
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testShowColumns()
            throws Exception
    {
        List<MaterializedRow> expected = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("login", "varchar", "")
                .row("id", "bigint", "")
                .build()
                .getMaterializedRows();
        MaterializedResult actual = queryRunner.execute("SHOW COLUMNS FROM stargazers");
        assertEquals(actual, expected);
    }

    @Test
    public void testSelectFromStargazers()
            throws Exception
    {
        MaterializedResult expectedSubset = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), createUnboundedVarcharType(), BIGINT)
                .row(GithubApiCredentials.NAME, GithubApiCredentials.ID)
                .build();
        MaterializedResult actual = queryRunner.execute("SELECT * FROM stargazers");
        assertContains(actual, expectedSubset);
    }
}
