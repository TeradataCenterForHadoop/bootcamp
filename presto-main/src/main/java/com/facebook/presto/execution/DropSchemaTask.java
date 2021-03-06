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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.metadata.MetadataUtil.createCatalogSchemaName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DropSchemaTask
        implements DataDefinitionTask<DropSchema>
{
    @Override
    public String getName()
    {
        return "DROP SCHEMA";
    }

    @Override
    public String explain(DropSchema statement, List<Expression> parameters)
    {
        return "DROP SCHEMA " + statement.getSchemaName();
    }

    @Override
    public CompletableFuture<?> execute(DropSchema statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        if (statement.isCascade()) {
            throw new PrestoException(NOT_SUPPORTED, "CASCADE is not yet supported for DROP SCHEMA");
        }

        Session session = stateMachine.getSession();
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        if (!metadata.schemaExists(session, schema)) {
            if (!statement.isExists()) {
                throw new SemanticException(MISSING_SCHEMA, statement, "Schema '%s' does not exist", schema);
            }
            return completedFuture(null);
        }

        accessControl.checkCanDropSchema(session.getRequiredTransactionId(), session.getIdentity(), schema);

        metadata.dropSchema(session, schema);

        return completedFuture(null);
    }
}
