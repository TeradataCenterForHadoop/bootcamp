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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.github.fge.uritemplate.URITemplateException;
import com.github.fge.uritemplate.vars.VariableMap;
import com.github.fge.uritemplate.vars.VariableMapBuilder;
import com.github.fge.uritemplate.vars.values.ScalarValue;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.github.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class GithubSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final GithubClient githubClient;
    private final GithubConfig config;

    @Inject
    public GithubSplitManager(GithubConnectorId connectorId, GithubClient githubClient,
            GithubConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.githubClient = requireNonNull(githubClient, "client is null");
        this.config = requireNonNull(config);
    }

    private int getLastPage(GithubTable table, VariableMap vars)
    {
        URI uri;
        try {
            uri = table.getSourceTemplate().toURI(vars);
        }
        catch (URITemplateException | URISyntaxException e) {
            // TODO: error handling for Presto connectors - ErrorCodeSupplier
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }

        return githubClient.getLastPage(uri, table.getToken());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns)
    {
        GithubTableLayoutHandle layoutHandle = checkType(layout, GithubTableLayoutHandle.class, "layout");
        GithubTableHandle tableHandle = layoutHandle.getTable();
        GithubTable table = githubClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        // TODO : add URIs for github -- for now, just one split
        URI uri;
        VariableMapBuilder variableMapBuilder = VariableMap.newBuilder()
                .addValue("owner", new ScalarValue(tableHandle.getCatalogName()))
                .addValue("schema", new ScalarValue(tableHandle.getSchemaName()))
                .addValue("username", new ScalarValue(tableHandle.getUsername()))
                .addValue("token", new ScalarValue(tableHandle.getToken()));
        int lastPage = getLastPage(table, variableMapBuilder.freeze());

        for (int i = 1; i <= lastPage; ++i) {
            variableMapBuilder.addScalarValue("page", i);

            try {
                uri = table.getSourceTemplate().toURI(variableMapBuilder.freeze());
            }
            catch (URITemplateException | URISyntaxException e) {
                // TODO: error handling for Presto connectors - ErrorCodeSupplier
                throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
            }

            splits.add(new GithubSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), uri, config.getToken()));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
