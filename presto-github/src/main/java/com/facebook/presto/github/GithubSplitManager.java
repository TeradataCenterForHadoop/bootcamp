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
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns)
    {
        // TODO: Step 4 - Create a split for each page in the results from the GitHub API.
        // You will need to fill in the query-specific values in the URITemplate.
        // You can use GithubClient#getLastPage to see how many pages there are.

        List<ConnectorSplit> splits = new ArrayList<>();
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
