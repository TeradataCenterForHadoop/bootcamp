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

import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.Objects.requireNonNull;

public class GithubModule
        implements Module
{
    private final String connectorId;
    private final TypeManager typeManager;

    public GithubModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(GithubConnector.class).in(Scopes.SINGLETON);
        binder.bind(GithubConnectorId.class).toInstance(new GithubConnectorId(connectorId));
        binder.bind(GithubMetadata.class).in(Scopes.SINGLETON);
        binder.bind(GithubClient.class).in(Scopes.SINGLETON);
        binder.bind(GithubSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(GithubRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(GithubConfig.class);

        jsonCodecBinder(binder).bindJsonCodec(GithubApiEndpoints.class);
        jsonCodecBinder(binder).bindListJsonCodec(GithubSchema.class);
    }
}
