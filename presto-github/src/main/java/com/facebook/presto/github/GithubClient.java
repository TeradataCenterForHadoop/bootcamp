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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.uritemplate.URITemplate;
import com.github.fge.uritemplate.URITemplateException;
import com.github.fge.uritemplate.vars.VariableMap;
import com.github.fge.uritemplate.vars.VariableMapBuilder;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class GithubClient
{
    private final Supplier<GithubApiEndpoints> endpoints;
    private final Supplier<List<GithubSchema>> schemas;
    private final Map<String, GithubTable> tables;
    private final HttpClient httpClient;

    @Inject
    public GithubClient(
            GithubConfig config,
            JsonCodec<GithubApiEndpoints> apiCodec,
            JsonCodec<List<GithubSchema>> schemaCodec)
            throws Exception
    {
        requireNonNull(config, "config is null");
        httpClient = new HttpClient(new SslContextFactory());
        httpClient.start();
        endpoints = Suppliers.memoize(endpointsSupplier(apiCodec, config));
        schemas = Suppliers.memoize(schemasSupplier(schemaCodec, endpoints, config));
        tables = ImmutableMap.of(
                "stargazers", new GithubTable("stargazers",
                        ImmutableList.of(
                                new GithubColumn("login", VarcharType.VARCHAR),
                                new GithubColumn("id", BigintType.BIGINT)),
                        new URITemplate("https://{username}:{token}@api.github.com/repos/{owner}/{schema}/stargazers{?page}"),
                        config.getToken()));
    }

    private static String getAuthHeader(String token)
    {
        String encoded = Base64.getEncoder().encodeToString(token.trim().getBytes(UTF_8));
        return "Basic " + encoded;
    }

    public byte[] getContent(URI target, String token)
    {
        try {
            ContentResponse response = httpClient.newRequest(target)
                    .agent("presto-github-connector")
                    .method(HttpMethod.GET)
                    .header(HttpHeader.AUTHORIZATION, getAuthHeader(token))
                    .send();

            if (response.getStatus() != 200) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, response.getReason());
            }

            return response.getContent();
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public HttpFields getHeaders(URI target, String token)
    {
        try {
            ContentResponse response = httpClient.newRequest(target)
                    .agent("presto-github-connector")
                    .method(HttpMethod.GET)
                    .header(HttpHeader.AUTHORIZATION, getAuthHeader(token))
                    .send();

            if (response.getStatus() != 200) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, response.getReason());
            }

            return response.getHeaders();
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public int getLastPage(URI uri, String token)
    {
        HttpFields headers = getHeaders(uri, token);
        String links = headers.get("Link");
        if (links == null) {
            return 1;
        }
        for (String link : Splitter.on(',').split(links)) {
            if (!link.endsWith("rel=\"last\"")) {
                continue;
            }
            List<String> parts = Splitter.on(' ').omitEmptyStrings().splitToList(link);
            String target = parts.get(0);
            int start = target.lastIndexOf('=') + 1;
            int end = target.lastIndexOf('>');
            String n = target.substring(start, end);
            return Integer.parseInt(n);
        }
        return 1;
    }

    /**
     * Given a string of bytes, returns a list of JSON objects represented by
     * a map of key-value pairs.
     */
    public static List<Map<String, Object>> parseJsonBytesToRecordList(byte[] bytes)
    {
        String jsonLines = new String(bytes, UTF_8);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return ((ArrayList<Map<String, Object>>) objectMapper.readValue(jsonLines, new TypeReference<ArrayList<Map<String, Object>>>() { }));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    public Set<String> getSchemaNames()
    {
        List<GithubSchema> s = schemas.get();
        return s.stream().map(x -> x.getName()).collect(Collectors.toSet());
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return tables.keySet();
    }

    public GithubTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        return tables.get(tableName);
    }

    private Supplier<GithubApiEndpoints> endpointsSupplier(
            final JsonCodec<GithubApiEndpoints> apiCodec, final GithubConfig config)
            throws URITemplateException, URISyntaxException
    {
        VariableMap variables = VariableMap.newBuilder()
                .addScalarValue("user", config.getOwner())
                .addScalarValue("org", config.getOwner())
                .addScalarValue("username", config.getUsername())
                .addScalarValue("token", config.getToken())
                .freeze();

        URI apiRootUri = config.getApiRoot().toURI(variables);
        return () -> {
            try {
                return lookupEndpoints(apiRootUri, config.getToken(), apiCodec);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private GithubApiEndpoints lookupEndpoints(URI apiRootUri, String token,
            JsonCodec<GithubApiEndpoints> catalogCodec)
            throws IOException
    {
        byte[] json = getContent(apiRootUri, token);
        return catalogCodec.fromJson(json);
    }

    private Supplier<List<GithubSchema>> schemasSupplier(
            final JsonCodec<List<GithubSchema>> schmemaCodec,
            final Supplier<GithubApiEndpoints> endpoints,
            final GithubConfig config)
            throws URITemplateException, URISyntaxException
    {
        URITemplate repoTemplate;
        VariableMapBuilder variables = VariableMap.newBuilder()
                .addScalarValue("username", config.getUsername())
                .addScalarValue("token", config.getToken());

        if (config.getOwnerType().equals("orgs")) {
            repoTemplate = endpoints.get().getOrgRepoTemplate();
            variables.addScalarValue("org", config.getOwner());
        }
        else {
            repoTemplate = endpoints.get().getUserRepoTemplate();
            variables.addScalarValue("user", config.getOwner());
        }

        URI schemaUri = repoTemplate.toURI(variables.freeze());

        return () -> {
            try {
                return lookupSchemas(schemaUri, config.getToken(), schmemaCodec);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private List<GithubSchema> lookupSchemas(URI schemaUri, String token,
            JsonCodec<List<GithubSchema>> schemaCodec)
            throws IOException
    {
        byte[] json = getContent(schemaUri, token);
        return schemaCodec.fromJson(json);
    }
}
