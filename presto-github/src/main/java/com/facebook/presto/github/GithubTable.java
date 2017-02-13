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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.github.fge.uritemplate.URITemplate;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class GithubTable
{
    private final String name;
    private final List<GithubColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final URITemplate sourceTemplate;
    private final String token;

    @JsonCreator
    public GithubTable(
            String name,
            List<GithubColumn> columns,
            URITemplate sourceTemplate,
            String token)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sourceTemplate = requireNonNull(sourceTemplate);
        this.token = requireNonNull(token);

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (GithubColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    public String getName()
    {
        return name;
    }

    public List<GithubColumn> getColumns()
    {
        return columns;
    }

    public URITemplate getSourceTemplate()
    {
        return sourceTemplate;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public String getToken()
    {
        return token;
    }
}
