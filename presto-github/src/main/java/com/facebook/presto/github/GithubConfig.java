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

import com.github.fge.uritemplate.URITemplate;
import com.github.fge.uritemplate.URITemplateParseException;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class GithubConfig
{
    private URITemplate apiRoot = new URITemplate("https://{username}:{token}@api.github.com");
    private String owner;
    private String ownerType = "orgs";
    private String username;
    private String token;

    public GithubConfig()
            throws URITemplateParseException
    {}

    @NotNull
    public URITemplate getApiRoot()
    {
        return apiRoot;
    }

    @NotNull
    public String getOwner()
    {
        return owner;
    }

    @NotNull
    public String getOwnerType()
    {
        return ownerType;
    }

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @NotNull
    public String getToken()
    {
        return token;
    }

    @Config("github-apiroot")
    public GithubConfig setApiRoot(String apiRoot)
            throws URITemplateParseException
    {
        this.apiRoot = new URITemplate(apiRoot.replace("https://", "https://{username}:{token}@"));
        return this;
    }

    @Config("github-owner")
    public GithubConfig setOwner(String owner)
    {
        this.owner = owner;
        return this;
    }

    @Config("github-owner-type")
    public GithubConfig setOwnerType(String ownerType)
    {
        this.ownerType = ownerType;
        return this;
    }

    @Config("github-username")
    public GithubConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("github-token")
    public GithubConfig setToken(String token)
    {
        this.token = token;
        return this;
    }
}
