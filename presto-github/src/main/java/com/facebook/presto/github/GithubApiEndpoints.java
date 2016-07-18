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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.fge.uritemplate.URITemplate;
import com.github.fge.uritemplate.URITemplateParseException;

public class GithubApiEndpoints
{
    URITemplate userRepoTemplate;
    URITemplate orgRepoTemplate;

    @JsonCreator
    public GithubApiEndpoints(
            @JsonProperty("user_repositories_url") String userRepoUriTemplate,
            @JsonProperty("organization_repositories_url") String orgRepoUriTemplate)
            throws URITemplateParseException
    {
        // Add basic authentication
        userRepoUriTemplate = userRepoUriTemplate
                .replace("https://", "https://{username}:{token}@");
        this.userRepoTemplate = new URITemplate(userRepoUriTemplate);

        orgRepoUriTemplate = orgRepoUriTemplate
                .replace("https://", "https://{username}:{token}@");
        this.orgRepoTemplate = new URITemplate(orgRepoUriTemplate);
    }

    public URITemplate getUserRepoTemplate()
    {
        return userRepoTemplate;
    }

    public URITemplate getOrgRepoTemplate()
    {
        return orgRepoTemplate;
    }
}
