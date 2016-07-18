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

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public final class GithubQueryRunner
{
    private GithubQueryRunner() {}

    public static DistributedQueryRunner createGithubQueryRunner(String ownerType, String owner)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 2);
        queryRunner.installPlugin(new GithubPlugin());
        queryRunner.createCatalog("github", "github", ImmutableMap.of(
                "github-owner", owner,
                "github-owner-type", ownerType,
                "github-username", GithubApiCredentials.NAME,
                "github-token", GithubApiCredentials.TOKEN));
        return queryRunner;
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("github")
                .setSchema("presto")
                .build();
    }
}
