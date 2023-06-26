/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.status.history.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;

public class QuestDbContext {
    private final CairoEngine engine;

    public QuestDbContext(final CairoEngine engine) {
        this.engine = engine;
    }

    public CairoEngine getEngine() {
        return engine;
    }

    public CairoConfiguration getConfiguration() {
        return engine.getConfiguration();
    }

    public SqlExecutionContext getSqlExecutionContext() {
        return SqlExecutionContextFactory.getInstance(engine);
    }

    public SqlCompiler getCompiler() {
        return new SqlCompiler(engine);
    }

    public void close() {
        engine.close();
    }
}
