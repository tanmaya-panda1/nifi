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

package org.apache.nifi.python;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PythonProcessConfig {

    private final String pythonCommand;
    private final File pythonFrameworkDirectory;
    private final List<File> pythonExtensionsDirectories;
    private final File pythonWorkingDirectory;
    private final File pythonLogsDirectory;
    private final Duration commsTimeout;
    private final int maxPythonProcesses;
    private final int maxPythonProcessesPerType;

    private PythonProcessConfig(final Builder builder) {
        this.pythonCommand = builder.pythonCommand;
        this.pythonFrameworkDirectory = builder.pythonFrameworkDirectory;
        this.pythonExtensionsDirectories = builder.pythonExtensionsDirectories;
        this.pythonLogsDirectory = builder.pythonLogsDirectory;
        this.pythonWorkingDirectory = builder.pythonWorkingDirectory;
        this.commsTimeout = builder.commsTimeout;
        this.maxPythonProcesses = builder.maxProcesses;
        this.maxPythonProcessesPerType = builder.maxProcessesPerType;
    }

    public String getPythonCommand() {
        return pythonCommand;
    }

    public File getPythonFrameworkDirectory() {
        return pythonFrameworkDirectory;
    }

    public List<File> getPythonExtensionsDirectories() {
        return pythonExtensionsDirectories;
    }

    public File getPythonLogsDirectory() {
        return pythonLogsDirectory;
    }

    public File getPythonWorkingDirectory() {
        return pythonWorkingDirectory;
    }

    public Duration getCommsTimeout() {
        return commsTimeout;
    }

    public int getMaxPythonProcesses() {
        return maxPythonProcesses;
    }

    public int getMaxPythonProcessesPerType() {
        return maxPythonProcessesPerType;
    }

    public static class Builder {
        private String pythonCommand = "python3";
        private File pythonFrameworkDirectory = new File("python/framework");
        private List<File> pythonExtensionsDirectories = Collections.singletonList(new File("python/extensions"));
        private File pythonLogsDirectory = new File("./logs");
        private File pythonWorkingDirectory = new File("python");
        private Duration commsTimeout = Duration.ofSeconds(0);
        private int maxProcesses;
        private int maxProcessesPerType;

        public Builder pythonCommand(final String command) {
            this.pythonCommand = command;
            return this;
        }

        public Builder pythonFrameworkDirectory(final File pythonFrameworkDirectory) {
            this.pythonFrameworkDirectory = pythonFrameworkDirectory;
            return this;
        }

        public Builder pythonExtensionsDirectories(final Collection<File> pythonExtensionsDirectories) {
            this.pythonExtensionsDirectories = new ArrayList<>(pythonExtensionsDirectories);
            return this;
        }

        public Builder pythonWorkingDirectory(final File pythonWorkingDirectory) {
            this.pythonWorkingDirectory = pythonWorkingDirectory;
            return this;
        }

        public Builder commsTimeout(final Duration duration) {
            if (duration == null) {
                return this;
            }

            this.commsTimeout = duration;
            return this;
        }

        public Builder maxPythonProcesses(final int maxProcesses) {
            if (maxProcesses < 0) {
                throw new IllegalArgumentException("Cannot configure max number of Python Processes to be less than 0");
            }

            this.maxProcesses = maxProcesses;
            return this;
        }

        public Builder maxPythonProcessesPerType(final int maxProcessesPerType) {
            if (maxProcessesPerType < 1) {
                throw new IllegalArgumentException("Cannot configure max number of Python Processes to be less than 1");
            }

            this.maxProcessesPerType = maxProcessesPerType;
            return this;
        }

        public Builder pythonLogsDirectory(final File logsDirectory) {
            this.pythonLogsDirectory = logsDirectory;
            return this;
        }

        public PythonProcessConfig build() {
            return new PythonProcessConfig(this);
        }
    }
}
