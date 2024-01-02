/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.amazonaws.services.managedflink.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

/**
 * This source can produce distinct events for each parallel sub-task. The eventGenerator function
 * receives the number of parallel sub-tasks and index of the sub-task it is currently running on.
 */
public class ParallelFixedPaceSource<T> extends RichParallelSourceFunction<T>
        implements ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelFixedPaceSource.class);

    private final long pauseMillis;
    private final Class<T> payloadClass;

    /**
     * Event generator. Expects the number of parallel sub-tasks, and the index of the current
     * sub-task (0-based)
     */
    private final BiFunction<Integer, Integer, T> eventGenerator;

    private volatile boolean isRunning = true;

    public ParallelFixedPaceSource(
            Class<T> payloadClass,
            BiFunction<Integer, Integer, T> eventGenerator,
            long pauseMillis) {
        this.payloadClass = payloadClass;
        this.eventGenerator = eventGenerator;
        this.pauseMillis = pauseMillis;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        int parallelInstance = getRuntimeContext().getIndexOfThisSubtask();
        int numberOfParallelInstances = getRuntimeContext().getNumberOfParallelSubtasks();
        LOG.info("Parallel source: subtask {} of {}.", parallelInstance, numberOfParallelInstances);

        while (isRunning) {
            T event = eventGenerator.apply(numberOfParallelInstances, parallelInstance);
            sourceContext.collect(event);
            Thread.sleep(pauseMillis);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return new GenericTypeInfo<>(payloadClass);
    }
}
