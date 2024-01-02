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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.function.Supplier;

/** Simple data generator, generating records with a fixed delay. */
public class FixedDelaySource<T> implements SourceFunction<T>, ResultTypeQueryable<T> {
    private volatile boolean isRunning = true;

    private final Supplier<T> eventGenerator;
    private final long pauseMillis;
    private final Class<T> payloadClass;

    public FixedDelaySource(Class<T> payloadClass, Supplier<T> eventGenerator, long pauseMillis) {
        Preconditions.checkArgument(
                pauseMillis > 0,
                "Pause between time-series must be > 0"); // If zero, the source may generate
        // duplicate timestamps
        this.eventGenerator = eventGenerator;
        this.pauseMillis = pauseMillis;
        this.payloadClass = payloadClass;
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        while (isRunning) {
            T event = eventGenerator.get();
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
