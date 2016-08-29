/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.core;

import java.io.IOException;

/**
 * Created by Barak Bar Orion
 * on 8/28/16.
 *
 * @since 12.0
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public interface HeapDumpMBean {

    boolean isEnabled();
    void setEnabled(boolean enabled);

    int getMaxHeaps();
    void setMaxHeaps(int maxHeaps);

    int getCurrentHeaps();
    void setCurrentHeaps(int currentHeaps);

    long getQuietPeriod();
    void setQuietPeriod(long millis);


    long getPid();
    String[] getCmd();

    void setCmd(String[] cmd);

    String getOutputFileName();

    void setOutputFileName(String name);

    String createHeapDump() throws IOException, InterruptedException;

    boolean onMemoryShortage() throws IOException, InterruptedException;
}
