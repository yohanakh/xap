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

package com.j_spaces.kernel.pool;

/**
 * Created by Barak Bar Orion
 * on 9/5/17.
 *
 * @since 12.1
 */
class DummyResource extends Resource{
    private final int ID;
    private boolean released = false;

    DummyResource(int id) {
        this.ID = id;
    }

    public int getId() {
        return ID;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DummyResource that = (DummyResource) o;

        return ID == that.ID;
    }

    @Override
    public void release() {
        super.release();
        released = true;
    }

    public boolean isReleased() {
        return released;
    }

    @Override
    public int hashCode() {
        return ID;
    }

    @Override
    public String toString() {
        //noinspection StringBufferReplaceableByString
        final StringBuilder sb = new StringBuilder("DummyResource{");
        sb.append("ID=").append(ID);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void clear() {
    }
}
