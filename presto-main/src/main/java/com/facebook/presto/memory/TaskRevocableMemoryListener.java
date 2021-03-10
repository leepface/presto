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
package com.facebook.presto.memory;

import com.facebook.presto.execution.TaskId;

import java.util.function.BiConsumer;

public interface TaskRevocableMemoryListener
{
    /**
     * Listener function that is called when a Task reserves
     * memory in a given MemoryPool successfully
     *
     * @param taskId the {@link TaskId} of the task that reserved the memory
     * @param memoryPool the {@link MemoryPool} where the reservation took place
     */
    void onMemoryReserved(TaskId taskId, MemoryPool memoryPool);

    static TaskRevocableMemoryListener onMemoryReserved(BiConsumer<TaskId, ? super MemoryPool> action)
    {
        return action::accept;
    }
}
