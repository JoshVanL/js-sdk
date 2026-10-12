/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import { createClient } from "@connectrpc/connect";
import { TaskHubWorker } from "../../../src/workflow/engine/transport/TaskHubWorker";

jest.mock("@connectrpc/connect", () => ({
  ...jest.requireActual("@connectrpc/connect"),
  createClient: jest.fn(),
}));

describe("TaskHubWorker", () => {
  it("runs workflow work items while every activity slot is busy", async () => {
    // Ten activities that start a child workflow and wait for it, as an
    // activity calling scheduleNewWorkflow does: the sidecar only answers
    // once the child's first workflow work item has run on this worker.
    const activity = { request: { case: "activityRequest", value: { name: "a" } }, completionToken: "a" };
    const child = { request: { case: "workflowRequest", value: { instanceId: "child" } }, completionToken: "w" };
    const items = [...Array(10).fill(activity), child];

    const getWorkItems = jest.fn((_req: unknown, opts: { signal: AbortSignal }) => ({
      [Symbol.asyncIterator]: () => ({
        next: () =>
          items.length > 0
            ? Promise.resolve({ done: false, value: items.shift() })
            : new Promise((resolve) => opts.signal.addEventListener("abort", () => resolve({ done: true }))),
      }),
    }));
    (createClient as jest.Mock).mockReturnValue({ hello: jest.fn().mockResolvedValue({}), getWorkItems });

    let releaseActivities = () => {};
    const activitiesBlocked = new Promise<void>((resolve) => (releaseActivities = resolve));
    const worker = new TaskHubWorker("localhost:50001");
    const executeActivity = jest.spyOn(worker as any, "executeActivity").mockReturnValue(activitiesBlocked);
    const executeOrchestrator = jest.spyOn(worker as any, "executeOrchestrator").mockResolvedValue(undefined);

    await worker.start();
    const deadline = Date.now() + 2000;
    while (executeOrchestrator.mock.calls.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    expect(executeActivity).toHaveBeenCalledTimes(10);
    expect(executeOrchestrator).toHaveBeenCalledTimes(1);

    releaseActivities();
    await worker.stop();
  }, 10000);
});
