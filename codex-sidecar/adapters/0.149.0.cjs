"use strict";

const { createAdapter } = require("./common.cjs");
const { createStableDomain } = require("./domain.cjs");

const domain = createStableDomain({
  upstreamVersion: "0.149.0",
  allowFunctionCallOutput: false,
});

// Every promoted 0.149.0 method is declared here. Adding a method requires a
// schema review and an explicit stable-domain projector.
module.exports = createAdapter({
  upstreamVersion: "0.149.0",
  adapterVersion: "0.149.0",
  requestProjectors: {
    initialize: domain.initializeRequest,
    "thread/start": domain.threadStartRequest,
    "thread/list": domain.threadListRequest,
    "thread/read": domain.threadReadRequest,
    "thread/resume": domain.threadResumeRequest,
    "thread/unsubscribe": domain.threadUnsubscribeRequest,
    "thread/turns/list": domain.threadTurnsListRequest,
    "thread/items/list": domain.threadItemsListRequest,
    "thread/queue/add": domain.threadQueueAddRequest,
    "thread/queue/list": domain.threadQueueListRequest,
    "thread/queue/start": domain.threadQueueStartRequest,
    "turn/start": domain.turnStartRequest,
    "turn/steer": domain.turnSteerRequest,
    "turn/interrupt": domain.turnInterruptRequest,
  },
  responseProjectors: {
    initialize: domain.initializeResponse,
    "thread/start": domain.threadStartResponse,
    "thread/list": domain.threadListResponse,
    "thread/read": domain.threadReadResponse,
    "thread/resume": domain.threadResumeResponse,
    "thread/unsubscribe": domain.threadUnsubscribeResponse,
    "thread/turns/list": domain.threadTurnsListResponse,
    "thread/items/list": domain.threadItemsListResponse,
    "thread/queue/add": domain.threadQueueAddResponse,
    "thread/queue/list": domain.threadQueueListResponse,
    "thread/queue/start": domain.threadQueueStartResponse,
    "turn/start": domain.turnStartResponse,
    "turn/steer": domain.turnSteerResponse,
    "turn/interrupt": domain.turnInterruptResponse,
  },
  localNotificationProjectors: {
    initialized: domain.initializedNotification,
  },
  notificationProjectors: {
    "account/rateLimits/updated": domain.accountRateLimitsUpdatedNotification,
    "remoteControl/status/changed": domain.remoteControlStatusChangedNotification,
    "thread/goal/cleared": domain.threadGoalClearedNotification,
    "thread/settings/updated": domain.threadSettingsUpdatedNotification,
    "thread/started": domain.threadStartedNotification,
    "thread/status/changed": domain.threadStatusChangedNotification,
    "thread/queue/changed": domain.threadQueueChangedNotification,
    "turn/started": domain.turnStartedNotification,
    "item/started": domain.itemStartedNotification,
    "item/agentMessage/delta": domain.agentMessageDeltaNotification,
    "item/commandExecution/outputDelta": domain.commandOutputDeltaNotification,
    "item/completed": domain.itemCompletedNotification,
    "thread/tokenUsage/updated": domain.tokenUsageNotification,
    "serverRequest/resolved": domain.serverRequestResolvedNotification,
    error: domain.errorNotification,
    "turn/completed": domain.turnCompletedNotification,
  },
  serverRequestProjectors: {
    "item/tool/call": domain.dynamicToolRequest,
    "item/commandExecution/requestApproval": domain.commandApprovalRequest,
    "item/fileChange/requestApproval": domain.fileApprovalRequest,
    "item/permissions/requestApproval": domain.permissionsApprovalRequest,
  },
  serverResponseProjectors: {
    "item/tool/call": domain.dynamicToolResponse,
    "item/commandExecution/requestApproval": domain.commandApprovalResponse,
    "item/fileChange/requestApproval": domain.fileApprovalResponse,
    "item/permissions/requestApproval": domain.permissionsApprovalResponse,
  },
});
