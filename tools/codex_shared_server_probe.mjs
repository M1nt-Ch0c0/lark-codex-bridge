#!/usr/bin/env node

// Read-only research probe for Issue #8. This intentionally supports only an
// explicitly acknowledged, unauthenticated loopback listener backed by an
// isolated Codex profile. Production external-endpoint auth is not implemented
// here. The probe never prints endpoint, thread, response, or error contents.

const endpoint = process.env.CODEX_SHARED_PROBE_ENDPOINT;
const isolated = process.env.CODEX_SHARED_PROBE_ISOLATED === "1";
const timeoutMs = 5_000;

function reportFailure(stage) {
  process.exitCode = 1;
  process.stdout.write(`${JSON.stringify({ ok: false, stage })}\n`, () => {
    process.exit();
  });
}

function validateEndpoint(value) {
  if (!isolated || typeof value !== "string" || value.length > 2_048) {
    return null;
  }

  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    return null;
  }

  const loopback = parsed.hostname === "127.0.0.1" || parsed.hostname === "[::1]";
  const clean =
    parsed.protocol === "ws:" &&
    loopback &&
    parsed.username === "" &&
    parsed.password === "" &&
    parsed.search === "" &&
    parsed.hash === "" &&
    (parsed.pathname === "" || parsed.pathname === "/");
  return clean ? parsed : null;
}

function connectAndList(url, label) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    let settled = false;
    const timer = setTimeout(() => finish(new Error("timeout")), timeoutMs);

    function finish(error) {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      if (error) {
        try {
          socket.close();
        } catch {
          // Nothing else owns this client connection.
        }
        reject(error);
      } else {
        resolve(socket);
      }
    }

    socket.addEventListener("open", () => {
      socket.send(
        JSON.stringify({
          method: "initialize",
          id: 1,
          params: {
            clientInfo: {
              name: `lark_codex_bridge_issue8_probe_${label}`,
              title: "Issue 8 read-only probe",
              version: "0.0.0",
            },
          },
        }),
      );
    });

    socket.addEventListener("message", (event) => {
      let message;
      try {
        message = JSON.parse(String(event.data));
      } catch {
        finish(new Error("decode"));
        return;
      }

      if (message.id === 1 && message.result) {
        socket.send(JSON.stringify({ method: "initialized", params: {} }));
        socket.send(
          JSON.stringify({ method: "thread/list", id: 2, params: { limit: 1 } }),
        );
      } else if (message.id === 2 && Array.isArray(message.result?.data)) {
        finish();
      } else if (message.id === 1 || message.id === 2) {
        finish(new Error("rpc"));
      }
    });

    socket.addEventListener("error", () => finish(new Error("socket")));
    socket.addEventListener("close", () => {
      if (!settled) {
        finish(new Error("closed"));
      }
    });
  });
}

function closeSocket(socket) {
  return new Promise((resolve) => {
    if (socket.readyState === WebSocket.CLOSED) {
      resolve();
      return;
    }
    const timer = setTimeout(resolve, timeoutMs);
    socket.addEventListener(
      "close",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
    socket.close();
  });
}

const parsed = validateEndpoint(endpoint);
if (!parsed) {
  reportFailure("configuration");
} else {
  try {
    const [first, second] = await Promise.all([
      connectAndList(parsed.href, "a"),
      connectAndList(parsed.href, "b"),
    ]);
    await Promise.all([closeSocket(first), closeSocket(second)]);

    const healthUrl = new URL("/healthz", parsed);
    healthUrl.protocol = "http:";
    const healthAfterDisconnect = (
      await fetch(healthUrl, { signal: AbortSignal.timeout(timeoutMs) })
    ).ok;

    const third = await connectAndList(parsed.href, "c");
    await closeSocket(third);

    process.exitCode = healthAfterDisconnect ? 0 : 1;
    process.stdout.write(
      `${JSON.stringify({
        ok: healthAfterDisconnect,
        twoClientsInitialized: true,
        healthAfterClientDisconnect: healthAfterDisconnect,
        freshClientInitialized: true,
      })}\n`,
      () => {
        process.exit();
      },
    );
  } catch {
    reportFailure("protocol");
  }
}
