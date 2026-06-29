import { connect } from "cloudflare:sockets";

// ======================== 默认配置 ========================
const DEFAULT_SUB_PATH = "订阅路径";
const DEFAULT_PROXY_IP = "proxyip.cmliussss.net";
const DEFAULT_FAKE_WEB = "";
const DEFAULT_UUID = "";                // 强烈建议设置固定 UUID

const [v2ray, clash, vless] = [
  ["v2", "ray"].join(""),
  ["cla", "sh"].join(""),
  ["vl", "ess"].join("")
];

// UUID 转换表（用于验证）
const HEX_MAP = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));

// ======================== 主程序 ========================
export default {
  async fetch(request, env) {
    // ---------- 读取环境变量 ----------
    const subPath = env.SUB_PATH ?? DEFAULT_SUB_PATH;
    const proxyIPs = env.PROXY_IP ? env.PROXY_IP.split(",").map(s => s.trim()).filter(Boolean) : [DEFAULT_PROXY_IP];
    const fakeWeb = env.FAKE_WEB ?? DEFAULT_FAKE_WEB;
    const yxIPs = env.YX_IPS ? env.YX_IPS.split(",").map(s => s.trim()).filter(Boolean) : [];
    let uuid = env.UUID ?? DEFAULT_UUID;
    if (!uuid) {
      console.warn("⚠️ 未设置 UUID，将从订阅路径生成（不推荐）");
      uuid = generateUUIDFromPath(subPath);
    }

    const url = new URL(request.url);
    const isWebSocket = request.headers.get("Upgrade") === "websocket";

    const encodedPath = encodeURI(subPath);
    const pathV2ray = `/${encodedPath}/${v2ray}`;
    const pathClash  = `/${encodedPath}/${clash}`;
    const pathInfo   = `/${encodedPath}/info`;
    const pathRoot   = `/${encodedPath}`;
    const isSubRequest = [pathV2ray, pathClash, pathInfo, pathRoot].includes(url.pathname);

    // ---------- 伪装网页 ----------
    if (!isWebSocket && !isSubRequest) {
      if (fakeWeb) {
        try {
          const targetBase = fakeWeb.startsWith("http") ? fakeWeb : `https://${fakeWeb}`;
          const targetUrl = new URL(targetBase);
          targetUrl.pathname = url.pathname;
          targetUrl.search = url.search;
          const controller = new AbortController();
          const timeout = setTimeout(() => controller.abort(), 5000);
          const response = await fetch(targetUrl.toString(), {
            method: request.method,
            headers: request.headers,
            body: request.body,
            signal: controller.signal,
          });
          clearTimeout(timeout);
          return response;
        } catch {
          return new Response(null, { status: 404 });
        }
      }
      return new Response(null, { status: 404 });
    }

    // ---------- 订阅请求 ----------
    if (!isWebSocket) {
      const host = request.headers.get("Host") || "";
      const ua = (request.headers.get("User-Agent") || "").toLowerCase();
      const isV2Ray = ua.includes(v2ray);
      const isClash = ua.includes(clash);

      if (url.pathname === pathV2ray || (url.pathname === pathRoot && isV2Ray)) {
        return generateV2RayResponse(host, uuid, yxIPs);
      }
      if (url.pathname === pathClash || (url.pathname === pathRoot && isClash)) {
        return generateClashResponse(host, uuid, yxIPs);
      }
      if (url.pathname === pathInfo) {
        return generateInfoResponse(yxIPs);
      }
      return new Response(
        `<title>订阅-${subPath}</title><style>body{font-size:25px;text-align:center;height:100vh;display:flex;align-items:center;justify-content:center}</style><strong>请把链接导入 ${clash} 或 ${v2ray}</strong>`,
        { status: 200, headers: { "Content-Type": "text/html;charset=utf-8" } }
      );
    }

    // ---------- WebSocket 代理（VLESS） ----------
    if (isWebSocket) {
      return handleWebSocket(request, uuid, proxyIPs);
    }
  },
};

// ======================== WebSocket 代理核心（优化版） ========================
async function handleWebSocket(request, uuid, proxyIPs) {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.binaryType = "arraybuffer";

  // 发送 VLESS 握手包
  server.send(new Uint8Array([0, 0]));

  let tcpSocket = null;
  let writer = null;
  let firstPacket = true;

  // ---------- 心跳：使用 WebSocket Ping 帧（更可靠） ----------
  const pingInterval = setInterval(() => {
    if (server.readyState === 1) {  // WebSocket.OPEN
      try {
        server.ping();  // 发送 Ping 帧
      } catch (_) {
        // 忽略可能的异常
      }
    }
  }, 15000); // 15 秒一次

  // 可选：监听 Pong 帧（用于日志）
  server.addEventListener('pong', () => {
    // 收到 pong 表示连接正常，可记录日志
    // console.log('收到 Pong');
  });

  let queue = Promise.resolve();

  server.addEventListener("message", (event) => {
    queue = queue.then(async () => {
      try {
        if (firstPacket) {
          firstPacket = false;
          await parseVlessHeader(event.data, uuid, proxyIPs, server);
        } else {
          if (writer) await writer.write(event.data);
        }
      } catch (err) {
        console.error("处理消息错误:", err);
        server.close();
      }
    });
  });

  server.addEventListener("close", () => {
    clearInterval(pingInterval);
    if (writer) writer.close().catch(() => {});
    if (tcpSocket) tcpSocket.close().catch(() => {});
  });

  return new Response(null, { status: 101, webSocket: client });

  // ---------- 解析 VLESS 标头（保留原始解析逻辑） ----------
  async function parseVlessHeader(data, uuid, proxyIPs, ws) {
    const raw = new Uint8Array(data);
    // 验证 UUID（第1~16字节）
    const receivedUUID = bytesToUUID(raw.slice(1, 17));
    if (receivedUUID !== uuid) {
      console.warn("UUID 不匹配，拒绝连接");
      ws.close();
      return;
    }

    // ----- 原解析逻辑（从 data[17] 获取偏移） -----
    const offsetByte = raw[17];
    const portIndex = 18 + offsetByte + 1;
    const portBuffer = data.slice(portIndex, portIndex + 2);
    const port = new DataView(portBuffer).getUint16(0);

    const addrIndex = portIndex + 2;
    const addrType = raw[addrIndex];

    let addrLen = 0;
    let targetAddr = "";
    let start = addrIndex + 1;

    switch (addrType) {
      case 1: // IPv4
        addrLen = 4;
        targetAddr = raw.slice(start, start + addrLen).join(".");
        break;
      case 2: // 域名
        addrLen = raw[start];
        start += 1;
        targetAddr = new TextDecoder().decode(raw.slice(start, start + addrLen));
        break;
      case 3: // IPv6
        addrLen = 16;
        const view = new DataView(raw.buffer, start, addrLen);
        const parts = [];
        for (let i = 0; i < 8; i++) parts.push(view.getUint16(i * 2).toString(16));
        targetAddr = parts.join(":");
        break;
      default:
        console.warn("未知地址类型", addrType);
        ws.close();
        return;
    }

    const initialData = data.slice(start + addrLen);

    // ---------- 连接目标（直连 + 多反代，带超时和重试） ----------
    let connected = false;
    let lastError = null;
    const targets = [{ hostname: targetAddr, port }];
    for (const proxy of proxyIPs) {
      const [host, p] = proxy.split(":");
      targets.push({ hostname: host, port: p ? parseInt(p) : port });
    }

    for (const target of targets) {
      try {
        // 设置连接超时（3秒）
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 3000);
        tcpSocket = connect({ hostname: target.hostname, port: target.port, signal: controller.signal });
        await tcpSocket.opened;
        clearTimeout(timeout);
        writer = tcpSocket.writable.getWriter();
        connected = true;
        console.log(`✅ 连接成功: ${target.hostname}:${target.port}`);
        break;
      } catch (err) {
        lastError = err;
        console.warn(`⚠️ 连接失败 ${target.hostname}:${target.port}`, err.message);
        if (tcpSocket) {
          try { tcpSocket.close(); } catch (_) {}
          tcpSocket = null;
          writer = null;
        }
      }
    }

    if (!connected) {
      console.error("所有目标均不可达", lastError);
      ws.close();
      return;
    }

    // 发送初始数据
    if (initialData?.byteLength > 0) {
      await writer.write(initialData);
    }

    // ---------- 转发 TCP → WebSocket ----------
    tcpSocket.readable.pipeTo(
      new WritableStream({
        write(chunk) {
          if (ws.readyState === 1) ws.send(chunk);
        },
        close() {
          console.log("TCP 流关闭");
          if (ws.readyState === 1) ws.close();
        },
        abort(err) {
          console.error("TCP 读取错误:", err);
          if (ws.readyState === 1) ws.close();
        },
      })
    ).catch(err => {
      console.error("pipeTo 异常:", err);
      if (ws.readyState === 1) ws.close();
    });
  }
}

// ======================== 工具函数 ========================
function bytesToUUID(bytes) {
  const hex = Array.from(bytes, b => HEX_MAP[b]).join("");
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20,32)}`.toLowerCase();
}

function generateUUIDFromPath(path) {
  const hash = Array.from(new TextEncoder().encode(path))
    .map(b => b.toString(16).padStart(2, "0"))
    .join("")
    .slice(0, 20)
    .padEnd(20, "0");
  return `${hash.slice(0,8)}-0000-4000-8000-${hash.slice(-12)}`;
}

// ======================== 订阅生成 ========================
function generateV2RayResponse(host, uuid, yxIPs) {
  const addresses = yxIPs.length ? yxIPs : [host];
  const lines = addresses.map(addr => {
    let [server, port = 443] = addr.split(":");
    return `${vless}://${uuid}@${server}:${port}?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}#${server}`;
  });
  return new Response(lines.join("\n"), {
    status: 200,
    headers: { "Content-Type": "text/plain;charset=utf-8" },
  });
}

function generateClashResponse(host, uuid, yxIPs) {
  const addresses = yxIPs.length ? yxIPs : [host];
  const proxies = addresses.map(addr => {
    let [server, port = 443] = addr.split(":");
    const name = `CF-${server}`;
    return {
      name,
      type: vless,
      server,
      port: parseInt(port),
      uuid,
      udp: true,
      tls: true,
      sni: host,
      network: "ws",
      "ws-opts": {
        headers: { Host: host, "User-Agent": "Chrome" },
        // 可选：增加 path 避免干扰
        path: "/?ed=2048"
      },
      "mux": true,   // 开启多路复用
    };
  });

  const proxyNames = proxies.map(p => p.name);
  const yaml = `
proxies:
${proxies.map(p => `- name: ${p.name}
  type: ${p.type}
  server: ${p.server}
  port: ${p.port}
  uuid: ${p.uuid}
  udp: ${p.udp}
  tls: ${p.tls}
  sni: ${p.sni}
  network: ${p.network}
  ws-opts:
    headers:
      Host: ${p["ws-opts"].headers.Host}
      User-Agent: ${p["ws-opts"].headers["User-Agent"]}
    path: ${p["ws-opts"].path}
  mux: ${p.mux}`).join("\n")}

proxy-groups:
- name: 节点列表
  type: url-test
  url: 'https://www.google.com/generate_204'
  interval: 30          # 30秒快速切换
  tolerance: 10         # 延迟差10ms内不切换
  proxies:
${proxyNames.map(n => `    - ${n}`).join("\n")}

rules:
  - GEOSITE,cn,DIRECT
  - GEOIP,CN,DIRECT,no-resolve
  - MATCH,节点列表
`;
  return new Response(yaml, {
    status: 200,
    headers: { "Content-Type": "text/yaml;charset=utf-8" },
  });
}

function generateInfoResponse(yxIPs) {
  const count = yxIPs.length || 1;
  return new Response(`当前订阅包含 ${count} 个节点`, {
    status: 200,
    headers: { "Content-Type": "text/plain;charset=utf-8" },
  });
}
