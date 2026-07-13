"""
mitmproxy script — 完整记录 Open WebUI → adapter 的每个请求和响应
用法: mitmdump --mode reverse:http://localhost:8001 --listen-port 8888 -s watch.py
"""
import json
import textwrap

SEP = "─" * 60


def _fmt(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def request(flow):
    path = flow.request.path.split("?")[0].rstrip("/")
    method = flow.request.method

    print(f"\n{SEP}")
    print(f"▶ {method} {path}")

    if method == "POST" and flow.request.content:
        try:
            body = json.loads(flow.request.content)
            messages = body.get("messages", [])
            print(f"  messages ({len(messages)} 条):")
            for i, m in enumerate(messages):
                role = m.get("role", "?").upper()
                content = m.get("content", "")
                # 截断超长内容（超过 300 字符显示省略）
                preview = content if len(content) <= 300 else content[:300] + f"... [{len(content)-300} 字符省略]"
                preview = textwrap.indent(preview, "    ")
                print(f"  [{i+1}] {role}:\n{preview}")
        except Exception:
            print(f"  raw body: {flow.request.content[:500]}")


def response(flow):
    status = flow.response.status_code
    ct = flow.response.headers.get("content-type", "")

    if "event-stream" in ct:
        # SSE 流：提取所有 text delta 拼成完整回复
        text = ""
        for line in flow.response.content.decode("utf-8", errors="replace").splitlines():
            if line.startswith("data: ") and line != "data: [DONE]":
                try:
                    chunk = json.loads(line[6:])
                    delta = chunk["choices"][0]["delta"].get("content", "")
                    text += delta
                except Exception:
                    pass
        print(f"◀ {status} (SSE) assistant回复 ({len(text)} 字符):")
        preview = text if len(text) <= 500 else text[:500] + f"... [{len(text)-500} 字符省略]"
        print(textwrap.indent(preview, "  "))
    else:
        try:
            body = json.loads(flow.response.content)
            print(f"◀ {status} (JSON):")
            print(textwrap.indent(_fmt(body)[:500], "  "))
        except Exception:
            print(f"◀ {status}")
