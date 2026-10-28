def _lan_ip() -> str:
    """取本机主网卡 IPv4（仅用于提示浏览器/手机访问地址；失败返回 127.0.0.1）。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 53))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def log_html(response):
    """浏览器全屏实时日志台页面（HTTP RPC：``/log_html(p)``，根路径自动跳转）。

    页面无任何上下工具栏：整个视口就是一个黑底等宽控制台，WebSocket 接
    /wslog，先收快照再收增量，滚到底部自动跟随、向上翻则暂停。
    """
    response.set_header("Content-Type", "text/html; charset=utf-8")
    response.set_header("Cache-Control", "no-store")
    response.set_data(_LOG_PAGE)


_LOG_PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pty local log</title>
<style>
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; padding: 0; overflow: hidden;
  background: #0c0c0c; color: #d8d8d8; }
#log {
  height: 100vh; width: 100vw; margin: 0; padding: 6px 10px 10px;
  border: 0; outline: 0; overflow: auto; white-space: pre-wrap;
  word-break: break-all; scrollbar-width: thin;
  font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, "Cascadia Mono", monospace;
}
#jump {
  position: fixed; left: 50%; bottom: 14px; transform: translateX(-50%);
  padding: 5px 14px; border-radius: 14px; display: none; cursor: pointer;
  background: rgba(60, 120, 200, .92); color: #fff; font: 12px/1.4 system-ui, sans-serif;
  user-select: none; box-shadow: 0 2px 10px rgba(0,0,0,.5);
}
</style>
</head>
<body>
<div id="log"></div>
<div id="jump">新日志 ↓ 点击回到底部</div>
<script>
const logEl = document.getElementById('log');
const jumpEl = document.getElementById('jump');
let socket, reconnectTimer, follow = true, documentCount = 0;

// ---- 极简 ANSI SGR 着色（其余 CSI/OSC 序列剥掉，保证只做日志查看不做终端仿真） ----
const SGR = {
  0:['',''], 1:['font-weight:bold',''], 2:['opacity:.75',''], 3:['font-style:italic',''],
  4:['text-decoration:underline',''], 9:['text-decoration:line-through',''],
  22:['font-weight:normal;opacity:1;font-style:normal',''], 23:['font-style:normal',''],
  24:['text-decoration:none',''], 27:['',''], 39:['color:',''], 49:['background:',''],
};
const FG30=['#2e2e2e','#c73535','#3f9b45','#b5a326','#3b6ec4','#a347b8','#2e8f8f','#c8c8c8'];
const FG90=['#707070','#f06868','#5fd070','#e8d250','#6c9cf0','#cd78e0','#4ec9c9','#ffffff'];
function cube(n){const v=[0,95,135,175,215,255];
  return 'rgb('+v[Math.floor(n/36)]+','+v[Math.floor((n%36)/6)]+','+v[n%6]+')';}
function color256(n){return n<16?null:n<232?cube(n-16):'rgb('+[8+10*(n-232),8+10*(n-232),8+10*(n-232)].join(',')+')';}
let curStyle='';
function sgrStyle(params){
  let css=curStyle;
  for(let i=0;i<params.length;i++){
    const p=params[i];
    if(p===0) css='';
    else if(SGR[p]) css += ';' + (p===39||p===49 ? SGR[p][0] : SGR[p][0]);
    else if(p>=30&&p<=37) css += ';color:' + FG30[p-30];
    else if(p>=90&&p<=97) css += ';color:' + FG90[p-90];
    else if(p>=40&&p<=47) css += ';background:' + FG30[p-40];
    else if(p>=100&&p<=107) css += ';background:' + FG90[p-100];
    else if((p===38||p===48)&&params[i+1]===5){const c=color256(+params[i+2]);if(c)css+=';'+(p===38?'color:':'background:')+c;i+=2;}
    else if((p===38||p===48)&&params[i+1]===2){const[r,g,b]=params.slice(i+2,i+5);css+=';'+(p===38?'color:':'background:')+`rgb(${r},${g},${b})`;i+=4;}
  }
  return css;
}
const ANSI_RE = /\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])/g;
function appendText(raw){
  if(!raw) return;
  let last=0, css=curStyle;
  raw.replace(ANSI_RE,(m,off)=>{
    if(off>last) pushSpan(raw.slice(last,off),css);
    if(m[1]==='['){
      const code=m.charCodeAt(m.length-1);
      if(code===109){const params=m.slice(2,-1).split(';').map(x=>x===''?0:+x);curStyle=sgrStyle(params);css=curStyle;}
    }
    last=off+m.length; return m;
  });
  if(last<raw.length) pushSpan(raw.slice(last),css);
}
function pushSpan(text,css){
  if(!text) return;
  let span=document.createElement('span');
  if(css){span.setAttribute('style',css.replace(/^;+/,''));}
  span.textContent=text; logEl.appendChild(span);
}
function appendBlock(text){
  if(!text) return;
  const wasFollow=follow;
  const parts=text.split('\n');
  for(let i=0;i<parts.length;i++){
    if(i>0) logEl.appendChild(document.createElement('br'));
    appendText(parts[i]);
  }
  // 帧边界永远落在行末：补一个换行，下一帧第一行不会接到本行尾巴
  logEl.appendChild(document.createElement('br'));
  documentCount += text.length + 1;
  if(documentCount > 4_000_000) {
    logEl.textContent=''; documentCount=0;
    appendText('…前面的日志已被浏览器裁剪…');
    logEl.appendChild(document.createElement('br'));
  }
  if(wasFollow) pinBottom();
}
function atBottom(){ return logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 24; }
function pinBottom(){ logEl.scrollTop = logEl.scrollHeight; }
logEl.addEventListener('scroll',()=>{
  follow=atBottom(); jumpEl.style.display=follow?'none':'block';
},{passive:true});
jumpEl.addEventListener('click',()=>{follow=true;pinBottom();jumpEl.style.display='none';});
document.addEventListener('click',()=>{ if(!follow){follow=true;pinBottom();jumpEl.style.display='none';} },true);

function connect(){
  clearTimeout(reconnectTimer);
  socket=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/wslog');
  socket.onopen=()=>{ document.title='pty local log'; };
  socket.onclose=()=>{ appendBlock('\n[连接断开，1s 后重连]\n'); reconnectTimer=setTimeout(connect,1000); };
  socket.onerror=()=>{};
  socket.onmessage=(ev)=>{
    const msg=JSON.parse(ev.data);
    if(msg.type==='snapshot'){ logEl.textContent=''; curStyle='';
      appendText(msg.text);
      if(msg.text) logEl.appendChild(document.createElement('br'));
      pinBottom(); }
    else if(msg.type==='lines'){ appendBlock(msg.text); if(follow) pinBottom(); }
    else if(msg.type==='pong'){ /* keepalive */ }
  };
}
setInterval(()=>{ if(socket&&socket.readyState===WebSocket.OPEN) socket.send('ping'); }, 10000);
connect();
</script>
</body>
</html>"""


def _install_ring_logging() -> None:
    """会话期所有 logging 只进环形缓冲（幂等）。

    正常路径下 multi_mqtt import 前已置 CMQ_NO_STDERR_LOG，root 上只有
    NullHandler；这里仍防御性移除一切 StreamHandler——paho 或第三方库也可能
    自己往 root 挂控制台 handler——再补 ring handler，级别维持 INFO。
    """
    root = logging.getLogger()
    if not any(isinstance(h, _RingLogHandler) for h in root.handlers):
        handler = _RingLogHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        root.addHandler(handler)
    for h in list(root.handlers):
        # _RingLogHandler 直接继承 logging.Handler，不会被这里误删
        if isinstance(h, logging.StreamHandler):
            root.removeHandler(h)
    if root.level == logging.NOTSET or root.level > logging.INFO:
        root.setLevel(logging.INFO)


def _install_log_capture() -> None:
    """开启"终端只显示远端 shell"模式：logging + stdout + stderr 全部进日志环。

    必须在起好 HTTP 日志口之后调用。先锁定解释器原始控制台的二进制流
    （PTY 渲染/致命错误仍直写真实终端），再接管 logging 与文本级
    stdout/stderr；幂等。
    """
    _bin_stdout()
    _bin_stderr()
    _install_ring_logging()
    if not isinstance(sys.stdout, _RingTextIO):
        sys.stdout = _RingTextIO("<stdout>")
    if not isinstance(sys.stderr, _RingTextIO):
        sys.stderr = _RingTextIO("<stderr>")


def _info(msg):
    _emit_local("[%s] %s\n" % (stime(), msg))


