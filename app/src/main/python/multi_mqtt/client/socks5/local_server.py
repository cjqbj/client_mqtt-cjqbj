#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""本地 SOCKS5 监听服务（仅 CONNECT、无认证；域名交远端解析 = socks5h）。"""
from __future__ import annotations

import socket
import struct
import threading
import time

from ..remote_cmd import RemoteError

from .config import _FIN_CLOSE_GRACE
from .session import RemoteSocks5


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 2326-2579 行（冻结基线，校验见 verify_parity.py） ——
def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("eof during socks handshake")
        buf += chunk
    return buf


def _socks_reply(sock: socket.socket, rep: int) -> None:
    # BND.ADDR/BND.PORT 对 CONNECT 成功语义不重要，填 0.0.0.0:0 即可
    sock.sendall(b"\x05" + bytes([rep]) + b"\x00\x01\x00\x00\x00\x00\x00\x00")


class _Socks5Server:
    """本地 SOCKS5 监听（仅 CONNECT、无认证；域名交远端解析 = socks5h）。"""

    def __init__(self, session: RemoteSocks5, host: str, port: int,
                 open_timeout: float = 30.0, on_log=None):
        self.session = session
        self.bind_host = host
        self.bind_port = int(port)
        self.open_timeout = float(open_timeout)
        self.on_log = on_log or (lambda m: None)
        self._lsock = None
        self._closed = threading.Event()

    def start(self) -> None:
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.bind((self.bind_host, self.bind_port))
        ls.listen(128)
        self._lsock = ls
        threading.Thread(target=self._accept_loop, name="s5-accept",
                         daemon=True).start()

    def close(self) -> None:
        self._closed.set()
        if self._lsock is not None:
            try:
                self._lsock.close()
            except Exception:
                pass

    def _accept_loop(self) -> None:
        while not self._closed.is_set() and not self.session.ended:
            try:
                csock, addr = self._lsock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(csock, addr),
                             name="s5-local", daemon=True).start()

    def _handle(self, csock: socket.socket, addr) -> None:
        cid = None
        entered_pump = False
        try:
            csock.settimeout(30)
            ver = _recv_exact(csock, 1)[0]
            nmethods = _recv_exact(csock, 1)[0]
            _recv_exact(csock, nmethods)
            if ver != 5:
                csock.close()
                return
            csock.sendall(b"\x05\x00")  # 无认证
            ver, cmd, _rsv, atyp = _recv_exact(csock, 4)
            if atyp == 1:
                host = socket.inet_ntoa(_recv_exact(csock, 4))
            elif atyp == 3:
                n = _recv_exact(csock, 1)[0]
                host = _recv_exact(csock, n).decode("latin-1")
            elif atyp == 4:
                host = socket.inet_ntop(socket.AF_INET6, _recv_exact(csock, 16))
            else:
                _socks_reply(csock, 8)   # address type not supported
                csock.close()
                return
            port = struct.unpack(">H", _recv_exact(csock, 2))[0]
            if cmd != 1:
                _socks_reply(csock, 7)   # command not supported（不支持 UDP 等）
                csock.close()
                return
            csock.settimeout(None)

            sess = self.session
            cid = sess.open_connection(host, port)
            try:
                res = sess.wait_opened(cid, self.open_timeout)
            except RemoteError as exc:
                self.on_log("[cid=%d] 远端 connect %s:%d 失败: %s"
                            % (cid, host, port, exc))
                _socks_reply(csock, 5)   # connection refused
                return
            if not res.get("ok"):
                self.on_log("[cid=%d] 远端 connect %s:%d 被拒: %s"
                            % (cid, host, port, res.get("error")))
                _socks_reply(csock, 5)
                return
            _socks_reply(csock, 0)
            self.on_log("[cid=%d] %s:%d 已建立（经 %s）"
                        % (cid, host, port, addr[0]))

            with sess._lock:
                conn = sess._conns.get(cid)
            if conn is None:
                return
            conn.local_sock = csock
            threading.Thread(target=self._local_writer, args=(cid, csock),
                             name="s5-lw-%d" % cid, daemon=True).start()
            # 进入泵送阶段后连接收尾移交给 reader/writer：
            # 本地 EOF 只是半关（远端响应还在路上），绝不能在这里关 csock。
            entered_pump = True
            self._local_reader(cid, csock)
        except (ConnectionError, OSError):
            pass
        except Exception as exc:
            self.on_log("[s5] 本地连接处理异常: %s: %s"
                        % (type(exc).__name__, exc))
            # 泵送阶段的非预期异常：reader 自身已对已知错误收尾，这里是最后
            # 一道保险（防止未来新增逃逸路径让 conn/writer/fd 静默泄漏）。
            # fin 已接管 socket（clean EOF）时不抢关：reader_done 已置位，
            # fin 线程会自己立刻 close。
            if entered_pump and cid is not None:
                try:
                    with self.session._lock:
                        cc = self.session._conns.get(cid)
                    if cc is not None and not cc.fin_close_pending:
                        self.session.drop_connection(
                            cid, reason="handle_fatal")
                except Exception:
                    pass
        finally:
            if not entered_pump:
                if cid is not None:
                    self.session.drop_connection(cid, reason="setup_failed")
                try:
                    csock.close()
                except Exception:
                    pass

    def _local_reader(self, cid: int, csock: socket.socket) -> None:
        """本地 socket -> 上行帧。EOF 时发 in-seq close（半关），连接其余
        收尾由 writer 收到远端 closed 后完成；本地读出错则整条连接收尾。"""
        sess = self.session
        # 入口即持有本连接对象：cid 单调不回收，之后即便条目被 drop 弹出
        # _conns，本线程的信号/状态也必须落到自己的 conn 上，不能再查字典。
        with sess._lock:
            own = sess._conns.get(cid)
        try:
            while not sess.ended:
                data = csock.recv(sess.frame_max)
                if not data:
                    sess.eof_connection(cid)
                    return
                sess.send_data(cid, data)
        except (ConnectionError, OSError, RemoteError):
            if own is not None:
                with own.state_lock:
                    remote_closed = own.remote_closed
                if remote_closed:
                    # csock 是 writer/fin 在远端 closed 后主动 shutdown/close
                    # 的，收尾归它们，这里只退出
                    return
            sess.drop_connection(cid, reason="local_error")
        except Exception:
            # 网络三异常以外的非预期错误（库内部 RuntimeError/MemoryError
            # 等）：正常路径不会走到。不兜住的话异常只冒泡到 _handle 记一行
            # 日志，而 entered_pump 后 _handle 不关 csock、writer 永远阻塞
            # 在 wq.get()——fd、conn 条目、writer 线程全部静默泄漏。
            try:
                sess.drop_connection(cid, reason="reader_fatal")
            except Exception:
                pass
        finally:
            # 读线程退出 = 应用端已关闭（FIN/RST 已到），fin 可安全 close
            # fd。必须无条件置位：clean EOF 时 drop_connection 已先于应用
            # 关闭把条目弹出 _conns，靠字典回查会让 fin 白等 120s grace。
            if own is not None:
                own.reader_done.set()

    def _local_writer(self, cid: int, csock: socket.socket) -> None:
        """下行帧 -> 本地 socket。收到 None（远端 closed 或本地收尾）即结束；
        正常路径（远端 closed）由这里做整条连接的最终收尾。"""
        sess = self.session
        with sess._lock:
            conn = sess._conns.get(cid)
        if conn is None:
            return
        clean = False
        already_closed = False
        try:
            while True:
                item = conn.wq.get()
                if item is None:
                    # 区分：远端 EOF（clean）还是 drop_connection 强收尾。
                    # 两个标志必须在同一把锁内成对快照，否则 None 入队与
                    # drop 同瞬发生时，正常远端 EOF 会被误判成 local_error。
                    with conn.state_lock:
                        already_closed = conn.closed
                        clean = conn.remote_closed and not already_closed
                    break
                csock.sendall(item)
        except (ConnectionError, OSError):
            clean = False
            with conn.state_lock:
                already_closed = conn.closed
        if clean:
            # 远端半关：只 shutdown 写方向给应用发 FIN，绝不能直接 close()——
            # Windows 下 socket 接收缓冲里还有应用未读完的数据时，close()
            # 会直接发 RST，把已完整送达的下载/响应在应用侧打成连接重置。
            try:
                csock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            # socket 最终 close 权移交专职 fin 线程：它等本地读线程退出
            # （应用读完）或 grace 上限；会话被 abort 时 0.5s 内兜底关 fd。
            # 会话侧资源（_conns 条目/重组/缓存/统计）则**立即**回收，
            # writer 线程不再为一个只读长连接白挂 120s。
            conn.fin_close_pending = True

            def _fin_close(_c=conn, _s=csock):
                deadline = time.monotonic() + _FIN_CLOSE_GRACE
                while not sess.ended:
                    remain = deadline - time.monotonic()
                    if remain <= 0:
                        break
                    if _c.reader_done.wait(min(0.5, remain)):
                        break
                if not _c.reader_done.is_set():
                    # 走到这里说明 reader 仍阻塞在 recv（应用拖着写端不关，
                    # 或会话 abort）。Windows 上另一线程 close() 叫不醒阻塞
                    # 的 recv（fd 被一直占着），必须先 shutdown 双向：recv
                    # 随即返回 EOF/异常，reader 退出并置 reader_done。
                    # 连接条目此时已被 drop 弹出，reader 的 eof/drop 均 no-op。
                    try:
                        _s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                try:
                    _s.close()
                except Exception:
                    pass

            threading.Thread(target=_fin_close,
                             name="s5-fin-%d" % cid, daemon=True).start()
            sess.drop_connection(cid, reason="remote_closed")
        else:
            try:
                csock.close()
            except Exception:
                pass
            if not already_closed:
                sess.drop_connection(cid, reason="local_error")
