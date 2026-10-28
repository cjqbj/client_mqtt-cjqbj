#!/usr/bin/env python3
import struct
import socket
import threading
import json
import traceback
import logging
from concurrent.futures import ThreadPoolExecutor

# 模块内独立日志配置，不污染全局
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(handler)
    
class TCPFrameProtocol:
    """长度前缀 + JSON 的帧协议"""
    HEADER_FMT = '!II'  # req_id: uint32, body_len: uint32 (网络字节序)
    HEADER_SIZE = struct.calcsize(HEADER_FMT)
    
        # 解决 r=client_sock 卡住 ， 更好的架构是 字符串化由 rpc_executor 解决
    @classmethod
    def encode(cls, req_id: int, body: dict) -> bytes:
        body_bytes = json.dumps(body, ensure_ascii=False, default=repr).encode('utf-8')
        header = struct.pack(cls.HEADER_FMT, req_id, len(body_bytes))
        return header + body_bytes
    
    @classmethod
    def decode_frames(cls, buffer: bytes):
        """从缓冲区解析完整帧，返回 (frames, remaining_buffer)"""
        frames = []
        while len(buffer) >= cls.HEADER_SIZE:
            req_id, body_len = struct.unpack(cls.HEADER_FMT, buffer[:cls.HEADER_SIZE])
            total_len = cls.HEADER_SIZE + body_len
            if len(buffer) < total_len:
                break  # 等待更多数据
            body_bytes = buffer[cls.HEADER_SIZE:total_len]
            try:
                body = json.loads(body_bytes.decode('utf-8'))
                frames.append((req_id, body))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Decode error: {e}, body: {body_bytes[:100]}")
            buffer = buffer[total_len:]
        return frames, buffer


class TCPServer:
    def __init__(self, host='0.0.0.0', port=1166,
                 globals=None,
                 executor_pool_size=10):
        self.host = host
        self.port = port
        # 持久命名空间：与 server_http / server_mqtt 同一模式——默认零拷贝
        # 直接持有调用方 globals（PythonExecutor copy_globals=False），调用
        # 方启动后才赋值的名字对本通道实时可见。
        self.globals_dict = globals

        # 执行器：请求级上下文（client_sock 等）经 execute(globals=...)
        # 注入共享命名空间，执行完自动还原，不泄漏进持久状态
        from rpc_executor import PythonExecutor
        self.executor = PythonExecutor(globals=globals)

        # 线程池处理业务逻辑
        self.worker_pool = ThreadPoolExecutor(max_workers=executor_pool_size)

        self.server_socket = None
        self.running = False

        self.client_locks = {} # 防止 多个 worker 线程同时往同一个 socket sendall 依然可能串帧
        self.client_locks_lock = threading.Lock()

    def start(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)
        self.running = True
        # port=0（让 OS 分配）时回填实际端口，方便测试/多实例
        self.port = self.server_socket.getsockname()[1]

        logger.info(f"TCP RPC server listening on {self.host}:{self.port}")
        
        accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        accept_thread.start()
        return self
    
    def _accept_loop(self):
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                logger.info(f"Client connected: {addr}")
                handler = threading.Thread(
                    target=self._handle_client,
                    args=(client_sock, addr),
                    daemon=True
                )
                handler.start()
            except OSError:
                if self.running:
                    logger.exception("Accept error")
                break
    
    def _handle_client(self, client_sock: socket.socket, addr):
        """处理单个客户端连接（长连接，多请求复用）"""
        # client_sock._write_lock = threading.Lock() # socket 对象不能加属性
        buffer = b''
        client_id = f"{addr[0]}:{addr[1]}"
        with self.client_locks_lock:
            self.client_locks[client_id] = threading.Lock()
        
        try:
            client_sock.settimeout(30.0)  # 空闲超时
            
            while self.running:
                # 接收数据
                chunk = client_sock.recv(65536)
                if not chunk:
                    logger.info(f"Client disconnected: {client_id}")
                    break
                
                buffer += chunk
                
                # 帧解析
                frames, buffer = TCPFrameProtocol.decode_frames(buffer)
                
                for req_id, request_body in frames:
                    # 异步处理，避免阻塞接收
                    self.worker_pool.submit(
                        self._process_request,
                        client_sock, client_id, req_id, request_body
                    )
                    
        except TimeoutError:#socket.timeout:
            logger.info(f"Client idle timeout: {client_id}")
        except ConnectionResetError:
            logger.info(f"Client reset: {client_id}")
        except Exception:
            logger.exception(f"Client handler error: {client_id}")
        finally:
            client_sock.close()
            with self.client_locks_lock:
                self.client_locks.pop(client_id, None)
            # 清理该客户端的pending请求
            
            
    def _process_request(self, client_sock, client_id, req_id, request_body):
        """执行业务逻辑并发送响应"""
        try:
            code = request_body.get('code', '')

            # 请求级上下文注入（server_http 模式）：client_sock 只在本请求
            # 执行期间可见，结束自动还原。刻意暴露原生 socket 是既有设计：
            # 用户代码可直接调用 recv()/accept() 等阻塞方法挂住 worker 线程，
            # 不做更安全的包装（历史注释明示这是设计好的行为）。
            execution = self.executor.execute(
                code,
                globals={'__name__': '__tcp_rpc__',
                         'client_sock': client_sock},
            )

            # 构建响应
            response = {
                'req_id': req_id,
                'ok': execution['ok'],
                'r': execution.get('r'),
                'stdout': execution.get('stdout', ''),
            }
            if not execution['ok']:
                response['error'] = execution.get('error')

        except Exception as e:
            response = {
                'req_id': req_id,
                'ok': False,
                'error': traceback.format_exc()
            }
        
        # 发送响应（需要锁，因为多个worker线程可能同时写同一socket）
        with self.client_locks_lock:
            lock = self.client_locks.get(client_id)
        if lock is None:
            logger.warning(f"Client gone: {client_id}")
            return

        try:
            frame = TCPFrameProtocol.encode(req_id, response)
            with lock:
                client_sock.sendall(frame)
        # except (BrokenPipeError, ConnectionResetError):
        except Exception as e:
            logger.warning(f"Send failed, client gone: {client_id}\n{e}")        
        
    def stop(self):
        self.running = False
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError:
                pass
        self.worker_pool.shutdown(wait=False)


# ============ 客户端实现 ============

class TCPClient:
    def __init__(self, host='127.0.0.1', port=1166):
        self.host = host
        self.port = port
        self.sock = None
        self.lock = threading.Lock()
        self.buffer = b''
        self.pending = {}  # req_id -> (event, result)
        self.reader_thread = None
        self.req_counter = 0
        self.connected = False
        
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.connected = True
        
        # 启动接收线程
        self.reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self.reader_thread.start()
        return self
    
    def _read_loop(self):
        """后台线程：持续接收并分发响应"""
        while self.connected:
            try:
                chunk = self.sock.recv(65536)
                if not chunk:
                    logger.warning("Server disconnected")
                    self.connected = False
                    break
                    
                self.buffer += chunk
                frames, self.buffer = TCPFrameProtocol.decode_frames(self.buffer)
                
                for req_id, response in frames:
                    with self.lock:
                        if req_id in self.pending:
                            event, _ = self.pending[req_id]
                            self.pending[req_id] = (event, response)
                            event.set()
                            
            except ConnectionResetError:
                logger.warning("Connection reset")
                self.connected = False
                break
            except Exception:
                logger.exception("Read error")
                self.connected = False
                break
        
        # 唤醒所有等待的请求
        with self.lock:
            for evt, _ in self.pending.values():
                evt.set()
    
    def call(self, code: str, timeout: float = 30.0) -> dict:
        """同步阻塞调用"""
        with self.lock:
            self.req_counter += 1
            req_id = self.req_counter
            
            event = threading.Event()
            self.pending[req_id] = (event, None)
        
        # 发送请求
        request = {'code': code}
        frame = TCPFrameProtocol.encode(req_id, request)
        
        try:
            self.sock.sendall(frame)
        except (BrokenPipeError, ConnectionResetError):
            raise ConnectionError("Not connected")
        
        # 等待响应
        if not event.wait(timeout=timeout):
            with self.lock:
                self.pending.pop(req_id, None)
            raise TimeoutError(f"Request {req_id} timeout")
        
        with self.lock:
            _, result = self.pending.pop(req_id, (None, None))
        
        if not self.connected:
            raise ConnectionError("Disconnected during request")
        
        return result
    
    def close(self):
        self.connected = False
        if self.sock:
            self.sock.close()


# ============ 使用示例 ============

if __name__ == '__main__':
    port=1166
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'server':
        # 与 server_http.__main__ 同一模式：持久命名空间直接持有本模块
        # globals（零拷贝），TCP/MQTT/HTTP 三通道语义一致
        server = TCPServer(port=port, globals=globals())
        server.start()
        input("Press Enter to stop\n")
        server.stop()
        
    elif len(sys.argv) > 1 and sys.argv[1] == 'client':
        client = TCPClient('127.0.0.1', port)
        client.connect()
        
        # 同步调用
        result = client.call("r = 1 + 1")
        print(f"Result: {result}")
        
        result = client.call("r = [x**2 for x in range(10)]")
        print(f"Result: {result}")
        
        print(client.call("dir()"))
        print(client.call("r=client_sock")) #不知道为什么这一句卡住 因为json序列化，但是最好架构参考上面注释TODO
        print(client.call("r=str(client_sock)"))
        print(client.call("r=repr(client_sock),dir(client_sock)"))
        
        import time
        time.sleep(5)
        client.close()