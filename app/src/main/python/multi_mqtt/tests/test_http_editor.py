import io
import json
import os
import sys
import tempfile
import unittest
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server_http
from client import http_editor


class _Handler:
    """_read_body 需要的 handler 形状：只有 environ['wsgi.input']（WSGI 垫片）。"""

    def __init__(self, body, command='POST'):
        self.command = command
        self.headers = {'Content-Length': str(len(body))}
        self.environ = {'wsgi.input': io.BytesIO(body)}


class HttpEditorSaveBodyTests(unittest.TestCase):
    """正文必须走 POST body：网关（nginx ~8KB）对长 URL 回 414 HTML 错误页，
    页面只能显示"保存失败: </html>"。"""

    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.path = os.path.join(self._dir.name, 'note.html')
        self.server = server_http.start_rpc_server(
            port=0, ip='127.0.0.1', key='-',
            globals={'http_editor': http_editor},
        )
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.base = 'http://127.0.0.1:%d/-' % self.server.server_port

    def _call(self, code, body=None):
        url = self.base + urllib.parse.quote(code, safe='')
        request = urllib.request.Request(
            url, data=None if body is None else body.encode('utf-8'))
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, response.read().decode('utf-8')
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode('utf-8', 'replace')

    def _open_page(self):
        status, page = self._call('http_editor.html(p, %r)' % self.path)
        self.assertEqual(status, 200, page[:300])
        self.assertIn('save_body', page, '页面必须走 POST body 保存')
        token = page.split('const token = ')[1].split(';')[0].strip().strip('"')
        return token

    def _read_file(self):
        with open(self.path, encoding='utf-8') as handle:
            return handle.read()

    def test_page_posts_body_instead_of_putting_content_in_url(self):
        page_token = self._open_page()
        self.assertEqual(page_token, http_editor.get_editor(self.path).token)

    def test_save_body_writes_chinese_and_emoji_content(self):
        token = self._open_page()
        content = '<html>\n' + ('/* 中文正文 */ 行\n' * 2000) + '🙂\n</html>'
        status, out = self._call("http_editor.save_body(p,'%s')" % token, content)
        self.assertEqual(status, 200, out)
        self.assertIn('"ok": true', out)
        self.assertEqual(self._read_file(), content)

    def test_empty_post_body_saves_empty_file(self):
        token = self._open_page()
        with open(self.path, 'w', encoding='utf-8') as handle:
            handle.write('old content')
        status, out = self._call("http_editor.save_body(p,'%s')" % token, '')
        self.assertEqual(status, 200, out)
        self.assertEqual(self._read_file(), '')

    def test_get_save_body_is_refused_and_does_not_wipe_file(self):
        token = self._open_page()
        with open(self.path, 'w', encoding='utf-8') as handle:
            handle.write('keep me')
        status, out = self._call("http_editor.save_body(p,'%s')" % token)
        self.assertEqual(status, 500, out)
        self.assertIn('POST', out)
        self.assertEqual(self._read_file(), 'keep me')

    def test_repeated_save_of_unchanged_content_keeps_revision(self):
        # revision 是"内容版本"，不是"保存次数"：Ctrl+S 连按不该把数字越堆越高
        token = self._open_page()
        content = 'same content 同样内容'
        status, out = self._call("http_editor.save_body(p,'%s')" % token, content)
        self.assertEqual(status, 200, out)
        self.assertEqual(json.loads(out)['revision'], 1)
        for _ in range(3):
            status, out = self._call("http_editor.save_body(p,'%s')" % token, content)
            self.assertEqual(status, 200, out)
            self.assertEqual(json.loads(out)['revision'], 1, out)
        status, out = self._call("http_editor.save_body(p,'%s')" % token, content + '!')
        self.assertEqual(json.loads(out)['revision'], 2)

    def test_unchanged_save_does_not_rewrite_file(self):
        token = self._open_page()
        status, out = self._call("http_editor.save_body(p,'%s')" % token, 'abc')
        self.assertEqual(status, 200, out)
        stamp = os.stat(self.path).st_mtime_ns
        status, out = self._call("http_editor.save_body(p,'%s')" % token, 'abc')
        self.assertEqual(status, 200, out)
        self.assertEqual(os.stat(self.path).st_mtime_ns, stamp, '相同内容不应写盘')

    def test_legacy_url_save_still_works(self):
        token = self._open_page()
        status, out = self._call("http_editor.save(p,'%s',%r)" % (token, 'hello 世界'))
        self.assertEqual(status, 200, out)
        self.assertEqual(self._read_file(), 'hello 世界')

    def test_read_body_supports_wsgi_environ_without_rfile(self):
        # WSGI 垫片只提供 environ['wsgi.input']，没有 rfile：
        # pythonanywhere 线上走的就是这条分支
        content = '中文 ^_^ 🙂'
        self.assertEqual(
            http_editor._read_body(_Handler(content.encode('utf-8'))), content)

    def test_read_body_rejects_non_post(self):
        with self.assertRaises(RuntimeError):
            http_editor._read_body(_Handler(b'', command='GET'))


class EditorTitleTests(unittest.TestCase):
    """两个编辑页的标签页标题都要以正在编辑的文件名开头。"""

    def test_http_editor_title_starts_with_file_name(self):
        self.assertIn("document.title = fileName + ' - HTTP Editor'", http_editor.PAGE)

    def test_realtime_editor_title_starts_with_file_name(self):
        import realtime_editor

        self.assertIn("document.title = fileName + ' - Realtime Editor'",
                      realtime_editor.PAGE)


if __name__ == '__main__':
    unittest.main()
