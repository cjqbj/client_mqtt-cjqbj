cd /workspaces/build_xime_home/multi_mqtt 
python3 -m unittest discover -s tests -v

# socks5 套件较重（真实 socket 线程生命周期），放在不带 __init__.py 的
# tests/socks5/ 下，默认 discover 不递归；需要时专门运行：
#   python3 -m unittest discover -s tests/socks5 -v
