# PTY 提示符精简

长提示符 `root@r-huggingface1q-...:~#` 来自远端登录 shell 的 PS1，与客户端无关，不支持参数去除，用 SSH 式前置命令（位置参数，连上自动敲一行）或直接改 PS1。

## 外层 shell

当前会话临时生效：

```sh
PS1=#
```

连接时自动生效：

```sh
python client/pty_client_mqtt.py -k <key> -t q PS1=#
python client/pty_client_mqtt.py -k <key> -t q "PS1='# '"     # # 后带空格
python client/pty_client_mqtt.py -k <key> -t q "PS1='\w # '"  # 保留当前目录
```

永久（新开 shell 生效）：

```sh
echo PS1=# >> ~/.bashrc
```

注意：`--no-login` 无效，交互 bash 仍读 .bashrc 设置 PS1。

## tmux

tmux pane 的 shell 由 tmux server 启动，外层前置命令改不到它。

- attach 后手动敲 `PS1=#`（最安全）
- 一键：仅当 pane 停在 shell 提示符时可用，pane 里开着 vim/top 会被注入字符

```sh
python client/pty_client_mqtt.py -k <key> -t q "tmux send-keys PS1=# Enter; tmux at"
```

- 永久：`echo PS1=# >> ~/.bashrc` 后新 window/pane 自动为 `#`；已有 pane 不重读，空闲 pane 批量刷新：

```sh
tmux list-panes -a -F '#S:#I.#P' | xargs -I P tmux send-keys -t P 'PS1=#' Enter
```

## 多层嵌套（tmux 内再 ssh）

PS1 是每台机器各自 shell 的属性，客户端只透传按键，无法穿透。每跳一台机器：

- 临时：逐层敲 `PS1=#`
- 永久：每台机器执行一次 `echo PS1=# >> ~/.bashrc`
