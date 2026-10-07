code = '''
import socket, platform, os, subprocess, time, urllib.request
# 公网IP
pub_ip=None
try:
    with urllib.request.urlopen('https://api.ipify.org',timeout=3) as resp:
        pub_ip=resp.read().decode().strip()
except: pass
# 内网IP
lan_ip=None
try:
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    s.connect(("8.8.8.8",80))
    lan_ip=s.getsockname()[0]
    s.close()
except: lan_ip="127.0.0.1"
# 基础信息
hostname=platform.node()
os_name=platform.system()
os_version=platform.version()
kernel_release=platform.release()
machine=platform.machine()
python_version=platform.python_version()
# 虚拟化检测
virt='unknown'
try:
    product=vendor=None
    try:
        with open('/sys/class/dmi/id/product_name') as f: product=f.read().strip()
    except: pass
    try:
        with open('/sys/class/dmi/id/sys_vendor') as f: vendor=f.read().strip()
    except: pass
    if product and vendor:
        combined=(product+' '+vendor).lower()
        for key,val in [('kvm','KVM'),('vmware','VMware'),('virtualbox','VirtualBox'),('xen','Xen'),('microsoft','Hyper-V'),('hyper-v','Hyper-V'),('qemu','QEMU')]:
            if key in combined: virt=val; break
    if virt=='unknown':
        with open('/proc/cpuinfo') as f:
            if 'hypervisor' in f.read(): virt='hypervisor'
except: pass
if virt=='unknown':
    try:
        out=subprocess.check_output(['systemd-detect-virt'],stderr=subprocess.DEVNULL,timeout=2)
        virt=out.decode().strip() or 'none'
    except: pass
# CPU 信息
cpu_model=None; cpu_cores=0
try:
    with open('/proc/cpuinfo') as f:
        for line in f:
            if line.startswith('model name'):
                cpu_model=line.split(':',1)[1].strip(); break
    with open('/proc/cpuinfo') as f:
        cpu_cores=f.read().count('processor')
except: pass
# 内存信息
mem_total=None; mem_avail=None; swap_total=None
try:
    with open('/proc/meminfo') as f:
        for line in f:
            if line.startswith('MemTotal'): mem_total=line.split(':',1)[1].strip()
            elif line.startswith('MemAvailable'): mem_avail=line.split(':',1)[1].strip()
            elif line.startswith('SwapTotal'): swap_total=line.split(':',1)[1].strip()
            if mem_total and mem_avail and swap_total: break
except: pass
# 根分区磁盘统计
disk_total=None; disk_used=None; disk_avail=None; disk_use_percent=None
try:
    st=os.statvfs('/')
    disk_total=st.f_frsize*st.f_blocks
    disk_avail=st.f_frsize*st.f_bavail
    disk_used=disk_total-disk_avail
    disk_use_percent=f"{(1-st.f_bavail/st.f_blocks)*100:.1f}%"
except: pass
# 磁盘挂载列表（易读字符串）
disk_mounts=[]
try:
    df_out=subprocess.check_output(['df','-P'],stderr=subprocess.DEVNULL,timeout=5).decode()
    lines=df_out.strip().splitlines()
    if len(lines)>1:
        for line in lines[1:]:
            p=line.split()
            if len(p)>=6:
                disk_mounts.append(f"{p[0]} {p[5]} {p[4]} {p[3]}/{p[1]}")
except: pass
# 负载
load1=load5=load15=None
try:
    with open('/proc/loadavg') as f:
        p=f.read().split()
        load1,load5,load15=float(p[0]),float(p[1]),float(p[2])
except: pass
# 运行时间
uptime_sec=None; boot_time=None
try:
    with open('/proc/uptime') as f: uptime_sec=float(f.read().split()[0])
    with open('/proc/stat') as f:
        for line in f:
            if line.startswith('btime'):
                boot_time=int(line.split()[1]); break
except: pass
# 时区
timezone=None
try:
    if os.path.exists('/etc/timezone'):
        with open('/etc/timezone') as f: timezone=f.read().strip()
    elif os.path.islink('/etc/localtime'):
        timezone=os.readlink('/etc/localtime').split('zoneinfo/')[-1]
except: pass
# Docker检测
is_docker=os.path.exists('/.dockerenv')
try:
    with open('/proc/1/cgroup') as f:
        if 'docker' in f.read(): is_docker=True
except: pass
# 当前用户
current_user=None
try: current_user=pwd.getpwuid(os.getuid()).pw_name
except: pass
# 关键环境变量
env={}
for k in ['CI','GITHUB_ACTIONS','RUNNER_NAME']:
    if k in os.environ: env[k]=os.environ[k]
# 组装结果
r={
    'hostname':hostname,
    'os':os_name,
    'os_version':os_version,
    'kernel':kernel_release,
    'machine':machine,
    'python':python_version,
    'virt':virt,
    'cpu_model':cpu_model,
    'cpu_cores':cpu_cores,
    'mem_total':mem_total,
    'mem_avail':mem_avail,
    'swap_total':swap_total,
    'disk_total':disk_total,
    'disk_used':disk_used,
    'disk_avail':disk_avail,
    'disk_use%':disk_use_percent,
    'disk_mounts':disk_mounts,
    'load_1m':load1,
    'load_5m':load5,
    'load_15m':load15,
    'uptime_sec':uptime_sec,
    'boot_time':boot_time,
    'timezone':timezone,
    'is_docker':is_docker,
    'user':current_user,
    'public_ip':pub_ip,
    'local_ip':lan_ip,
    'env':env
}
'''
from client import client_mqtt
res = client_mqtt.rpc(code)
print(res['r'])
res
#mqtt


'''

{'boot_time': 1788956653,
 'cpu_cores': 4,
 'cpu_model': 'AMD EPYC 7763 64-Core Processor',
 'disk_avail': 92455452672,
 'disk_mounts': ['/dev/root / 41% 90288528/151263856',
                 'tmpfs /dev/shm 1% 8186640/8186724',
                 'tmpfs /run 1% 3273680/3274692',
                 'tmpfs /run/lock 0% 5120/5120',
                 'efivarfs /sys/firmware/efi/efivars 1% 131036/131072',
                 '/dev/sda16 /boot 8% 773388/901520',
                 '/dev/sda15 /boot/efi 6% 100582/106832',
                 'tmpfs /run/user/1001 1% 1637332/1637344'],
 'disk_total': 154894188544,
 'disk_use%': '40.3%',
 'disk_used': 62438735872,
 'env': {'CI': 'true', 'GITHUB_ACTIONS': 'true', 'RUNNER_NAME': 'GitHub Actions 1000000004'},
 'hostname': 'runnervmejwal',
 'is_docker': False,
 'kernel': '6.17.0-1022-azure',
 'load_15m': 0.06,
 'load_1m': 0.0,
 'load_5m': 0.02,
 'local_ip': '10.1.0.23',
 'machine': 'x86_64',
 'mem_avail': '15386164 kB',
 'mem_total': '16373452 kB',
 'os': 'Linux',
 'os_version': '#22-Ubuntu SMP Mon Jul 27 17:24:03 UTC 2026',
 'public_ip': '52.157.33.164',
 'python': '3.12.14',
 'swap_total': '3145724 kB',
 'timezone': 'Etc/UTC',
 'uptime_sec': 1208.33,
 'user': 'runner',
 'virt': 'Hyper-V'}
Out[208]:
{'req_id': '20260909_204419.678 682ab6',
 'r': "{'boot_time': 1788956653,\n 'cpu_cores': 4,\n 'cpu_model': 'AMD EPYC 7763 64-Core Processor',\n 'disk_avail': 92455452672,\n 'disk_mounts': ['/dev/root / 41% 90288528/151263856',\n                 'tmpfs /dev/shm 1% 8186640/8186724',\n                 'tmpfs /run 1% 3273680/3274692',\n                 'tmpfs /run/lock 0% 5120/5120',\n                 'efivarfs /sys/firmware/efi/efivars 1% 131036/131072',\n                 '/dev/sda16 /boot 8% 773388/901520',\n                 '/dev/sda15 /boot/efi 6% 100582/106832',\n                 'tmpfs /run/user/1001 1% 1637332/1637344'],\n 'disk_total': 154894188544,\n 'disk_use%': '40.3%',\n 'disk_used': 62438735872,\n 'env': {'CI': 'true', 'GITHUB_ACTIONS': 'true', 'RUNNER_NAME': 'GitHub Actions 1000000004'},\n 'hostname': 'runnervmejwal',\n 'is_docker': False,\n 'kernel': '6.17.0-1022-azure',\n 'load_15m': 0.06,\n 'load_1m': 0.0,\n 'load_5m': 0.02,\n 'local_ip': '10.1.0.23',\n 'machine': 'x86_64',\n 'mem_avail': '15386164 kB',\n 'mem_total': '16373452 kB',\n 'os': 'Linux',\n 'os_version': '#22-Ubuntu SMP Mon Jul 27 17:24:03 UTC 2026',\n 'public_ip': '52.157.33.164',\n 'python': '3.12.14',\n 'swap_total': '3145724 kB',\n 'timezone': 'Etc/UTC',\n 'uptime_sec': 1208.33,\n 'user': 'runner',\n 'virt': 'Hyper-V'}",
 'stdout': '',
 'ok': True,
 'server_time': 1788957862.1995106,
 'server_from': 'public-mqtt-broker.bevywise.com',
 'latency_ms': 359.89,
 'client_from': 'mqtt.tyckr.io'}

'''