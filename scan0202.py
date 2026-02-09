from flask import Flask, request,Response, jsonify, send_file, abort
import subprocess
import re
import threading
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

app = Flask(__name__)

# 配置
SCAN_RESULTS_DIR = Path("./scan_results")
SCAN_RESULTS_DIR.mkdir(exist_ok=True)


def generate_timestamp():
    """生成时间戳标识符: 20260129_143052"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_nmap_scan(task_id,ip_list, ports, options):
    """执行单个 IP 的 nmap 扫描，输出 XML 格式"""
    xml_file = SCAN_RESULTS_DIR / f"{task_id}.xml"
    done_file = SCAN_RESULTS_DIR / f"{task_id}.txt"

    cmd = ['nmap', '-sT', '-p', ports, '-oX', str(xml_file), ip_list[0]]

    if options:
        allowed_options = ['-A', '-O', '--script', '-Pn', '-sU']
        for opt in options.split():
            if opt in allowed_options or opt.startswith('--script='):
                cmd.insert(-2, opt)

    try:
        process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,universal_newlines=True, timeout=300)
        # 写入完成标记文件（内容为 done）
        with open(done_file, 'w') as f:
            f.write("done")
        return process.returncode == 0
    except Exception as e:
        print(f"扫描 {ip_list[0]} 失败: {e}")
        return False


def batch_scan_task(task_id, ip_list, ports, options):
    """后台批量扫描任务"""
    xml_file = SCAN_RESULTS_DIR / f"{task_id}.xml"
    done_file = SCAN_RESULTS_DIR / f"{task_id}.txt"

    # 创建合并的 XML 结构
    root = ET.Element("nmaprun")
    root.set("scanner", "nmap")
    root.set("args", f"nmap -p {ports} {' '.join(ip_list)}")
    root.set("start", datetime.now().isoformat())
    root.set("task_id", task_id)

    for ip in ip_list:
        ip_xml_file = SCAN_RESULTS_DIR / f"{task_id}_{ip.replace('.', '_')}.xml"
        success = run_nmap_scan(ip, ports, options, ip_xml_file)

        if success and ip_xml_file.exists():
            try:
                tree = ET.parse(ip_xml_file)
                host_elem = tree.find("host")
                if host_elem is not None:
                    root.append(host_elem)
                ip_xml_file.unlink()  # 删除临时文件
            except Exception as e:
                print(f"解析 {ip} 结果失败: {e}")

    # 保存合并后的 XML
    tree = ET.ElementTree(root)
    tree.write(xml_file, encoding='utf-8', xml_declaration=True)

    # 写入完成标记文件（内容为 done）
    with open(done_file, 'w') as f:
        f.write("done")

    print(f"[任务完成] {task_id}")


@app.route('/scan', methods=['POST'])
def start_scan():
    """
    启动批量扫描任务（异步）

    请求体 JSON:
    {
        "ips": ["192.168.1.1", "10.0.0.1"],
        "ports": "1-1000",      // 可选，默认 1-1000
        "options": "-Pn"        // 可选
    }
    """
    data = request.get_json(silent=True) or {}

    ip_list = data.get('ips')
    if not ip_list or not isinstance(ip_list, list):
        return jsonify({
            'error': 'Missing or invalid parameter: ips (must be a list)',
            'example': {'ips': ['192.168.1.1', '10.0.0.1'], 'ports': '1-1000'}
        }), 400

    # 验证 IP 格式
    for ip in ip_list:
        if not re.match(r'^[a-zA-Z0-9\.\-:]+$', str(ip)):
            return jsonify({'error': f'Invalid IP format: {ip}'}), 400

    ports = data.get('ports', '1-1000')
    if not re.match(r'^[0-9,\-]+$', ports):
        return jsonify({'error': 'Invalid ports format'}), 400

    options = data.get('options', '')

    # 生成任务 ID
    task_id = generate_timestamp()

    # 启动后台线程（daemon=True 表示主进程结束时线程也结束）
    thread = threading.Thread(
        target=run_nmap_scan,
        args=(task_id, ip_list, ports, options),
        daemon=True
    )
    thread.start()

    # 立即返回，不等待扫描完成
    return jsonify({
        'success': True,
        'task_id': task_id,
        'message': 'Scan task started asynchronously',
        'ips_count': len(ip_list),
        'check_status_file': f'{task_id}.txt',
        'result_file': f'{task_id}.xml'
    })


@app.route('/scan/status/<task_id>', methods=['GET'])
def check_status(task_id):
    """
    检查扫描任务状态
    外部循环请求此接口，当返回 completed 时表示完成
    """
    # 安全校验 task_id 格式 (YYYYMMDD_HHMMSS)
    if not re.match(r'^\d{8}_\d{6}$', task_id):
        return jsonify({'error': 'Invalid task_id format'}), 400

    xml_file = SCAN_RESULTS_DIR / f"{task_id}.xml"
    done_file = SCAN_RESULTS_DIR / f"{task_id}.txt"

    # 检查完成标记文件
    if done_file.exists():
        with open(done_file, 'r') as f:
            content = f.read().strip()
        if content == "done":
            return jsonify({
                'task_id': task_id,
                'status': 'completed',
                'message': 'Scan completed',
                'result_file': f"{task_id}.xml",
                'result_url': f'/scan/result/{task_id}'
            })

    # 检查 XML 文件是否存在（正在运行）
    if xml_file.exists():
        return jsonify({
            'task_id': task_id,
            'status': 'running',
            'message': 'Scan in progress'
        })

    # 任务不存在
    return jsonify({
        'task_id': task_id,
        'status': 'not_found',
        'message': 'Task not found'
    }), 404


@app.route('/scan/result/<task_id>', methods=['GET'])
def get_result(task_id):
    """
    获取扫描结果文件 (XML 格式)
    直接返回文件内容
    """
    # 安全校验
    if not re.match(r'^\d{8}_\d{6}$', task_id):
        return jsonify({'error': 'Invalid task_id format'}), 400

    xml_file = SCAN_RESULTS_DIR / f"{task_id}.xml"

    if not xml_file.exists():
        abort(404, description=f"Result file for task {task_id} not found")

    try:
        with open(xml_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # 使用 Response 直接返回内容，不触发下载
        return Response(
            content,
            mimetype='application/xml',
            headers={
                'Content-Type': 'application/xml; charset=utf-8'
            }
        )
    except Exception as e:
        return jsonify({
            'error': f'Failed to read result file: {str(e)}',
            'task_id': task_id
        }), 500


@app.route('/scan/result/<task_id>/download', methods=['GET'])
def download_result(task_id):
    """下载扫描结果文件"""
    if not re.match(r'^\d{8}_\d{6}$', task_id):
        return jsonify({'error': 'Invalid task_id format'}), 400

    xml_file = SCAN_RESULTS_DIR / f"{task_id}.xml"

    if not xml_file.exists():
        abort(404, description=f"Result file for task {task_id} not found")
    
    try:
        with open(xml_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 使用 Response 直接返回内容，不触发下载
        return Response(
            content,
            mimetype='application/xml',
            headers={
                'Content-Type': 'application/xml; charset=utf-8'
            }
        )
    except Exception as e:
        return jsonify({
            'error': f'Failed to read result file: {str(e)}',
            'task_id': task_id
        }), 500


@app.route('/scan/list', methods=['GET'])
def list_results():
    """列出所有已完成的扫描任务"""
    results = []
    for done_file in sorted(SCAN_RESULTS_DIR.glob('*.txt'), reverse=True):
        task_id = done_file.stem
        xml_file = SCAN_RESULTS_DIR / f"{task_id}.xml"
        if xml_file.exists():
            with open(done_file, 'r') as f:
                status = f.read().strip()
            stat = xml_file.stat()
            results.append({
                'task_id': task_id,
                'status': status,
                'completed_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'file_size_bytes': stat.st_size,
                'result_url': f'/scan/result/{task_id}'
            })

    return jsonify({
        'count': len(results),
        'results': results[:50]  # 最多返回 50 个
    })


@app.route('/')
def index():
    """API 文档"""
    return jsonify({
        'service': 'Async Nmap Scanner API',
        'version': '2.0',
        'description': '异步批量 IP 扫描，通过文件标记追踪状态',
        'endpoints': {
            'POST /scan': '启动批量扫描任务（异步）',
            'GET /scan/status/<task_id>': '检查任务状态（循环调用直到 completed）',
            'GET /scan/result/<task_id>': '查看扫描结果 XML 文件',
            'GET /scan/result/<task_id>/download': '下载扫描结果',
            'GET /scan/list': '列出所有已完成的任务'
        }
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
