# 测试环境：模块级 app = create_app() 在导入时即生效，
# 这里把默认数据目录指到可写的临时位置（真实环境变量优先）。
import os
import tempfile

_tmp = tempfile.mkdtemp(prefix="sorter-default-data-")
os.environ.setdefault("LOGS_DIR", os.path.join(_tmp, "logs"))
os.environ.setdefault("TEMP_DIR", os.path.join(_tmp, "tmp"))
os.environ.setdefault("RESULTS_DIR", os.path.join(_tmp, "results"))
