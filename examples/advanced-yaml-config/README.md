Advanced YAML config
=====================

This example shows how to use [tags](https://chuyuanliu.github.io/heptools/guide/optional/config_parser.html) in YAML configs. The files need to be loaded in the following order:

```python
from pathlib import Path
from heptools.config import ConfigParser

base = Path("examples/advanced-yaml-config/")
parser = ConfigParser()
configs = parser(
    base / "initialize.cfg.yml",
    base / "variables.cfg.yml",
    base / "job1.cfg.yml",
    base / "job2.cfg.yml",
)
```
