Advanced YAML config
=====================

This example shows how to use [tags](https://chuyuanliu.github.io/heptools/guide/optional/config_parser.html) in YAML configs. Note that some parts of this example are **overcomplicated** in order to incorporate all built-in tags. Use the following code to load the example:

```python
from pathlib import PurePosixPath
from heptools.config import ConfigParser

base = PurePosixPath("examples/advanced-yaml-config/")
parser = ConfigParser()
configs = parser(
    base / "initialize.cfg.yml",
    base / "job1.cfg.yml",
    base / "job2.cfg.yml",
)
```
