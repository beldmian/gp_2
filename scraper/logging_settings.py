import logging
from pathlib import Path


log_file_path = Path(__file__).resolve().parents[1] / "logs" / "hh.log"
log_level = logging.INFO
log_format = "%(asctime)s %(process)d %(levelname)s %(name)s %(message)s"


def setup_logging() -> None:
    if getattr(setup_logging, "_ready", False):
        return
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=log_level,
        format=log_format,
        filename=log_file_path,
        encoding="utf-8",
    )
    setup_logging._ready = True
