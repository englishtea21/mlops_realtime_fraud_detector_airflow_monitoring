import logging
import os
import shutil
import threading
import time
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier


logger = logging.getLogger(__name__)

THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", ""))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))
BUNDLED_MODEL = Path(os.getenv("BUNDLED_MODEL_PATH", "/app/models/my_catboost.cbm"))

_lock = threading.Lock()
_model = None
_version = None


def _model_path(version: str) -> Path:
    return MODELS_DIR / f"my_catboost_{version}.cbm"


def ensure_bootstrap():
    """Гарантирует наличие активной модели в общем каталоге.

    Если есть зашитая (bundled) модель — копирует её как initial.
    Иначе ждёт, пока Airflow обучит и опубликует первую модель,
    вместо того чтобы падать и убивать контейнер.
    """
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    latest = MODELS_DIR / "latest.txt"
    dest = _model_path("initial")
    deadline = time.time() + 600  # ждём до 10 минут появления модели
    while True:
        if latest.exists() and latest.read_text(encoding="utf-8").strip():
            return
        # Если есть зашитая модель — используем её как initial.
        if not dest.exists() and BUNDLED_MODEL.exists():
            shutil.copyfile(BUNDLED_MODEL, dest)
            latest.write_text("initial\n", encoding="utf-8")
            logger.info("Скопирована исходная модель в %s", dest)
            return
        if time.time() >= deadline:
            raise FileNotFoundError(
                f"Модель не появилась в {MODELS_DIR} за 10 минут. "
                f"Проверьте Airflow DAG retrain_catboost."
            )
        logger.info("Активной модели пока нет, ожидаю публикации из Airflow...")
        time.sleep(5)


def _read_latest_version() -> str:
    ensure_bootstrap()
    version = (MODELS_DIR / "latest.txt").read_text(encoding="utf-8").strip()
    if not version:
        raise RuntimeError(f"Пустой указатель модели: {MODELS_DIR / 'lates]t.txt'}")
    return version


def _load_model(version: str) -> CatBoostClassifier:
    path = _model_path(version)
    if not path.exists():
        raise FileNotFoundError(f"Файл модели {version} не найден: {path}")
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
    model = CatBoostClassifier()
    model.load_model(str(path))
    logger.info("Загружена модель версии %s из %s", version, path)
    return model


def _current_model():
    """Возвращает модель, которая соответствует latest.txt. Вызывать под _lock."""
    global _model, _version
    version = _read_latest_version()
    if _model is None or version != _version:
        _model = _load_model(version)
        _version = version
    return _model, _version


def make_pred(dt, source_info="kafka"):
    with _lock:
        model, model_version = _current_model()
        features = list(model.feature_names_)
        missing = [column for column in features if column not in dt.columns]
        if missing:
            raise ValueError(f"Во входных данных нет признаков модели: {missing}")
        frame = dt[features].copy()
        for idx in model.get_cat_feature_indices():
            column = features[idx]
            if not pd.api.types.is_numeric_dtype(frame[column]):
                frame[column] = frame[column].astype(str)
        y_proba = model.predict_proba(frame)[:, 1]

    submission = pd.DataFrame({
        "score": y_proba,
        "fraud_flag": (y_proba > THRESHOLD) * 1,
    })
    logger.info("Prediction complete for data from %s, model_version=%s", source_info, model_version)
    return submission, y_proba, model_version
