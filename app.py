import os
import re
import unicodedata
from io import BytesIO
from uuid import uuid4
from datetime import datetime

import pandas as pd
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, abort, send_file
)

# ----------------------------
# Config
# ----------------------------
app = Flask(__name__)

# SECRET_KEY: en Render configúralo como env var (recomendado).
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")

# Límite de subida (ajústalo según tus Excel)
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB

ALLOWED_EXTS = {"xlsx", "xlsm", "xls"}

# Cache en memoria (Render: se pierde si reinicia el servicio)
DATASETS = {}  # token -> dict con dataframes ya normalizados


# ----------------------------
# Helpers (normalización / lectura)
# ----------------------------
def _normalize_col_name(name: str) -> str:
    name = str(name).strip()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))  # sin tildes
    name = re.sub(r"[^A-Za-z0-9]+", "_", name)
    return name.strip("_").upper()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    seen = {}
    new_cols = []
    for c in df.columns:
        base = _normalize_col_name(c)
        if base in seen:
            seen[base] += 1
            base = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
        new_cols.append(base)
    df.columns = new_cols
    return df


def allowed_filename(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_EXTS


def read_excel_upload(file_storage) -> pd.DataFrame:
    filename = file_storage.filename or ""
    if not allowed_filename(filename):
        raise ValueError(f"Archivo no permitido: {filename}. Usa xlsx/xlsm/xls.")

    ext = filename.rsplit(".", 1)[1].lower()
    engine = "xlrd" if ext == "xls" else "openpyxl"

    raw = file_storage.read()
    if not raw:
        raise ValueError(f"Archivo vacío: {filename}")

    df = pd.read_excel(BytesIO(raw), dtype=str, engine=engine)
    df = normalize_columns(df)
    df = df.fillna("")
    return df


def parse_datetime_series(s: pd.Series) -> pd.Series:
    # Normaliza “a.m.”/“p.m.” típicos en exportes
    s2 = s.astype(str).str.replace("a.m.", "AM", regex=False).str.replace("p.m.", "PM", regex=False)
    return pd.to_datetime(s2, errors="coerce", dayfirst=True)


def validate_required(df: pd.DataFrame, required: set, label: str) -> list[str]:
    missing = [c for c in sorted(required) if c not in df.columns]
    if missing:
        return [f"{label}: faltan columnas requeridas: {', '.join(missing)}"]
    return []


# ----------------------------
# Context processor (footer year)
# ----------------------------
@app.context_processor
def inject_now():
    return {"now": datetime.now()}


# ----------------------------
# Routes
# ----------------------------
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/")
def index():
    return redirect(url_for("importar"))


@app.route("/importar", methods=["GET", "POST"])
def importar():
    if request.method == "GET":
        return render_template("importar.html")

    # POST
    f_ocup = request.files.get("excel_ocupacion")
    f_log = request.files.get("excel_log_tarjetas")
    f_map = request.files.get("excel_mapa_habitaciones")

    if not f_ocup or not f_log or not f_map:
        flash("Debes subir los 3 archivos Excel (Ocupación, Log Tarjetas, Mapa Habitaciones).", "danger")
        return redirect(url_for("importar"))

    try:
        df_ocup = read_excel_upload(f_ocup)
        df_log = read_excel_upload(f_log)
        df_map = read_excel_upload(f_map)

        # Renombres útiles (caso típico: DUEÑO duplicado en el log)
        # Ejemplo normalizado: DUENO y DUENO_2
        if "DUENO_2" in df_log.columns and "DUENO" in df_log.columns:
            df_log = df_log.rename(columns={"DUENO": "DUENO_CODIGO", "DUENO_2": "DUENO_NOMBRE"})

        # Parseos sugeridos (no obligatorios, pero útiles)
        for c in ("INICIO", "TERMINO"):
            if c in df_ocup.columns:
                df_ocup[c] = pd.to_datetime(df_ocup[c], errors="coerce", dayfirst=True).dt.date.astype(str)

        if "FECHA" in df_log.columns:
            df_log["FECHA_PARSED"] = parse_datetime_series(df_log["FECHA"]).astype(str)

        # Validación mínima (ajustable)
        errors = []
        errors += validate_required(
            df_ocup,
            required={"MODULO", "LUGAR", "HABITACION", "RUT", "NOMBRE"},
            label="Ocupación"
        )
        errors += validate_required(
            df_log,
            required={"NRO_TARJETA", "NRO_HABITACION", "HABITACION", "FECHA"},
            label="Log de Tarjetas"
        )
        errors += validate_required(
            df_map,
            required={"HABITACION", "MODULO", "PISO", "HKEYPLUS"},
            label="Mapa Habitaciones"
        )

        if errors:
            for e in errors:
                flash(e, "danger")
            return redirect(url_for("importar"))

        token = uuid4().hex
        DATASETS[token] = {
            "ocupacion": df_ocup,
            "log_tarjetas": df_log,
            "map_habitaciones": df_map,
            "created_at": datetime.now().isoformat(timespec="seconds"),
        }
        session["last_token"] = token

        flash("Archivos importados correctamente. Revisa la previsualización.", "success")
        return redirect(url_for("preview", token=token))

    except Exception as ex:
        flash(f"Error al importar: {ex}", "danger")
        return redirect(url_for("importar"))


@app.get("/preview/<token>")
def preview(token: str):
    payload = DATASETS.get(token)
    if not payload:
        flash("No se encontraron datos para ese token (puede haberse reiniciado el servicio).", "warning")
        return redirect(url_for("importar"))

    def pack(df: pd.DataFrame, n=15):
        cols = list(df.columns)
        rows = df.head(n).to_dict(orient="records")
        return cols, rows, len(df)

    ocup_cols, ocup_rows, ocup_n = pack(payload["ocupacion"])
    log_cols, log_rows, log_n = pack(payload["log_tarjetas"])
    map_cols, map_rows, map_n = pack(payload["map_habitaciones"])

    return render_template(
        "preview.html",
        token=token,
        created_at=payload.get("created_at"),
        ocup_cols=ocup_cols, ocup_rows=ocup_rows, ocup_n=ocup_n,
        log_cols=log_cols, log_rows=log_rows, log_n=log_n,
        map_cols=map_cols, map_rows=map_rows, map_n=map_n,
    )


@app.get("/descargar/<token>/<dataset>.csv")
def descargar_csv(token: str, dataset: str):
    payload = DATASETS.get(token)
    if not payload or dataset not in payload:
        abort(404)

    df = payload[dataset]
    bio = BytesIO()
    df.to_csv(bio, index=False, encoding="utf-8-sig")
    bio.seek(0)

    filename = f"{dataset}_{token[:8]}.csv"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="text/csv; charset=utf-8"
    )


@app.get("/conciliacion")
def conciliacion_placeholder():
    # Aquí conectaremos los “siguientes pasos” que me indiques:
    # - match de habitaciones vía mapa
    # - cruce ocupación vs logs
    # - reglas de negocio
    token = session.get("last_token")
    return render_template("placeholder.html", token=token)


if __name__ == "__main__":
    # En producción (Render) se usa gunicorn.
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=True)
