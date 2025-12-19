import os
import re
import unicodedata
from io import BytesIO
from uuid import uuid4
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, abort, send_file
)

from sqlalchemy import (
    create_engine, Column, Integer, String, Text,
    DateTime, ForeignKey, func
)
from sqlalchemy.orm import (
    scoped_session, sessionmaker, declarative_base, relationship
)
from sqlalchemy.dialects.postgresql import JSONB

# ----------------------------
# Env
# ----------------------------
load_dotenv()

# ----------------------------
# Flask
# ----------------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-me")
app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB

ALLOWED_EXTS = {"xlsx", "xlsm", "xls"}

# ----------------------------
# DB (PostgreSQL en Render)
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise RuntimeError(
        "Falta DATABASE_URL. Configúrala en Render (Environment Variables) con el Internal Database URL."
    )

# En Render normalmente funciona con SSL. Para desarrollo local, puedes setear:
# DB_SSLMODE=prefer (o disable si tu Postgres local no usa SSL)
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"sslmode": DB_SSLMODE},
)

db_session = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()
Base.query = db_session.query_property()


class UploadBatch(Base):
    __tablename__ = "upload_batches"
    id = Column(Integer, primary_key=True)
    token = Column(String(64), unique=True, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    ontracking_rows = relationship("OntrackingRow", back_populates="batch", cascade="all, delete-orphan")
    cardlog_rows = relationship("CardLogRow", back_populates="batch", cascade="all, delete-orphan")
    roommap_rows = relationship("RoomMapRow", back_populates="batch", cascade="all, delete-orphan")


class OntrackingRow(Base):
    __tablename__ = "ontracking_rows"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("upload_batches.id"), nullable=False, index=True)

    modulo = Column(String(50))
    lugar = Column(String(80))
    habitacion = Column(String(80), index=True)
    empresa = Column(Text)
    ontracking_id = Column(String(80))
    cama = Column(String(20))
    inicio = Column(String(40))
    termino = Column(String(40))
    dia = Column(String(40))
    camas_ocupadas = Column(String(40))
    rut = Column(String(40), index=True)
    nombre = Column(Text)

    raw = Column(JSONB, nullable=False)
    batch = relationship("UploadBatch", back_populates="ontracking_rows")


class CardLogRow(Base):
    __tablename__ = "cardlog_rows"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("upload_batches.id"), nullable=False, index=True)

    nro_tarjeta = Column(String(50), index=True)
    nro_habitacion = Column(String(80), index=True)
    habitacion = Column(String(80), index=True)
    metodo_apertura_puerta = Column(Text)
    tipo_tarjeta = Column(String(80))
    fecha = Column(Text)
    dueno_codigo = Column(String(80))
    dueno_nombre = Column(Text)

    raw = Column(JSONB, nullable=False)
    batch = relationship("UploadBatch", back_populates="cardlog_rows")


class RoomMapRow(Base):
    __tablename__ = "roommap_rows"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("upload_batches.id"), nullable=False, index=True)

    habitacion = Column(String(80), index=True)
    modulo = Column(String(80), index=True)
    piso = Column(String(20))
    hkeyplus = Column(String(80), index=True)

    raw = Column(JSONB, nullable=False)
    batch = relationship("UploadBatch", back_populates="roommap_rows")


def init_db():
    Base.metadata.create_all(bind=engine)


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


# Inicializa tablas
init_db()

# ----------------------------
# Helpers (Excel)
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
    engine_name = "xlrd" if ext == "xls" else "openpyxl"

    raw = file_storage.read()
    if not raw:
        raise ValueError(f"Archivo vacío: {filename}")

    df = pd.read_excel(BytesIO(raw), dtype=str, engine=engine_name)
    df = normalize_columns(df)
    df = df.fillna("")
    return df


def rename_by_candidates(df: pd.DataFrame, candidates_map: dict[str, list[str]]) -> pd.DataFrame:
    df = df.copy()
    colset = set(df.columns)

    rename = {}
    for canonical, candidates in candidates_map.items():
        for cand in candidates:
            if cand in colset:
                rename[cand] = canonical
                break
    return df.rename(columns=rename)


def validate_required(df: pd.DataFrame, required: set, label: str) -> list[str]:
    missing = [c for c in sorted(required) if c not in df.columns]
    if missing:
        return [f"{label}: faltan columnas requeridas: {', '.join(missing)}"]
    return []


def parse_datetime_series(s: pd.Series) -> pd.Series:
    s2 = s.astype(str).str.replace("a.m.", "AM", regex=False).str.replace("p.m.", "PM", regex=False)
    return pd.to_datetime(s2, errors="coerce", dayfirst=True)


# ----------------------------
# Canonicalización de columnas
# ----------------------------
ONTRACKING_COLMAP = {
    "MODULO": ["MODULO", "MODU", "MOD"],
    "LUGAR": ["LUGAR", "LUGA", "LUG"],
    "HABITACION": ["HABITACION", "HABITACI", "HABITA", "HAB"],
    "EMPRESA": ["EMPRESA", "EMPRES"],
    "ID": ["ID"],
    "CAMA": ["CAMA", "CAM", "CAR"],  # en tu captura aparece como "Car"
    "INICIO": ["INICIO", "INICI"],
    "TERMINO": ["TERMINO", "TERMIN", "TERM"],
    "DIA": ["DIA"],
    "CAMAS_OCUPADAS": ["CAMAS_OCUPADAS", "CAMAS_OCUPD", "CAMAS_OCUPDAS", "CAMAS_OCUP"],
    "RUT": ["RUT"],
    "NOMBRE": ["NOMBRE", "NOMBR"],
}


CARDLOG_COLMAP = {
    "NRO_TARJETA": ["NRO_TARJETA", "NRO_TARJET", "NRO", "NRO_TARJETA_", "NRO__TARJETA"],
    "NRO_HABITACION": ["NRO_HABITACION", "NRO_HABITAC", "NRO_HABITACION_", "NRO__HABITACION", "NRO_HABITACION_"],
    "HABITACION": ["HABITACION", "HABITACI", "HABITA"],
    "METODO_APERTURA_PUERTA": ["METODO_APERTURA_PUERTA", "METODO_APERTURA", "METODO", "METODO_APERTURA_PUERT"],
    "TIPO_DE_TARJETA": ["TIPO_DE_TARJETA", "TIPO_TARJETA", "TIPO", "TIPO_DE_TARJET"],
    "FECHA": ["FECHA"],
    # DUEÑO puede venir duplicado -> DUENO y DUENO_2
    "DUENO": ["DUENO", "DUEÑO", "DUE_O", "DUE", "OWNER"],
    "DUENO_2": ["DUENO_2", "DUEÑO_2"],
}


ROOMMAP_COLMAP = {
    "HABITACION": ["HABITACION", "HABITACI", "HABITA", "HAB"],
    "MODULO": ["MODULO", "MODU", "MOD"],
    "PISO": ["PISO"],
    "HKEYPLUS": ["HKEYPLUS", "HKEY_PLUS", "HKEY", "HKEYPLU"],
}


def canonicalize_ontracking(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_by_candidates(df, ONTRACKING_COLMAP)

    if "RUT" in df.columns:
        df["RUT"] = df["RUT"].astype(str).str.strip()
    if "HABITACION" in df.columns:
        df["HABITACION"] = df["HABITACION"].astype(str).str.strip()

    # Opcional: parse fechas si vienen en formato fecha
    for c in ("INICIO", "TERMINO"):
        if c in df.columns:
            dt = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
            df[c] = dt.dt.date.astype(str).replace("NaT", "")

    return df


def canonicalize_cardlog(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_by_candidates(df, CARDLOG_COLMAP)

    # Si venía DUEÑO duplicado, queda como DUENO y DUENO_2; lo renombramos a campos claros
    if "DUENO" in df.columns and "DUENO_2" in df.columns:
        df = df.rename(columns={"DUENO": "DUENO_CODIGO", "DUENO_2": "DUENO_NOMBRE"})
    elif "DUENO" in df.columns and "DUENO_CODIGO" not in df.columns:
        # si solo hay un DUENO, lo dejamos como DUENO_CODIGO
        df = df.rename(columns={"DUENO": "DUENO_CODIGO"})

    if "FECHA" in df.columns:
        df["FECHA_PARSED"] = parse_datetime_series(df["FECHA"]).astype(str)

    # Limpiezas
    for c in ("NRO_TARJETA", "NRO_HABITACION", "HABITACION"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


def canonicalize_roommap(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_by_candidates(df, ROOMMAP_COLMAP)

    for c in ("HABITACION", "MODULO", "HKEYPLUS"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


# ----------------------------
# Context processor (footer)
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

    f_ocup = request.files.get("excel_ocupacion")
    f_log = request.files.get("excel_log_tarjetas")
    f_map = request.files.get("excel_mapa_habitaciones")

    if not f_ocup or not f_log or not f_map:
        flash("Debes subir los 3 archivos Excel (Ocupación/Ontracking, Log Tarjetas, Mapa Habitaciones).", "danger")
        return redirect(url_for("importar"))

    try:
        # Leer
        df_ocup = read_excel_upload(f_ocup)
        df_log = read_excel_upload(f_log)
        df_map = read_excel_upload(f_map)

        # Canonicalizar
        df_ocup = canonicalize_ontracking(df_ocup)
        df_log = canonicalize_cardlog(df_log)
        df_map = canonicalize_roommap(df_map)

        # Validaciones mínimas
        errors = []
        errors += validate_required(
            df_ocup,
            required={"MODULO", "LUGAR", "HABITACION", "RUT", "NOMBRE"},
            label="Ocupación (Ontracking)"
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

        # Guardar en DB
        token = uuid4().hex
        batch = UploadBatch(token=token)
        db_session.add(batch)
        db_session.flush()  # asigna batch.id

        # Ontracking
        ocup_records = df_ocup.to_dict(orient="records")
        ocup_rows = []
        for r in ocup_records:
            ocup_rows.append({
                "batch_id": batch.id,
                "modulo": r.get("MODULO", ""),
                "lugar": r.get("LUGAR", ""),
                "habitacion": r.get("HABITACION", ""),
                "empresa": r.get("EMPRESA", ""),
                "ontracking_id": r.get("ID", ""),
                "cama": r.get("CAMA", ""),
                "inicio": r.get("INICIO", ""),
                "termino": r.get("TERMINO", ""),
                "dia": r.get("DIA", ""),
                "camas_ocupadas": r.get("CAMAS_OCUPADAS", ""),
                "rut": r.get("RUT", ""),
                "nombre": r.get("NOMBRE", ""),
                "raw": r,
            })
        db_session.bulk_insert_mappings(OntrackingRow, ocup_rows)

        # Log Tarjetas
        log_records = df_log.to_dict(orient="records")
        log_rows = []
        for r in log_records:
            log_rows.append({
                "batch_id": batch.id,
                "nro_tarjeta": r.get("NRO_TARJETA", ""),
                "nro_habitacion": r.get("NRO_HABITACION", ""),
                "habitacion": r.get("HABITACION", ""),
                "metodo_apertura_puerta": r.get("METODO_APERTURA_PUERTA", ""),
                "tipo_tarjeta": r.get("TIPO_DE_TARJETA", ""),
                "fecha": r.get("FECHA", ""),
                "dueno_codigo": r.get("DUENO_CODIGO", ""),
                "dueno_nombre": r.get("DUENO_NOMBRE", ""),
                "raw": r,
            })
        db_session.bulk_insert_mappings(CardLogRow, log_rows)

        # Mapa Habitaciones
        map_records = df_map.to_dict(orient="records")
        map_rows = []
        for r in map_records:
            map_rows.append({
                "batch_id": batch.id,
                "habitacion": r.get("HABITACION", ""),
                "modulo": r.get("MODULO", ""),
                "piso": r.get("PISO", ""),
                "hkeyplus": r.get("HKEYPLUS", ""),
                "raw": r,
            })
        db_session.bulk_insert_mappings(RoomMapRow, map_rows)

        db_session.commit()

        session["last_token"] = token
        flash("Archivos importados y guardados en la base de datos.", "success")
        return redirect(url_for("preview", token=token))

    except Exception as ex:
        db_session.rollback()
        flash(f"Error al importar: {ex}", "danger")
        return redirect(url_for("importar"))


@app.get("/preview/<token>")
def preview(token: str):
    batch = db_session.query(UploadBatch).filter_by(token=token).first()
    if not batch:
        flash("No se encontró ese lote en la base de datos.", "warning")
        return redirect(url_for("importar"))

    def fetch_raw(model, limit=15):
        rows = (
            db_session.query(model)
            .filter_by(batch_id=batch.id)
            .order_by(model.id.asc())
            .limit(limit)
            .all()
        )
        return [r.raw for r in rows]

    ocup_rows = fetch_raw(OntrackingRow)
    log_rows = fetch_raw(CardLogRow)
    map_rows = fetch_raw(RoomMapRow)

    ocup_n = db_session.query(func.count(OntrackingRow.id)).filter_by(batch_id=batch.id).scalar() or 0
    log_n = db_session.query(func.count(CardLogRow.id)).filter_by(batch_id=batch.id).scalar() or 0
    map_n = db_session.query(func.count(RoomMapRow.id)).filter_by(batch_id=batch.id).scalar() or 0

    ocup_cols = ["MODULO", "LUGAR", "HABITACION", "EMPRESA", "ID", "CAMA", "INICIO", "TERMINO", "DIA", "CAMAS_OCUPADAS", "RUT", "NOMBRE"]
    log_cols = ["NRO_TARJETA", "NRO_HABITACION", "HABITACION", "METODO_APERTURA_PUERTA", "TIPO_DE_TARJETA", "FECHA", "DUENO_CODIGO", "DUENO_NOMBRE"]
    map_cols = ["HABITACION", "MODULO", "PISO", "HKEYPLUS"]

    return render_template(
        "preview.html",
        token=token,
        created_at=batch.created_at,
        ocup_cols=ocup_cols, ocup_rows=ocup_rows, ocup_n=ocup_n,
        log_cols=log_cols, log_rows=log_rows, log_n=log_n,
        map_cols=map_cols, map_rows=map_rows, map_n=map_n,
    )


@app.get("/descargar/<token>/<dataset>.csv")
def descargar_csv(token: str, dataset: str):
    batch = db_session.query(UploadBatch).filter_by(token=token).first()
    if not batch:
        abort(404)

    model_map = {
        "ocupacion": OntrackingRow,
        "log_tarjetas": CardLogRow,
        "map_habitaciones": RoomMapRow,
    }
    if dataset not in model_map:
        abort(404)

    model = model_map[dataset]
    rows = (
        db_session.query(model)
        .filter_by(batch_id=batch.id)
        .order_by(model.id.asc())
        .all()
    )

    raw_list = [r.raw for r in rows]
    df = pd.DataFrame(raw_list)

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
    token = session.get("last_token")
    return render_template("placeholder.html", token=token)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=True)
