import os
import re
import unicodedata
from io import BytesIO
from uuid import uuid4
from datetime import datetime, date

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

from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

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
DATABASE_URL = (os.getenv("DATABASE_URL", "") or "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL (Render Environment Variables).")

DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"sslmode": DB_SSLMODE},
)

db_session = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))
Base = declarative_base()
Base.query = db_session.query_property()


# =========================================================
# MODELOS
# =========================================================
class UploadBatch(Base):
    __tablename__ = "upload_batches"
    id = Column(Integer, primary_key=True)
    token = Column(String(64), unique=True, nullable=False, index=True, default=lambda: uuid4().hex)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    ontracking_rows = relationship("OntrackingRow", back_populates="batch", cascade="all, delete-orphan")
    cardlog_rows = relationship("CardLogRow", back_populates="batch", cascade="all, delete-orphan")
    roommap_rows = relationship("RoomMapRow", back_populates="batch", cascade="all, delete-orphan")


class OntrackingRow(Base):
    __tablename__ = "ontracking_rows"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("upload_batches.id"), nullable=False, index=True)

    modulo = Column(String(50))
    lugar = Column(String(80), index=True)       # CLAVE mostrada (S1805)
    habitacion = Column(String(80))              # se guarda si existe, no se usa para match
    empresa = Column(Text)
    ontracking_id = Column(String(80))
    cama = Column(String(20))
    inicio = Column(String(40))                  # "YYYY-MM-DD"
    termino = Column(String(40))                 # "YYYY-MM-DD"
    dia = Column(String(40), index=True)         # "YYYY-MM-DD"
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
    habitacion = Column(String(80), index=True)  # L1805-2 (archivo log)
    metodo_apertura_puerta = Column(Text)
    tipo_tarjeta = Column(String(80))
    fecha = Column(Text)                         # string original con hora
    dueno_codigo = Column(String(80))
    dueno_nombre = Column(Text)

    raw = Column(JSONB, nullable=False)
    batch = relationship("UploadBatch", back_populates="cardlog_rows")


class RoomMapRow(Base):
    __tablename__ = "roommap_rows"
    id = Column(Integer, primary_key=True)
    batch_id = Column(Integer, ForeignKey("upload_batches.id"), nullable=False, index=True)

    habitacion = Column(String(80), index=True)  # clave ontracking (S1805)
    modulo = Column(String(80), index=True)
    piso = Column(String(20))
    hkeyplus = Column(String(80), index=True)    # clave log (L1805-2)

    raw = Column(JSONB, nullable=False)
    batch = relationship("UploadBatch", back_populates="roommap_rows")


def init_db():
    Base.metadata.create_all(bind=engine)


@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


init_db()


# =========================================================
# HELPERS
# =========================================================
def _normalize_col_name(name: str) -> str:
    name = str(name).strip()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = re.sub(r"[^A-Za-z0-9]+", "_", name)
    return name.strip("_").upper()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    seen = {}
    cols = []
    for c in df.columns:
        base = _normalize_col_name(c)
        if base in seen:
            seen[base] += 1
            base = f"{base}_{seen[base]}"
        else:
            seen[base] = 1
        cols.append(base)
    df.columns = cols
    return df


def allowed_filename(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_EXTS


def read_excel_upload(file_storage) -> pd.DataFrame:
    filename = file_storage.filename or ""
    if not allowed_filename(filename):
        raise ValueError(f"Archivo no permitido: {filename}")

    ext = filename.rsplit(".", 1)[1].lower()
    engine_name = "xlrd" if ext == "xls" else "openpyxl"

    raw = file_storage.read()
    if not raw:
        raise ValueError(f"Archivo vacío: {filename}")

    df = pd.read_excel(BytesIO(raw), dtype=str, engine=engine_name)
    df = normalize_columns(df).fillna("")
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
        return [f"{label}: faltan columnas: {', '.join(missing)}"]
    return []


def parse_log_datetime(value: str):
    """Parsea FECHA (con hora). Acepta 'a.m.'/'p.m.'."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = s.replace("a.m.", "AM").replace("p.m.", "PM").replace("A.M.", "AM").replace("P.M.", "PM")
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def clamp_int(x, default, lo, hi):
    try:
        v = int(x)
    except Exception:
        return default
    return max(lo, min(hi, v))


def get_active_batch():
    """Lote activo: el de sesión o el más reciente."""
    bid = session.get("last_batch_id")
    if bid:
        b = db_session.query(UploadBatch).filter_by(id=bid).first()
        if b:
            return b
    return db_session.query(UploadBatch).order_by(UploadBatch.created_at.desc()).first()


def normalize_on_key(value: str) -> str:
    """
    Normaliza clave Ontracking/Mapa para que se vea como S#### si corresponde.
    - 'S1805' -> 'S1805'
    - '1805'  -> 'S1805'
    - '29'    -> 'S0029'
    - '2F29'  -> '2F29' (no se fuerza)
    """
    s = str(value or "").strip().upper()
    if not s:
        return ""
    if s.startswith("S") and s[1:].isdigit():
        return "S" + s[1:].zfill(4)
    if s.isdigit():
        return "S" + s.zfill(4)
    return s


def normalize_hk(value: str) -> str:
    """Normaliza clave log HKEYPLUS/Lxxxx-2: trim+upper."""
    return str(value or "").strip().upper()


# =========================================================
# CANONICALIZACIÓN
# =========================================================
ONTRACKING_COLMAP = {
    "MODULO": ["MODULO", "MODU", "MOD"],
    "LUGAR": ["LUGAR", "LUGA", "LUG"],
    "HABITACION": ["HABITACION", "HABITACI", "HABITA", "HAB"],
    "EMPRESA": ["EMPRESA", "EMPRES"],
    "ID": ["ID"],
    "CAMA": ["CAMA", "CAM", "CAR"],
    "INICIO": ["INICIO", "INICI"],
    "TERMINO": ["TERMINO", "TERMIN", "TERM"],
    "DIA": ["DIA"],
    "CAMAS_OCUPADAS": ["CAMAS_OCUPADAS", "CAMAS_OCUPD", "CAMAS_OCUPDAS", "CAMAS_OCUP"],
    "RUT": ["RUT"],
    "NOMBRE": ["NOMBRE", "NOMBR"],
}

CARDLOG_COLMAP = {
    "NRO_TARJETA": ["NRO_TARJETA", "NRO_TARJET", "NRO_TARJ"],
    "NRO_HABITACION": ["NRO_HABITACION", "NRO_HABITAC", "NRO_HAB"],
    "HABITACION": ["HABITACION", "HABITACI", "HABITA"],
    "METODO_APERTURA_PUERTA": ["METODO_APERTURA_PUERTA", "METODO_APERTURA", "METODO"],
    "TIPO_DE_TARJETA": ["TIPO_DE_TARJETA", "TIPO_TARJETA", "TIPO"],
    "FECHA": ["FECHA"],
    "DUENO": ["DUENO", "DUEÑO", "OWNER"],
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

    for c in ("RUT", "MODULO"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # LUGAR es la clave "S1805" que se usa en conciliación
    if "LUGAR" in df.columns:
        df["LUGAR"] = df["LUGAR"].astype(str).apply(normalize_on_key)

    if "HABITACION" in df.columns:
        df["HABITACION"] = df["HABITACION"].astype(str).str.strip()

    for c in ("INICIO", "TERMINO", "DIA"):
        if c in df.columns:
            dt = pd.to_datetime(df[c], errors="coerce", dayfirst=True)
            df[c] = dt.dt.date.astype(str).replace("NaT", "")
    return df


def canonicalize_cardlog(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_by_candidates(df, CARDLOG_COLMAP)

    # Si trae Dueño duplicado (código y nombre)
    if "DUENO" in df.columns and "DUENO_2" in df.columns:
        df = df.rename(columns={"DUENO": "DUENO_CODIGO", "DUENO_2": "DUENO_NOMBRE"})
    elif "DUENO" in df.columns and "DUENO_CODIGO" not in df.columns:
        df = df.rename(columns={"DUENO": "DUENO_CODIGO"})

    for c in ("NRO_TARJETA", "NRO_HABITACION"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    if "HABITACION" in df.columns:
        df["HABITACION"] = df["HABITACION"].astype(str).apply(normalize_hk)

    if "FECHA" in df.columns:
        df["FECHA"] = df["FECHA"].astype(str).str.strip()

    if "DUENO_CODIGO" in df.columns:
        df["DUENO_CODIGO"] = df["DUENO_CODIGO"].astype(str).str.strip()
    if "DUENO_NOMBRE" in df.columns:
        df["DUENO_NOMBRE"] = df["DUENO_NOMBRE"].astype(str).str.strip()

    return df


def canonicalize_roommap(df: pd.DataFrame) -> pd.DataFrame:
    df = rename_by_candidates(df, ROOMMAP_COLMAP)

    if "HABITACION" in df.columns:
        df["HABITACION"] = df["HABITACION"].astype(str).apply(normalize_on_key)

    if "HKEYPLUS" in df.columns:
        df["HKEYPLUS"] = df["HKEYPLUS"].astype(str).apply(normalize_hk)

    for c in ("MODULO", "PISO"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    return df


# =========================================================
# PLANTILLAS
# =========================================================
TEMPLATE_COLUMNS = {
    "ontracking": [
        "MODULO", "LUGAR", "HABITACION", "EMPRESA", "ID", "CAMA",
        "INICIO", "TERMINO", "DIA", "CAMAS_OCUPADAS", "RUT", "NOMBRE"
    ],
    "log_tarjetas": [
        "NRO_TARJETA", "NRO_HABITACION", "HABITACION",
        "METODO_APERTURA_PUERTA", "TIPO_DE_TARJETA", "FECHA",
        "DUENO_CODIGO", "DUENO_NOMBRE"
    ],
    "mapa_habitaciones": [
        "HABITACION", "MODULO", "PISO", "HKEYPLUS"
    ],
}

TEMPLATE_SHEETS = {
    "ontracking": "Ontracking",
    "log_tarjetas": "LogTarjetas",
    "mapa_habitaciones": "MapaHabitaciones",
}


def build_template_xlsx(template_key: str) -> BytesIO:
    cols = TEMPLATE_COLUMNS[template_key]
    sheet = TEMPLATE_SHEETS[template_key]
    df = pd.DataFrame(columns=cols)

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)
        ws = writer.book[sheet]
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = ws.dimensions

        header_font = Font(bold=True)
        header_alignment = Alignment(horizontal="center", vertical="center")
        for cell in ws[1]:
            cell.font = header_font
            cell.alignment = header_alignment

        for i, col_name in enumerate(cols, start=1):
            width = max(14, min(42, len(col_name) + 2))
            ws.column_dimensions[get_column_letter(i)].width = width

    bio.seek(0)
    return bio


# =========================================================
# CONTEXT
# =========================================================
@app.context_processor
def inject_now():
    return {"now": datetime.now()}


# =========================================================
# ROUTES
# =========================================================
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/")
def index():
    return redirect(url_for("importar"))


@app.get("/plantilla/<template_key>.xlsx")
def descargar_plantilla(template_key: str):
    if template_key not in TEMPLATE_COLUMNS:
        abort(404)
    bio = build_template_xlsx(template_key)
    return send_file(
        bio,
        as_attachment=True,
        download_name=f"plantilla_{template_key}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ===========================
# IMPORTAR (MISMO BATCH)
# ===========================
@app.route("/importar", methods=["GET", "POST"])
def importar():
    if request.method == "GET":
        batch = get_active_batch()
        last_dt = batch.created_at if batch else None
        return render_template("importar.html", last_dt=last_dt)

    f_ocup = request.files.get("excel_ocupacion")
    f_log = request.files.get("excel_log_tarjetas")
    f_map = request.files.get("excel_mapa_habitaciones")

    has_ocup = bool(f_ocup and (f_ocup.filename or "").strip())
    has_log = bool(f_log and (f_log.filename or "").strip())
    has_map = bool(f_map and (f_map.filename or "").strip())

    if not (has_ocup or has_log or has_map):
        flash("Debes subir al menos 1 archivo.", "danger")
        return redirect(url_for("importar"))

    try:
        # REUTILIZA BATCH ACTIVO. Si no hay, crea.
        batch = get_active_batch()
        if not batch:
            batch = UploadBatch(token=uuid4().hex)
            db_session.add(batch)
            db_session.flush()
            session["last_batch_id"] = batch.id

        saved_sets = []

        # Reemplaza dataset dentro del mismo batch (evita duplicados)
        if has_ocup:
            df = canonicalize_ontracking(read_excel_upload(f_ocup))
            errors = validate_required(df, {"LUGAR", "DIA", "RUT", "NOMBRE"}, "Ontracking")
            if errors:
                for e in errors:
                    flash(e, "danger")
                db_session.rollback()
                return redirect(url_for("importar"))

            db_session.query(OntrackingRow).filter_by(batch_id=batch.id).delete(synchronize_session=False)

            rows = []
            for r in df.to_dict(orient="records"):
                rows.append({
                    "batch_id": batch.id,
                    "modulo": r.get("MODULO", ""),
                    "lugar": r.get("LUGAR", ""),          # S1805
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
            if rows:
                db_session.bulk_insert_mappings(OntrackingRow, rows)
            saved_sets.append("Ontracking (reemplazado)")

        if has_log:
            df = canonicalize_cardlog(read_excel_upload(f_log))
            errors = validate_required(df, {"HABITACION", "FECHA"}, "Log Tarjetas")
            if errors:
                for e in errors:
                    flash(e, "danger")
                db_session.rollback()
                return redirect(url_for("importar"))

            db_session.query(CardLogRow).filter_by(batch_id=batch.id).delete(synchronize_session=False)

            rows = []
            for r in df.to_dict(orient="records"):
                rows.append({
                    "batch_id": batch.id,
                    "nro_tarjeta": r.get("NRO_TARJETA", ""),
                    "nro_habitacion": r.get("NRO_HABITACION", ""),
                    "habitacion": r.get("HABITACION", ""),  # Lxxxx-2
                    "metodo_apertura_puerta": r.get("METODO_APERTURA_PUERTA", ""),
                    "tipo_tarjeta": r.get("TIPO_DE_TARJETA", ""),
                    "fecha": r.get("FECHA", ""),
                    "dueno_codigo": r.get("DUENO_CODIGO", ""),
                    "dueno_nombre": r.get("DUENO_NOMBRE", ""),
                    "raw": r,
                })
            if rows:
                db_session.bulk_insert_mappings(CardLogRow, rows)
            saved_sets.append("Log Tarjetas (reemplazado)")

        if has_map:
            df = canonicalize_roommap(read_excel_upload(f_map))
            errors = validate_required(df, {"HABITACION", "HKEYPLUS"}, "Mapa")
            if errors:
                for e in errors:
                    flash(e, "danger")
                db_session.rollback()
                return redirect(url_for("importar"))

            db_session.query(RoomMapRow).filter_by(batch_id=batch.id).delete(synchronize_session=False)

            rows = []
            for r in df.to_dict(orient="records"):
                rows.append({
                    "batch_id": batch.id,
                    "habitacion": r.get("HABITACION", ""),  # Sxxxx
                    "modulo": r.get("MODULO", ""),
                    "piso": r.get("PISO", ""),
                    "hkeyplus": r.get("HKEYPLUS", ""),      # Lxxxx-2
                    "raw": r,
                })
            if rows:
                db_session.bulk_insert_mappings(RoomMapRow, rows)
            saved_sets.append("Mapa (reemplazado)")

        db_session.commit()
        session["last_batch_id"] = batch.id

        flash("Importación OK: " + ", ".join(saved_sets), "success")
        return redirect(url_for("preview"))

    except Exception as ex:
        db_session.rollback()
        flash(f"Error: {ex}", "danger")
        return redirect(url_for("importar"))


# =========================================================
# PREVIEW (tabs + paginación + borrado)
# =========================================================
@app.get("/preview")
def preview():
    batch = get_active_batch()
    if not batch:
        return render_template("preview.html", has_data=False)

    tab = (request.args.get("tab") or "ontracking").strip().lower()
    page = clamp_int(request.args.get("page"), 1, 1, 100000)
    per_page = clamp_int(request.args.get("per_page"), 25, 10, 200)

    counts = {
        "ontracking": db_session.query(func.count(OntrackingRow.id)).filter_by(batch_id=batch.id).scalar() or 0,
        "log": db_session.query(func.count(CardLogRow.id)).filter_by(batch_id=batch.id).scalar() or 0,
        "mapa": db_session.query(func.count(RoomMapRow.id)).filter_by(batch_id=batch.id).scalar() or 0,
    }

    def paginate(q):
        total = q.count()
        total_pages = max(1, (total + per_page - 1) // per_page)
        p = min(page, total_pages)
        rows = q.offset((p - 1) * per_page).limit(per_page).all()
        return rows, total, total_pages, p

    rows = []
    total = 0
    total_pages = 1

    if tab == "ontracking":
        q = (db_session.query(OntrackingRow)
             .filter_by(batch_id=batch.id)
             .order_by(OntrackingRow.id.desc()))
        rows, total, total_pages, page = paginate(q)

    elif tab == "log":
        q = (db_session.query(CardLogRow)
             .filter_by(batch_id=batch.id)
             .order_by(CardLogRow.id.desc()))
        rows, total, total_pages, page = paginate(q)

    elif tab == "mapa":
        q = (db_session.query(RoomMapRow)
             .filter_by(batch_id=batch.id)
             .order_by(RoomMapRow.id.desc()))
        rows, total, total_pages, page = paginate(q)
    else:
        tab = "ontracking"
        q = (db_session.query(OntrackingRow)
             .filter_by(batch_id=batch.id)
             .order_by(OntrackingRow.id.desc()))
        rows, total, total_pages, page = paginate(q)

    return render_template(
        "preview.html",
        has_data=True,
        created_at=batch.created_at,
        counts=counts,
        tab=tab,
        rows=rows,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
    )


@app.post("/admin/delete_all")
def admin_delete_all():
    batch = get_active_batch()
    if not batch:
        flash("No hay lote activo.", "warning")
        return redirect(url_for("preview"))

    try:
        db_session.delete(batch)
        db_session.commit()
        session.pop("last_batch_id", None)
        flash("Se eliminó el lote completo.", "success")
    except Exception as ex:
        db_session.rollback()
        flash(f"Error eliminando lote: {ex}", "danger")

    return redirect(url_for("preview"))


@app.post("/admin/delete_row/<dataset>/<int:row_id>")
def admin_delete_row(dataset: str, row_id: int):
    batch = get_active_batch()
    if not batch:
        flash("No hay lote activo.", "warning")
        return redirect(url_for("preview"))

    model_map = {"ontracking": OntrackingRow, "log": CardLogRow, "mapa": RoomMapRow}
    if dataset not in model_map:
        abort(404)

    model = model_map[dataset]
    obj = db_session.query(model).filter_by(batch_id=batch.id, id=row_id).first()
    if not obj:
        flash("Registro no encontrado.", "warning")
        return redirect(url_for("preview", tab=dataset))

    try:
        db_session.delete(obj)
        db_session.commit()
        flash("Registro eliminado.", "success")
    except Exception as ex:
        db_session.rollback()
        flash(f"Error eliminando registro: {ex}", "danger")

    tab = request.form.get("tab") or dataset
    page = request.form.get("page") or "1"
    per_page = request.form.get("per_page") or "25"
    return redirect(url_for("preview", tab=tab, page=page, per_page=per_page))


@app.post("/admin/delete_ontracking_date")
def admin_delete_ontracking_date():
    batch = get_active_batch()
    if not batch:
        flash("No hay lote activo.", "warning")
        return redirect(url_for("preview"))

    d = (request.form.get("date") or "").strip()
    if not d:
        flash("Debes indicar una fecha.", "danger")
        return redirect(url_for("preview", tab="ontracking"))

    try:
        target = datetime.strptime(d, "%Y-%m-%d").date().isoformat()
    except Exception:
        flash("Fecha inválida.", "danger")
        return redirect(url_for("preview", tab="ontracking"))

    try:
        n = (db_session.query(OntrackingRow)
             .filter_by(batch_id=batch.id)
             .filter(OntrackingRow.dia == target)
             .delete(synchronize_session=False))
        db_session.commit()
        flash(f"Ontracking eliminado para DIA={target} ({n} filas).", "success")
    except Exception as ex:
        db_session.rollback()
        flash(f"Error eliminando por fecha: {ex}", "danger")

    return redirect(url_for("preview", tab="ontracking"))


@app.post("/admin/delete_log_date")
def admin_delete_log_date():
    batch = get_active_batch()
    if not batch:
        flash("No hay lote activo.", "warning")
        return redirect(url_for("preview", tab="log"))

    d = (request.form.get("date") or "").strip()
    if not d:
        flash("Debes indicar una fecha.", "danger")
        return redirect(url_for("preview", tab="log"))

    try:
        target_date = datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        flash("Fecha inválida.", "danger")
        return redirect(url_for("preview", tab="log"))

    try:
        logs = db_session.query(CardLogRow).filter_by(batch_id=batch.id).all()
        ids = []
        for r in logs:
            dt = parse_log_datetime(r.fecha)
            if dt and dt.date() == target_date:
                ids.append(r.id)

        if not ids:
            flash("No se encontraron logs para esa fecha.", "warning")
            return redirect(url_for("preview", tab="log"))

        n = (db_session.query(CardLogRow)
             .filter_by(batch_id=batch.id)
             .filter(CardLogRow.id.in_(ids))
             .delete(synchronize_session=False))
        db_session.commit()
        flash(f"Logs eliminados para {target_date.isoformat()} ({n} filas).", "success")
    except Exception as ex:
        db_session.rollback()
        flash(f"Error eliminando logs por fecha: {ex}", "danger")

    return redirect(url_for("preview", tab="log"))


# =========================================================
# CONCILIACIÓN
# match: Ontracking.LUGAR (S1805) -> Mapa.HABITACION (S1805) -> Mapa.HKEYPLUS (L1805-2) -> Log.HABITACION (L1805-2)
# =========================================================
def build_map_on_to_hk(batch_id: int):
    rows = db_session.query(RoomMapRow).filter_by(batch_id=batch_id).all()
    on_to_hk = {}
    for r in rows:
        on_key = normalize_on_key(r.habitacion)
        hk = normalize_hk(r.hkeyplus)
        if on_key:
            on_to_hk[on_key] = hk
    return on_to_hk


def logs_grouped_for_date(batch_id: int, target: date):
    """HK -> {count, last:{opener, dt_str}}"""
    all_logs = (db_session.query(CardLogRow)
                .filter_by(batch_id=batch_id)
                .order_by(CardLogRow.id.asc())
                .all())

    by_room = {}
    for r in all_logs:
        dt = parse_log_datetime(r.fecha)
        if not dt or dt.date() != target:
            continue

        hk = normalize_hk(r.habitacion)
        opener = (r.dueno_nombre or r.dueno_codigo or "").strip()

        item = {
            "dt": dt,
            "dt_str": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "opener": opener,
        }

        by_room.setdefault(hk, {"count": 0, "last": None})
        by_room[hk]["count"] += 1

        last = by_room[hk]["last"]
        if last is None or item["dt"] > last["dt"]:
            by_room[hk]["last"] = item

    return by_room


@app.get("/conciliacion")
def conciliacion():
    batch = get_active_batch()
    if not batch:
        return render_template("conciliacion.html", has_data=False)

    date_str = (request.args.get("date") or "").strip()
    page = clamp_int(request.args.get("page"), default=1, lo=1, hi=100000)
    per_page = clamp_int(request.args.get("per_page"), default=25, lo=10, hi=200)

    if not date_str:
        return render_template(
            "conciliacion.html",
            has_data=True,
            created_at=batch.created_at,
            has_run=False,
            date_str="",
        )

    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        flash("Fecha inválida.", "danger")
        return redirect(url_for("conciliacion"))

    dstr = target_date.isoformat()

    q = (db_session.query(OntrackingRow)
         .filter_by(batch_id=batch.id)
         .filter(OntrackingRow.dia == dstr)
         .order_by(OntrackingRow.id.asc()))

    total = q.count()
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    on_rows = q.offset((page - 1) * per_page).limit(per_page).all()

    on_to_hk = build_map_on_to_hk(batch.id)
    logs_by_hk = logs_grouped_for_date(batch.id, target_date)

    rows_out = []
    for r in on_rows:
        on_room = normalize_on_key(r.lugar)  # S1805 (esto es lo que quieres mostrar)
        hk = on_to_hk.get(on_room, "")       # L1805-2 desde mapa

        if not hk:
            status = "Sin mapa"
            apertura_text = ""
            log_count = 0
        else:
            info = logs_by_hk.get(hk)
            if not info:
                status = "Sin log"
                apertura_text = ""
                log_count = 0
            else:
                status = "OK"
                log_count = int(info["count"])
                last = info["last"]
                apertura_text = f"{last['opener']} — {last['dt_str']}" if last else ""

        rows_out.append({
            "on_room": on_room,
            "hk": hk,
            "rut": r.rut or "",
            "nombre": r.nombre or "",
            "empresa": r.empresa or "",
            "inicio": r.inicio or "",
            "termino": r.termino or "",
            "status": status,
            "log_count": log_count,
            "apertura_text": apertura_text,
        })

    return render_template(
        "conciliacion.html",
        has_data=True,
        created_at=batch.created_at,
        has_run=True,
        date_str=date_str,
        rows=rows_out,
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")), debug=True)
