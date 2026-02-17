import os
import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean, text
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship
from sqlalchemy.exc import IntegrityError

# --- CONFIGURACIÓN BASE DE DATOS ---
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./local_test.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELOS ---

class Cliente(Base):
    __tablename__ = "clientes"
    id         = Column(Integer, primary_key=True, index=True)
    nombre     = Column(String, nullable=False)
    documento  = Column(String, nullable=True)
    telefono   = Column(String, nullable=True)
    email      = Column(String, nullable=True)
    direccion  = Column(String, nullable=True)
    activo     = Column(Boolean, default=True)
    fecha_registro = Column(DateTime, default=datetime.datetime.utcnow)

class Producto(Base):
    __tablename__ = "productos"
    id           = Column(Integer, primary_key=True, index=True)
    codigo       = Column(String, unique=True, nullable=False)
    nombre       = Column(String, nullable=False)
    descripcion  = Column(String, nullable=True)
    precio       = Column(Float, nullable=False, default=0)
    stock        = Column(Integer, nullable=False, default=0)
    stock_minimo = Column(Integer, default=10)
    activo       = Column(Boolean, default=True)
    fecha_creacion = Column(DateTime, default=datetime.datetime.utcnow)

class Recibo(Base):
    __tablename__ = "recibos"
    id          = Column(Integer, primary_key=True, index=True)
    numero      = Column(String, unique=True, nullable=False)
    cliente_id  = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    fecha       = Column(DateTime, default=datetime.datetime.utcnow)
    subtotal    = Column(Float, default=0)
    descuento   = Column(Float, default=0)
    total       = Column(Float, nullable=False, default=0)
    estado      = Column(String, default="PENDIENTE")
    observaciones = Column(String, nullable=True)
    items       = relationship("ReciboDetalle", back_populates="recibo", cascade="all, delete-orphan")

class ReciboDetalle(Base):
    __tablename__ = "recibos_detalle"
    id              = Column(Integer, primary_key=True, index=True)
    recibo_id       = Column(Integer, ForeignKey("recibos.id"))
    producto_id     = Column(Integer, ForeignKey("productos.id"), nullable=True)
    cantidad        = Column(Integer, nullable=False)
    precio_unitario = Column(Float, nullable=False)
    subtotal        = Column(Float, nullable=False)
    recibo          = relationship("Recibo", back_populates="items")

class Reserva(Base):
    __tablename__ = "reservas"
    id             = Column(Integer, primary_key=True, index=True)
    cliente_id     = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    producto_id    = Column(Integer, ForeignKey("productos.id"), nullable=True)
    cantidad       = Column(Integer, nullable=False)
    fecha_reserva  = Column(DateTime, default=datetime.datetime.utcnow)
    fecha_entrega  = Column(String, nullable=True)
    estado         = Column(String, default="PENDIENTE")
    observaciones  = Column(String, nullable=True)

class Venta(Base):
    __tablename__ = "ventas"
    id          = Column(Integer, primary_key=True, index=True)
    recibo_id   = Column(Integer, ForeignKey("recibos.id"), nullable=True)
    cliente_id  = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    fecha       = Column(DateTime, default=datetime.datetime.utcnow)
    total       = Column(Float, nullable=False)
    forma_pago  = Column(String, nullable=True)
    vendedor    = Column(String, nullable=True)

class CuentaCorriente(Base):
    __tablename__ = "cuenta_corriente"
    id          = Column(Integer, primary_key=True, index=True)
    cliente_id  = Column(Integer, ForeignKey("clientes.id"))
    tipo        = Column(String, nullable=False)  # 'DEBE' o 'HABER'
    monto       = Column(Float, nullable=False)
    concepto    = Column(String, nullable=True)
    recibo_id   = Column(Integer, ForeignKey("recibos.id"), nullable=True)
    fecha       = Column(DateTime, default=datetime.datetime.utcnow)
    saldo       = Column(Float, nullable=True)

# --- SCHEMAS PYDANTIC ---

class ClienteCreate(BaseModel):
    nombre:    str
    documento: Optional[str] = ""
    telefono:  Optional[str] = ""
    email:     Optional[str] = ""
    direccion: Optional[str] = ""

class ProductoCreate(BaseModel):
    codigo:       str
    nombre:       str
    descripcion:  Optional[str] = ""
    precio:       float
    stock:        Optional[int] = 0
    stock_minimo: Optional[int] = 10

class ReciboItemCreate(BaseModel):
    producto_id:     Optional[int] = None
    cantidad:        int
    precio_unitario: float
    subtotal:        float

class ReciboCreate(BaseModel):
    numero:       str
    cliente_id:   Optional[int] = None
    subtotal:     Optional[float] = 0
    descuento:    Optional[float] = 0
    total:        float
    estado:       Optional[str] = "PENDIENTE"
    observaciones: Optional[str] = ""
    items:        Optional[List[ReciboItemCreate]] = []

class ReservaCreate(BaseModel):
    cliente_id:    Optional[int] = None
    producto_id:   Optional[int] = None
    cantidad:      int
    fecha_entrega: Optional[str] = ""
    estado:        Optional[str] = "PENDIENTE"
    observaciones: Optional[str] = ""

class VentaCreate(BaseModel):
    recibo_id:  Optional[int] = None
    cliente_id: Optional[int] = None
    total:      float
    forma_pago: Optional[str] = ""
    vendedor:   Optional[str] = ""

class CuentaCorrienteCreate(BaseModel):
    cliente_id: int
    tipo:       str  # 'DEBE' o 'HABER'
    monto:      float
    concepto:   Optional[str] = ""
    recibo_id:  Optional[int] = None

# --- APP ---

app = FastAPI(
    title="ERP System API",
    description="Backend REST para Sistema ERP Profesional",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

# --- ROOT ---

@app.get("/")
def root():
    return {"sistema": "ERP System API", "version": "1.0.0", "estado": "activo"}

# --- STATS ---

@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    from sqlalchemy import func
    total_recibos    = db.query(func.count(Recibo.id)).scalar() or 0
    productos_stock  = db.query(func.count(Producto.id)).filter(Producto.stock > 0).scalar() or 0
    reservas_activas = db.query(func.count(Reserva.id)).filter(Reserva.estado == "PENDIENTE").scalar() or 0
    ventas_mes = db.query(func.coalesce(func.sum(Venta.total), 0)).filter(
        func.extract("month", Venta.fecha) == datetime.datetime.utcnow().month,
        func.extract("year",  Venta.fecha) == datetime.datetime.utcnow().year
    ).scalar() or 0

    # Clientes con deuda
    debe  = db.query(CuentaCorriente.cliente_id, func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.tipo == "DEBE").group_by(CuentaCorriente.cliente_id).all()
    haber = db.query(CuentaCorriente.cliente_id, func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.tipo == "HABER").group_by(CuentaCorriente.cliente_id).all()
    haber_dict = {h[0]: h[1] for h in haber}
    clientes_con_deuda = sum(1 for d in debe if d[1] > haber_dict.get(d[0], 0))

    return {
        "total_recibos":      total_recibos,
        "clientes_con_deuda": clientes_con_deuda,
        "productos_stock":    productos_stock,
        "reservas_activas":   reservas_activas,
        "ventas_mes":         float(ventas_mes)
    }

# ==========================================
# CLIENTES
# ==========================================

@app.get("/clientes")
def list_clientes(db: Session = Depends(get_db)):
    return db.query(Cliente).filter(Cliente.activo == True).order_by(Cliente.nombre).all()

@app.get("/clientes/{cid}")
def get_cliente(cid: int, db: Session = Depends(get_db)):
    c = db.query(Cliente).filter(Cliente.id == cid).first()
    if not c:
        raise HTTPException(404, "Cliente no encontrado")
    return c

@app.post("/clientes")
def create_cliente(data: ClienteCreate, db: Session = Depends(get_db)):
    c = Cliente(**data.dict())
    db.add(c)
    db.commit()
    db.refresh(c)
    return c

@app.put("/clientes/{cid}")
def update_cliente(cid: int, data: ClienteCreate, db: Session = Depends(get_db)):
    c = db.query(Cliente).filter(Cliente.id == cid).first()
    if not c:
        raise HTTPException(404, "Cliente no encontrado")
    for k, v in data.dict().items():
        setattr(c, k, v)
    db.commit()
    return {"status": "ok"}

@app.delete("/clientes/{cid}")
def delete_cliente(cid: int, db: Session = Depends(get_db)):
    c = db.query(Cliente).filter(Cliente.id == cid).first()
    if not c:
        raise HTTPException(404, "Cliente no encontrado")
    try:
        c.activo = False
        db.commit()
        return {"status": "ok"}
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "No se puede eliminar: tiene datos asociados")

# ==========================================
# PRODUCTOS
# ==========================================

@app.get("/productos")
def list_productos(db: Session = Depends(get_db)):
    return db.query(Producto).filter(Producto.activo == True).order_by(Producto.nombre).all()

@app.get("/productos/{pid}")
def get_producto(pid: int, db: Session = Depends(get_db)):
    p = db.query(Producto).filter(Producto.id == pid).first()
    if not p:
        raise HTTPException(404, "Producto no encontrado")
    return p

@app.post("/productos")
def create_producto(data: ProductoCreate, db: Session = Depends(get_db)):
    p = Producto(**data.dict())
    db.add(p)
    db.commit()
    db.refresh(p)
    return p

@app.put("/productos/{pid}")
def update_producto(pid: int, data: ProductoCreate, db: Session = Depends(get_db)):
    p = db.query(Producto).filter(Producto.id == pid).first()
    if not p:
        raise HTTPException(404, "Producto no encontrado")
    for k, v in data.dict().items():
        setattr(p, k, v)
    db.commit()
    return {"status": "ok"}

@app.patch("/productos/{pid}/stock")
def update_stock(pid: int, stock: int, db: Session = Depends(get_db)):
    p = db.query(Producto).filter(Producto.id == pid).first()
    if not p:
        raise HTTPException(404, "Producto no encontrado")
    p.stock = stock
    db.commit()
    return {"status": "ok"}

@app.delete("/productos/{pid}")
def delete_producto(pid: int, db: Session = Depends(get_db)):
    p = db.query(Producto).filter(Producto.id == pid).first()
    if not p:
        raise HTTPException(404, "Producto no encontrado")
    p.activo = False
    db.commit()
    return {"status": "ok"}

# ==========================================
# RECIBOS
# ==========================================

@app.get("/recibos")
def list_recibos(limit: int = 100, db: Session = Depends(get_db)):
    recibos = db.query(Recibo).order_by(Recibo.fecha.desc()).limit(limit).all()
    result = []
    for r in recibos:
        cliente = db.query(Cliente).filter(Cliente.id == r.cliente_id).first()
        result.append({
            "id":       r.id,
            "numero":   r.numero,
            "cliente":  cliente.nombre if cliente else "Sin cliente",
            "fecha":    r.fecha.isoformat() if r.fecha else None,
            "total":    r.total,
            "estado":   r.estado
        })
    return result

@app.get("/recibos/filtrar")
def filtrar_recibos(desde: str, hasta: str, db: Session = Depends(get_db)):
    recibos = db.query(Recibo).filter(
        Recibo.fecha >= desde,
        Recibo.fecha <= hasta + " 23:59:59"
    ).order_by(Recibo.fecha.desc()).all()
    result = []
    for r in recibos:
        cliente = db.query(Cliente).filter(Cliente.id == r.cliente_id).first()
        result.append({
            "id":      r.id,
            "numero":  r.numero,
            "cliente": cliente.nombre if cliente else "Sin cliente",
            "fecha":   r.fecha.isoformat() if r.fecha else None,
            "total":   r.total,
            "estado":  r.estado
        })
    return result

@app.get("/recibos/{rid}")
def get_recibo(rid: int, db: Session = Depends(get_db)):
    r = db.query(Recibo).filter(Recibo.id == rid).first()
    if not r:
        raise HTTPException(404, "Recibo no encontrado")
    return r

@app.post("/recibos")
def create_recibo(data: ReciboCreate, db: Session = Depends(get_db)):
    items = data.items or []
    recibo_data = data.dict()
    recibo_data.pop("items")
    r = Recibo(**recibo_data)
    db.add(r)
    db.flush()
    for item in items:
        db.add(ReciboDetalle(recibo_id=r.id, **item.dict()))
    db.commit()
    db.refresh(r)
    return r

@app.patch("/recibos/{rid}/estado")
def update_estado_recibo(rid: int, estado: str, db: Session = Depends(get_db)):
    r = db.query(Recibo).filter(Recibo.id == rid).first()
    if not r:
        raise HTTPException(404, "Recibo no encontrado")
    r.estado = estado
    db.commit()
    return {"status": "ok"}

@app.delete("/recibos/{rid}")
def delete_recibo(rid: int, db: Session = Depends(get_db)):
    r = db.query(Recibo).filter(Recibo.id == rid).first()
    if not r:
        raise HTTPException(404, "Recibo no encontrado")
    db.delete(r)
    db.commit()
    return {"status": "deleted"}

# ==========================================
# RESERVAS
# ==========================================

@app.get("/reservas")
def list_reservas(db: Session = Depends(get_db)):
    reservas = db.query(Reserva).order_by(Reserva.fecha_reserva.desc()).all()
    result = []
    for r in reservas:
        cliente = db.query(Cliente).filter(Cliente.id == r.cliente_id).first()
        producto = db.query(Producto).filter(Producto.id == r.producto_id).first()
        result.append({
            "id":            r.id,
            "cliente":       cliente.nombre if cliente else "N/A",
            "producto":      producto.nombre if producto else "N/A",
            "cantidad":      r.cantidad,
            "fecha_entrega": r.fecha_entrega,
            "estado":        r.estado,
            "observaciones": r.observaciones
        })
    return result

@app.post("/reservas")
def create_reserva(data: ReservaCreate, db: Session = Depends(get_db)):
    r = Reserva(**data.dict())
    db.add(r)
    db.commit()
    db.refresh(r)
    return r

@app.patch("/reservas/{rid}/estado")
def update_estado_reserva(rid: int, estado: str, db: Session = Depends(get_db)):
    r = db.query(Reserva).filter(Reserva.id == rid).first()
    if not r:
        raise HTTPException(404, "Reserva no encontrada")
    r.estado = estado
    db.commit()
    return {"status": "ok"}

@app.delete("/reservas/{rid}")
def delete_reserva(rid: int, db: Session = Depends(get_db)):
    r = db.query(Reserva).filter(Reserva.id == rid).first()
    if not r:
        raise HTTPException(404, "Reserva no encontrada")
    db.delete(r)
    db.commit()
    return {"status": "deleted"}

# ==========================================
# VENTAS
# ==========================================

@app.get("/ventas")
def list_ventas(limit: int = 100, db: Session = Depends(get_db)):
    ventas = db.query(Venta).order_by(Venta.fecha.desc()).limit(limit).all()
    result = []
    for v in ventas:
        cliente = db.query(Cliente).filter(Cliente.id == v.cliente_id).first()
        result.append({
            "id":        v.id,
            "fecha":     v.fecha.isoformat() if v.fecha else None,
            "cliente":   cliente.nombre if cliente else "Sin cliente",
            "total":     v.total,
            "forma_pago": v.forma_pago,
            "vendedor":  v.vendedor
        })
    return result

@app.get("/ventas/filtrar")
def filtrar_ventas(desde: str, hasta: str, db: Session = Depends(get_db)):
    ventas = db.query(Venta).filter(
        Venta.fecha >= desde,
        Venta.fecha <= hasta + " 23:59:59"
    ).order_by(Venta.fecha.desc()).all()
    result = []
    for v in ventas:
        cliente = db.query(Cliente).filter(Cliente.id == v.cliente_id).first()
        result.append({
            "id":        v.id,
            "fecha":     v.fecha.isoformat() if v.fecha else None,
            "cliente":   cliente.nombre if cliente else "Sin cliente",
            "total":     v.total,
            "forma_pago": v.forma_pago,
            "vendedor":  v.vendedor
        })
    return result

@app.post("/ventas")
def create_venta(data: VentaCreate, db: Session = Depends(get_db)):
    v = Venta(**data.dict())
    db.add(v)
    db.commit()
    db.refresh(v)
    return v

# ==========================================
# CUENTA CORRIENTE
# ==========================================

@app.get("/cuenta-corriente/{cliente_id}")
def get_cuenta_corriente(cliente_id: int, db: Session = Depends(get_db)):
    movs = db.query(CuentaCorriente).filter(
        CuentaCorriente.cliente_id == cliente_id
    ).order_by(CuentaCorriente.fecha.desc()).all()
    result = []
    for m in movs:
        result.append({
            "id":       m.id,
            "tipo":     m.tipo,
            "monto":    m.monto,
            "concepto": m.concepto,
            "fecha":    m.fecha.isoformat() if m.fecha else None,
            "saldo":    m.saldo
        })
    return result

@app.get("/cuenta-corriente/{cliente_id}/saldo")
def get_saldo(cliente_id: int, db: Session = Depends(get_db)):
    from sqlalchemy import func
    debe  = db.query(func.coalesce(func.sum(CuentaCorriente.monto), 0)).filter(CuentaCorriente.cliente_id == cliente_id, CuentaCorriente.tipo == "DEBE").scalar() or 0
    haber = db.query(func.coalesce(func.sum(CuentaCorriente.monto), 0)).filter(CuentaCorriente.cliente_id == cliente_id, CuentaCorriente.tipo == "HABER").scalar() or 0
    return {"saldo": float(debe) - float(haber)}

@app.post("/cuenta-corriente")
def create_movimiento(data: CuentaCorrienteCreate, db: Session = Depends(get_db)):
    m = CuentaCorriente(**data.dict())
    db.add(m)
    db.commit()
    db.refresh(m)
    return m