import os
import datetime
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Numeric, func, desc
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship

# --- CONFIGURACIÓN BASE DE DATOS ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./sistema_erp.db")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True,
    # connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {} 
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELOS SQLALCHEMY ---

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
    descripcion  = Column(String, nullable=True) # Usado como Categoría en la UI
    precio       = Column(Numeric(12, 2), nullable=False, default=0)
    stock        = Column(Integer, nullable=False, default=0)
    stock_minimo = Column(Integer, default=10)
    activo       = Column(Boolean, default=True)

class Recibo(Base):
    __tablename__ = "recibos"
    id          = Column(Integer, primary_key=True, index=True)
    numero      = Column(String, unique=True, nullable=False)
    cliente_id  = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    fecha       = Column(DateTime, default=datetime.datetime.utcnow)
    total       = Column(Numeric(12, 2), nullable=False, default=0)
    estado      = Column(String, default="PENDIENTE")
    observaciones = Column(String, nullable=True)
    items       = relationship("ReciboDetalle", back_populates="recibo", cascade="all, delete-orphan")

class ReciboDetalle(Base):
    __tablename__ = "recibos_detalle"
    id              = Column(Integer, primary_key=True, index=True)
    recibo_id       = Column(Integer, ForeignKey("recibos.id"))
    producto_id     = Column(Integer, ForeignKey("productos.id"), nullable=True)
    cantidad        = Column(Integer, nullable=False)
    precio_unitario = Column(Numeric(12, 2), nullable=False)
    subtotal        = Column(Numeric(12, 2), nullable=False)
    recibo          = relationship("Recibo", back_populates="items")

class Reserva(Base):
    __tablename__ = "reservas"
    id             = Column(Integer, primary_key=True, index=True)
    # Permitimos texto libre si no hay ID relacionado (flexibilidad UI)
    cliente_txt    = Column(String, nullable=True) 
    producto_txt   = Column(String, nullable=True)
    cantidad       = Column(Integer, nullable=False, default=1)
    fecha_reserva  = Column(DateTime, default=datetime.datetime.utcnow)
    fecha_entrega  = Column(String, nullable=True) # Guardamos como ISO string
    estado         = Column(String, default="PENDIENTE")
    observaciones  = Column(String, nullable=True)

class Venta(Base):
    __tablename__ = "ventas"
    id          = Column(Integer, primary_key=True, index=True)
    recibo_id   = Column(Integer, ForeignKey("recibos.id"), nullable=True)
    cliente_id  = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    fecha       = Column(DateTime, default=datetime.datetime.utcnow)
    total       = Column(Numeric(12, 2), nullable=False)
    forma_pago  = Column(String, nullable=True)
    vendedor    = Column(String, nullable=True)

class CuentaCorriente(Base):
    __tablename__ = "cuenta_corriente"
    id          = Column(Integer, primary_key=True, index=True)
    cliente_id  = Column(Integer, ForeignKey("clientes.id"))
    tipo        = Column(String, nullable=False)  # 'DEBE' (generado por recibo) o 'HABER' (pago)
    monto       = Column(Numeric(12, 2), nullable=False)
    concepto    = Column(String, nullable=True)
    recibo_id   = Column(Integer, ForeignKey("recibos.id"), nullable=True)
    fecha       = Column(DateTime, default=datetime.datetime.utcnow)

# --- SCHEMAS PYDANTIC ---

class ClienteCreate(BaseModel):
    nombre: str
    documento: Optional[str] = ""
    telefono: Optional[str] = ""
    email: Optional[str] = ""
    direccion: Optional[str] = ""

class ProductoCreate(BaseModel):
    codigo: str
    nombre: str
    descripcion: Optional[str] = "" # Categoria
    precio: float
    stock: int
    stock_minimo: int

class ReciboItemCreate(BaseModel):
    producto_id: int
    cantidad: int
    precio_unitario: float
    subtotal: float

class ReciboCreate(BaseModel):
    numero: str
    cliente_id: int
    total: float
    estado: Optional[str] = "PENDIENTE"
    observaciones: Optional[str] = ""
    descuento: Optional[float] = 0
    items: List[ReciboItemCreate]

class ReservaCreate(BaseModel):
    cliente_nombre_temp: str
    producto_nombre_temp: str
    cantidad: int
    fecha_entrega: Optional[str] = ""
    estado: str
    observaciones: Optional[str] = ""

# --- APP ---

app = FastAPI(title="ERP System API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

# --- ENDPOINTS ---

@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    # Datos reales para el Dashboard
    total_recibos = db.query(func.count(Recibo.id)).scalar()
    stock_items = db.query(func.count(Producto.id)).filter(Producto.stock > 0).scalar()
    reservas = db.query(func.count(Reserva.id)).filter(Reserva.estado == "PENDIENTE").scalar()
    
    # Ventas del mes
    now = datetime.datetime.now()
    ventas_mes = db.query(func.sum(Recibo.total)).filter(
        func.extract('month', Recibo.fecha) == now.month,
        func.extract('year', Recibo.fecha) == now.year
    ).scalar() or 0

    # Clientes con deuda (cálculo rápido)
    # Esto es una simplificación. En producción se haría con una query agregada compleja.
    clientes_deuda = 0
    clientes = db.query(Cliente.id).all()
    for cli in clientes:
        debe = db.query(func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.cliente_id == cli.id, CuentaCorriente.tipo == "DEBE").scalar() or 0
        haber = db.query(func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.cliente_id == cli.id, CuentaCorriente.tipo == "HABER").scalar() or 0
        if (debe - haber) > 1: # Margen de error de $1
            clientes_deuda += 1

    return {
        "total_recibos": total_recibos or 0,
        "clientes_con_deuda": clientes_deuda,
        "productos_stock": stock_items or 0,
        "reservas_activas": reservas or 0,
        "ventas_mes": float(ventas_mes)
    }

# === CLIENTES ===
@app.get("/clientes")
def list_clientes(db: Session = Depends(get_db)):
    return db.query(Cliente).filter(Cliente.activo == True).order_by(Cliente.nombre).all()

@app.post("/clientes")
def create_cliente(data: ClienteCreate, db: Session = Depends(get_db)):
    c = Cliente(**data.dict())
    db.add(c)
    db.commit()
    db.refresh(c)
    return c

# === PRODUCTOS ===
@app.get("/productos")
def list_productos(db: Session = Depends(get_db)):
    return db.query(Producto).filter(Producto.activo == True).order_by(Producto.nombre).all()

@app.post("/productos")
def create_producto(data: ProductoCreate, db: Session = Depends(get_db)):
    p = Producto(**data.dict())
    db.add(p)
    db.commit()
    return p

@app.put("/productos/{pid}")
def update_producto(pid: int, data: ProductoCreate, db: Session = Depends(get_db)):
    p = db.query(Producto).filter(Producto.id == pid).first()
    if not p: raise HTTPException(404, "Producto no encontrado")
    
    p.nombre = data.nombre
    p.descripcion = data.descripcion # Categoria
    p.precio = data.precio
    p.stock = data.stock
    p.stock_minimo = data.stock_minimo
    # No actualizamos codigo si no es necesario
    
    db.commit()
    return {"status": "ok"}

# === RECIBOS Y STOCK (CORE) ===
@app.get("/recibos")
def list_recibos(limit: int = 100, db: Session = Depends(get_db)):
    recibos = db.query(Recibo).order_by(desc(Recibo.fecha)).limit(limit).all()
    res = []
    for r in recibos:
        cli = db.query(Cliente).filter(Cliente.id == r.cliente_id).first()
        res.append({
            "id": r.id, "numero": r.numero,
            "cliente": cli.nombre if cli else "Desconocido",
            "fecha": r.fecha.isoformat(),
            "total": float(r.total),
            "estado": r.estado
        })
    return res

@app.post("/recibos")
def create_recibo(data: ReciboCreate, db: Session = Depends(get_db)):
    """
    Crea Recibo + Descuenta Stock + Genera Deuda en Cta Cte
    """
    try:
        # 1. Crear Recibo
        new_recibo = Recibo(
            numero=data.numero,
            cliente_id=data.cliente_id,
            total=data.total,
            estado=data.estado,
            observaciones=data.observaciones
        )
        db.add(new_recibo)
        db.flush() # Obtener ID

        # 2. Procesar Items y Stock
        for item in data.items:
            # Validar producto
            prod = db.query(Producto).filter(Producto.id == item.producto_id).first()
            if not prod:
                raise HTTPException(400, f"Producto ID {item.producto_id} no existe")
            
            # Descontar stock
            if prod.stock < item.cantidad:
                # Opcional: Permitir stock negativo o lanzar error. 
                # Aquí permitimos negativo para no bloquear ventas, pero es decisión de negocio.
                pass 
            
            prod.stock -= item.cantidad
            
            # Crear detalle
            det = ReciboDetalle(
                recibo_id=new_recibo.id,
                producto_id=item.producto_id,
                cantidad=item.cantidad,
                precio_unitario=item.precio_unitario,
                subtotal=item.subtotal
            )
            db.add(det)

        # 3. Generar Cuenta Corriente (DEBE)
        # Todo recibo PENDIENTE genera deuda. Si se paga al contado, 
        # el frontend debería mandar otro request para saldar o crear un recibo PAGADO.
        # Asumimos que genera deuda siempre y luego se paga.
        cc_entry = CuentaCorriente(
            cliente_id=data.cliente_id,
            tipo="DEBE",
            monto=data.total,
            concepto=f"Recibo {data.numero}",
            recibo_id=new_recibo.id
        )
        db.add(cc_entry)
        
        # 4. Registrar Venta (para reporte de ventas)
        venta = Venta(
            recibo_id=new_recibo.id,
            cliente_id=data.cliente_id,
            total=data.total,
            forma_pago="Cuenta Corriente",
            vendedor="Admin"
        )
        db.add(venta)

        db.commit()
        return {"status": "ok", "recibo_id": new_recibo.id}

    except Exception as e:
        db.rollback()
        print(f"Error creando recibo: {e}")
        raise HTTPException(500, str(e))

# === CUENTA CORRIENTE ===
@app.get("/cuenta-corriente/{cliente_id}")
def get_cc(cliente_id: int, db: Session = Depends(get_db)):
    movs = db.query(CuentaCorriente).filter(CuentaCorriente.cliente_id == cliente_id).order_by(desc(CuentaCorriente.fecha)).all()
    
    # Calculamos saldo acumulado visualmente (opcional, o se hace en frontend)
    # Aquí devolvemos la lista cruda
    res = []
    saldo_acumulado = 0 # Esto es complejo de calcular en orden inverso sin window functions
    
    # Calculamos saldo total primero
    debe = db.query(func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.cliente_id == cliente_id, CuentaCorriente.tipo == "DEBE").scalar() or 0
    haber = db.query(func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.cliente_id == cliente_id, CuentaCorriente.tipo == "HABER").scalar() or 0
    saldo_actual = float(debe) - float(haber)
    
    # Devolvemos movimientos con saldo snapshot (simple)
    for m in movs:
        res.append({
            "id": m.id,
            "fecha": m.fecha.isoformat(),
            "tipo": m.tipo,
            "monto": float(m.monto),
            "concepto": m.concepto,
            "saldo": 0 # El frontend calcula el historial gráfico
        })
    return res

@app.get("/cuenta-corriente/{cliente_id}/saldo")
def get_saldo_cliente(cliente_id: int, db: Session = Depends(get_db)):
    debe = db.query(func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.cliente_id == cliente_id, CuentaCorriente.tipo == "DEBE").scalar() or 0
    haber = db.query(func.sum(CuentaCorriente.monto)).filter(CuentaCorriente.cliente_id == cliente_id, CuentaCorriente.tipo == "HABER").scalar() or 0
    return {"saldo": float(debe) - float(haber)}

# === RESERVAS ===
@app.get("/reservas")
def list_reservas(db: Session = Depends(get_db)):
    reservas = db.query(Reserva).order_by(desc(Reserva.fecha_reserva)).all()
    res = []
    for r in reservas:
        res.append({
            "id": r.id,
            "cliente": r.cliente_txt,
            "producto": r.producto_txt, # O servicio
            "cantidad": r.cantidad,
            "fecha_entrega": r.fecha_entrega,
            "estado": r.estado
        })
    return res

@app.post("/reservas")
def create_reserva(data: ReservaCreate, db: Session = Depends(get_db)):
    r = Reserva(
        cliente_txt=data.cliente_nombre_temp,
        producto_txt=data.producto_nombre_temp,
        cantidad=data.cantidad,
        fecha_entrega=data.fecha_entrega,
        estado=data.estado,
        observaciones=data.observaciones
    )
    db.add(r)
    db.commit()
    return r

@app.patch("/reservas/{rid}/estado")
def update_reserva_estado(rid: int, estado: str, db: Session = Depends(get_db)):
    r = db.query(Reserva).filter(Reserva.id == rid).first()
    if not r: raise HTTPException(404)
    r.estado = estado
    db.commit()
    return {"status": "ok"}

# === VENTAS (Reporte) ===
@app.get("/ventas")
def list_ventas(limit: int = 100, db: Session = Depends(get_db)):
    ventas = db.query(Venta).order_by(desc(Venta.fecha)).limit(limit).all()
    res = []
    for v in ventas:
        cli = db.query(Cliente).filter(Cliente.id == v.cliente_id).first()
        res.append({
            "id": v.id,
            "fecha": v.fecha.isoformat(),
            "cliente": cli.nombre if cli else "Desconocido",
            "total": float(v.total),
            "forma_pago": v.forma_pago,
            "vendedor": v.vendedor
        })
    return res
