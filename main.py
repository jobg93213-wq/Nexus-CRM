from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, DateTime, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

DATABASE_URL = "sqlite:///./crm.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────── MODELS ───────────

class PipelineDB(Base):
    __tablename__ = "pipelines"
    pipeline_id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    client_type = Column(String, nullable=False, default="физ")  # "физ" или "юр"
    description = Column(String, nullable=True)
    # Стадии воронки хранятся строкой через "|", порядок важен
    stages = Column(String, nullable=False)


class ManagerDB(Base):
    __tablename__ = "managers"
    manager_id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    phone = Column(String, nullable=True)
    email = Column(String, nullable=True)


class LeadDB(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    phone = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, default="Новый клиент")
    loss_reason_id = Column(Integer, ForeignKey("loss_reasons.reason_id"), nullable=True)
    # Соцсети лида
    telegram = Column(String, nullable=True)
    viber = Column(String, nullable=True)
    vk = Column(String, nullable=True)
    # Воронка / менеджер / тип клиента
    pipeline_id = Column(Integer, ForeignKey("pipelines.pipeline_id"), nullable=False, default=1)
    manager_id = Column(Integer, ForeignKey("managers.manager_id"), nullable=True)
    client_type = Column(String, nullable=False, default="физ")  # "физ" или "юр"


class StageHistoryDB(Base):
    __tablename__ = "stage_history"
    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String, nullable=False)
    old_status = Column(String, nullable=False)
    new_status = Column(String, nullable=False)
    changed_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class LossReasonDB(Base):
    __tablename__ = "loss_reasons"
    reason_id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)


class TaskDB(Base):
    __tablename__ = "tasks"
    task_id = Column(Integer, primary_key=True, index=True)
    phone = Column(String, ForeignKey("leads.phone"), nullable=False)
    description = Column(String, nullable=False)
    deadline = Column(Integer, nullable=False)
    is_completed = Column(Boolean, default=False)


# ─────────── DB INIT ───────────

Base.metadata.create_all(bind=engine)


def run_migrations():
    """Идемпотентные миграции для уже существующей БД (crm.db)."""
    with engine.begin() as conn:
        try:
            existing = {row[1] for row in conn.execute(text("PRAGMA table_info(leads)"))}
        except Exception:
            existing = set()

        # Удалить budget если остался от старой версии модели
        if "budget" in existing:
            try:
                conn.execute(text("ALTER TABLE leads DROP COLUMN budget"))
                print("[migrate] dropped legacy column: budget")
            except Exception as e:
                print(f"[migrate] drop budget: {e}")

        # Удалить instagram если был (заменили на viber)
        if "instagram" in existing:
            try:
                conn.execute(text("ALTER TABLE leads DROP COLUMN instagram"))
                print("[migrate] dropped legacy column: instagram")
            except Exception as e:
                print(f"[migrate] drop instagram: {e}")

        # Добавить соцсети (на случай если БД совсем старая)
        for col in ("telegram", "viber", "vk"):
            if col not in existing:
                try:
                    conn.execute(text(f"ALTER TABLE leads ADD COLUMN {col} VARCHAR"))
                    print(f"[migrate] added column: {col}")
                except Exception as e:
                    print(f"[migrate] add {col}: {e}")

        # Добавить поля воронок/менеджеров/типа клиента
        if "pipeline_id" not in existing:
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN pipeline_id INTEGER DEFAULT 1"))
                print("[migrate] added column: pipeline_id")
            except Exception as e:
                print(f"[migrate] add pipeline_id: {e}")
        if "manager_id" not in existing:
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN manager_id INTEGER"))
                print("[migrate] added column: manager_id")
            except Exception as e:
                print(f"[migrate] add manager_id: {e}")
        if "client_type" not in existing:
            try:
                conn.execute(text("ALTER TABLE leads ADD COLUMN client_type VARCHAR DEFAULT 'физ'"))
                print("[migrate] added column: client_type")
            except Exception as e:
                print(f"[migrate] add client_type: {e}")


run_migrations()


# ─────────── СИД: ВОРОНКИ, МЕНЕДЖЕРЫ, ДЕМО-ЛИД (тематика: клининг) ───────────

# Все стадии каждой воронки прописаны через "|", порядок = порядок движения сделки по воронке.
DEFAULT_PIPELINES = [
    dict(
        pipeline_id=1,
        name="Физические лица — уборка квартир/домов",
        client_type="физ",
        description="Разовая и регулярная уборка жилья для частных клиентов",
        stages="Новая заявка|Уточнение деталей заказа|Расчет стоимости|Согласование даты и времени|"
               "Бронирование подтверждено|Уборка выполнена|Оплата получена|Повторный заказ|Отказ",
    ),
    dict(
        pipeline_id=2,
        name="Юридические лица — клининг офисов",
        client_type="юр",
        description="B2B продажи клининга офисов и коммерческих помещений",
        stages="Первый контакт|Квалификация заявки|Выезд на замер объекта|Коммерческое предложение отправлено|"
               "Переговоры / согласование условий|Договор подписан|Первая уборка выполнена|Регулярный контракт|Отказ",
    ),
    dict(
        pipeline_id=3,
        name="Генеральная уборка",
        client_type="физ",
        description="Комплексная генеральная уборка квартир, домов, коттеджей",
        stages="Заявка|Оценка объема работ|Расчет стоимости|Согласование даты|Уборка выполнена|Оплата получена|Отказ",
    ),
    dict(
        pipeline_id=4,
        name="Уборка после ремонта",
        client_type="физ",
        description="Клининг после строительных и ремонтных работ",
        stages="Заявка|Осмотр объекта|Расчет стоимости|Согласование даты|Работы выполнены|Оплата получена|Отказ",
    ),
    dict(
        pipeline_id=5,
        name="Химчистка мебели и ковров",
        client_type="физ",
        description="Химчистка мягкой мебели, ковров, матрасов на выезде",
        stages="Заявка|Уточнение изделий и загрязнений|Расчет стоимости|Выезд мастера|Химчистка выполнена|Оплата получена|Отказ",
    ),
    dict(
        pipeline_id=6,
        name="Мойка окон и фасадов",
        client_type="юр",
        description="Мойка окон, витражей и фасадов зданий, в т.ч. с альпинистами",
        stages="Заявка|Замер объекта|Коммерческое предложение|Согласование условий|Работы выполнены|Оплата получена|Отказ",
    ),
    dict(
        pipeline_id=7,
        name="Абонементное обслуживание (регулярный клининг)",
        client_type="юр",
        description="Продажа регулярного обслуживания по подписке/абонементу",
        stages="Заявка|Презентация условий абонемента|Пробная уборка|Согласование графика|Договор подписан|"
               "Активный абонемент|Отказ",
    ),
    dict(
        pipeline_id=8,
        name="Клининг после потопа/пожара (аварийные работы)",
        client_type="физ",
        description="Срочный клининг после ЧП: затопление, пожар, форс-мажор",
        stages="Экстренная заявка|Выезд специалиста|Оценка ущерба и объема работ|Расчет стоимости|"
               "Работы выполнены|Оплата получена|Отказ",
    ),
]


def seed_data():
    with SessionLocal() as db:
        # Воронки
        if db.query(PipelineDB).count() == 0:
            for p in DEFAULT_PIPELINES:
                db.add(PipelineDB(**p))
            db.commit()
            print("[seed] добавлены воронки продаж (8 шт.)")

        # Менеджер по умолчанию
        manager = db.query(ManagerDB).filter(ManagerDB.manager_id == 1).first()
        if not manager:
            manager = ManagerDB(manager_id=1, name="Менеджер 1", phone=None, email=None)
            db.add(manager)
            db.commit()
            print("[seed] добавлен менеджер: Менеджер 1")

        # Демо-клиент для воронки "Физические лица"
        demo_phone = "+70000000001"
        demo_lead = db.query(LeadDB).filter(LeadDB.phone == demo_phone).first()
        if not demo_lead:
            demo_lead = LeadDB(
                name="Клиент 1",
                phone=demo_phone,
                status="Новая заявка",
                pipeline_id=1,
                manager_id=1,
                client_type="физ",
            )
            db.add(demo_lead)
            db.commit()
            print("[seed] добавлен демо-клиент: Клиент 1 (воронка «Физические лица»)")


seed_data()


# ─────────── APP ───────────

app = FastAPI(title="CRM")

# ─────────── БАЗОВАЯ АВТОРИЗАЦИЯ (HTTP Basic) ───────────
import base64
import secrets
from starlette.requests import Request
from starlette.responses import JSONResponse

BASIC_AUTH_LOGIN = "developer"
BASIC_AUTH_PASSWORD = "123456CRM"


@app.middleware("http")
async def basic_auth_middleware(request: Request, call_next):
    # Preflight-запросы браузера пропускаем без проверки, иначе сломается CORS
    if request.method == "OPTIONS":
        return await call_next(request)

    # Публичные роуты — без Basic Auth
    if request.url.path in ("/public/leads", "/widget.js"):
        return await call_next(request)

    auth_header = request.headers.get("Authorization")
    unauthorized = JSONResponse(
        status_code=401,
        content={"error": "Требуется авторизация"},
        headers={"WWW-Authenticate": "Basic"},
    )

    if not auth_header or not auth_header.startswith("Basic "):
        return unauthorized

    try:
        decoded = base64.b64decode(auth_header.split(" ", 1)[1]).decode("utf-8")
        login, _, password = decoded.partition(":")
    except Exception:
        return unauthorized

    valid_login = secrets.compare_digest(login, BASIC_AUTH_LOGIN)
    valid_password = secrets.compare_digest(password, BASIC_AUTH_PASSWORD)
    if not (valid_login and valid_password):
        return unauthorized

    return await call_next(request)


# CORS должен быть добавлен ПОСЛЕ basic_auth_middleware, чтобы обернуть его снаружи —
# тогда CORS-заголовки будут присутствовать даже в ответах 401.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────── SCHEMAS ───────────

class Lead(BaseModel):
    name: str
    phone: str
    status: Optional[str] = None
    loss_reason_id: Optional[int] = None
    telegram: Optional[str] = None
    viber: Optional[str] = None
    vk: Optional[str] = None
    pipeline_id: int = 1
    manager_id: Optional[int] = None
    client_type: str = "физ"


class Pipeline(BaseModel):
    pipeline_id: int
    name: str
    client_type: str = "физ"
    description: Optional[str] = None
    stages: str  # стадии через "|"


class Manager(BaseModel):
    manager_id: int
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None


class StageHistory(BaseModel):
    phone: str
    old_status: str
    new_status: str


class LossReason(BaseModel):
    reason_id: int
    title: str


class Task(BaseModel):
    task_id: int
    phone: str
    description: str
    deadline: int
    is_completed: bool = False


ALLOW_STATUSES = [
    "Новый клиент", "В работе", "Выставить счет", "Просчитать заказ",
    "Созвониться", "Ответить Email", "Сделать рассылку",
    "Ответить в мессенджере", "Выслать прайс", "Поздравить с днем рождения",
    "Отказ"
]


def _clean(v):
    """Пустые строки из формы -> NULL в БД."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


# ─────────── ENDPOINTS ───────────

@app.post("/leads")
def post_zapr(lead: Lead, db: Session = Depends(get_db)):
    existing_lead = db.query(LeadDB).filter(LeadDB.phone == lead.phone).first()
    if existing_lead:
        return {"error": "Лид с таким номером телефона уже существует"}

    pipeline = db.query(PipelineDB).filter(PipelineDB.pipeline_id == lead.pipeline_id).first()
    if not pipeline:
        return {"error": f"Воронка с id {lead.pipeline_id} не найдена"}

    if lead.manager_id is not None:
        manager = db.query(ManagerDB).filter(ManagerDB.manager_id == lead.manager_id).first()
        if not manager:
            return {"error": f"Менеджер с id {lead.manager_id} не найден"}

    stages = pipeline.stages.split("|")
    status = lead.status if (lead.status and lead.status in stages) else stages[0]

    db_lead = LeadDB(
        name=lead.name,
        phone=lead.phone,
        status=status,
        loss_reason_id=lead.loss_reason_id,
        telegram=_clean(lead.telegram),
        viber=_clean(lead.viber),
        vk=_clean(lead.vk),
        pipeline_id=lead.pipeline_id,
        manager_id=lead.manager_id,
        client_type=lead.client_type,
    )
    db.add(db_lead)
    db.commit()
    db.refresh(db_lead)
    return {
        "message": "Лид успешно добавлен",
        "data": lead.model_dump()
    }


@app.get("/leads")
def get_zapr(db: Session = Depends(get_db)):
    leads = db.query(LeadDB).all()
    return leads


@app.put("/leads/update-status")
def put_zapros(phone: str, new_status: str, loss_reason_id: Optional[int] = None, db: Session = Depends(get_db)):
    lead = db.query(LeadDB).filter(LeadDB.phone == phone).first()
    if not lead:
        return {"error": "Клиент с таким номером не найден."}

    pipeline = db.query(PipelineDB).filter(PipelineDB.pipeline_id == lead.pipeline_id).first()
    allowed_statuses = pipeline.stages.split("|") if pipeline else ALLOW_STATUSES

    if new_status not in allowed_statuses:
        return {"error": f"Статус '{new_status}' недопустим для воронки '{pipeline.name if pipeline else lead.pipeline_id}'"}

    if new_status == "Отказ" and loss_reason_id is None:
        return {"error": "При переводе в статус 'Отказ' необходимо указать loss_reason_id"}

    if loss_reason_id is not None:
        reason = db.query(LossReasonDB).filter(LossReasonDB.reason_id == loss_reason_id).first()
        if not reason:
            return {"error": f"Причина отказа с id {loss_reason_id} не найдена"}

    if lead:
        old_status = lead.status
        lead.status = new_status
        lead.loss_reason_id = loss_reason_id

        history = StageHistoryDB(
            phone=phone,
            old_status=old_status,
            new_status=new_status
        )
        db.add(history)
        db.commit()
        return {"message": "Статус успешно изменен"}

    return {"error": "Клиент с таким номером не найден."}


@app.delete("/leads/delete")
def delete_lead(phone: str, db: Session = Depends(get_db)):
    lead = db.query(LeadDB).filter(LeadDB.phone == phone).first()
    if lead:
        db.delete(lead)
        db.commit()
        return {"message": "Клиент удален"}
    return {"error": "Телефона нет в базе"}


@app.get("/pipeline/history")
def get_pipeline_history(db: Session = Depends(get_db)):
    history = db.query(StageHistoryDB).all()
    return history


@app.get("/analytics/pipeline-conversion")
def form_realise(db: Session = Depends(get_db)):
    total_leads = db.query(LeadDB).count()
    if total_leads == 0:
        return {"conversion_rate": 0.0, "message": "Нет лидов для расчета конверсии"}

    pipelines = {p.pipeline_id: p.stages.split("|") for p in db.query(PipelineDB).all()}
    leads = db.query(LeadDB).all()
    successful_leads = 0
    for lead in leads:
        stages = pipelines.get(lead.pipeline_id)
        if stages and lead.status == stages[-2 if stages[-1] == "Отказ" else -1]:
            successful_leads += 1

    conversion_rate = (successful_leads / total_leads) * 100
    return {
        "total_leads": total_leads,
        "successful_leads": successful_leads,
        "conversion_rate": round(conversion_rate, 2)
    }


@app.get("/pipeline/history-by-phone")
def get_history_by_phone(phone: str, db: Session = Depends(get_db)):
    history = db.query(StageHistoryDB).filter(StageHistoryDB.phone == phone).all()
    return history


@app.post("/tasks")
def create_task(task: Task, db: Session = Depends(get_db)):
    lead = db.query(LeadDB).filter(LeadDB.phone == task.phone).first()
    if not lead:
        return {"error": "Нельзя создать задачу для лида которого нет"}

    db_task = TaskDB(
        task_id=task.task_id,
        phone=task.phone,
        description=task.description,
        deadline=task.deadline,
        is_completed=task.is_completed
    )
    db.add(db_task)
    db.commit()
    return {"message": "Задача успешно добавлена", "data": task.model_dump()}


@app.get("/tasks/all-pending")
def get_all_pending_tasks(db: Session = Depends(get_db)):
    active_tasks = db.query(TaskDB).filter(TaskDB.is_completed == False).all()
    return active_tasks


@app.put("/tasks/complete")
def put_one(task_id: int, db: Session = Depends(get_db)):
    task = db.query(TaskDB).filter(TaskDB.task_id == task_id).first()
    if task:
        task.is_completed = True
        db.commit()
        return {"message": "Успех"}
    return {"error": "Задача с таким айди не найдена:("}


@app.post('/loss-reasons')
def post_loss_reason(loss: LossReason, db: Session = Depends(get_db)):
    existing_reason = db.query(LossReasonDB).filter(LossReasonDB.reason_id == loss.reason_id).first()
    if existing_reason:
        return {"error": "Причина отказа с таким id уже существует"}

    db_loss = LossReasonDB(reason_id=loss.reason_id, title=loss.title)
    db.add(db_loss)
    db.commit()
    return {"message": "Причина отказа успешно добавлена", "data": loss.model_dump()}


@app.get('/loss-reasons')
def get_loss_reasons(db: Session = Depends(get_db)):
    reasons = db.query(LossReasonDB).all()
    return reasons


# ─────────── ВОРОНКИ (PIPELINES) ───────────

@app.get('/pipelines')
def get_pipelines(db: Session = Depends(get_db)):
    pipelines = db.query(PipelineDB).all()
    return [
        {
            "pipeline_id": p.pipeline_id,
            "name": p.name,
            "client_type": p.client_type,
            "description": p.description,
            "stages": p.stages.split("|"),
        }
        for p in pipelines
    ]


@app.post('/pipelines')
def post_pipeline(pipeline: Pipeline, db: Session = Depends(get_db)):
    existing = db.query(PipelineDB).filter(PipelineDB.pipeline_id == pipeline.pipeline_id).first()
    if existing:
        return {"error": "Воронка с таким id уже существует"}

    db_pipeline = PipelineDB(
        pipeline_id=pipeline.pipeline_id,
        name=pipeline.name,
        client_type=pipeline.client_type,
        description=pipeline.description,
        stages=pipeline.stages,
    )
    db.add(db_pipeline)
    db.commit()
    return {"message": "Воронка успешно добавлена", "data": pipeline.model_dump()}


# ─────────── МЕНЕДЖЕРЫ ───────────

@app.get('/managers')
def get_managers(db: Session = Depends(get_db)):
    return db.query(ManagerDB).all()


@app.post('/managers')
def post_manager(manager: Manager, db: Session = Depends(get_db)):
    existing = db.query(ManagerDB).filter(ManagerDB.manager_id == manager.manager_id).first()
    if existing:
        return {"error": "Менеджер с таким id уже существует"}

    db_manager = ManagerDB(
        manager_id=manager.manager_id,
        name=manager.name,
        phone=_clean(manager.phone),
        email=_clean(manager.email),
    )
    db.add(db_manager)
    db.commit()
    return {"message": "Менеджер успешно добавлен", "data": manager.model_dump()}


@app.put('/leads/assign-manager')
def assign_manager(phone: str, manager_id: Optional[int] = None, db: Session = Depends(get_db)):
    lead = db.query(LeadDB).filter(LeadDB.phone == phone).first()
    if not lead:
        return {"error": "Клиент с таким номером не найден."}
    if manager_id is not None:
        manager = db.query(ManagerDB).filter(ManagerDB.manager_id == manager_id).first()
        if not manager:
            return {"error": f"Менеджер с id {manager_id} не найден"}
    lead.manager_id = manager_id
    db.commit()
    return {"message": "Менеджер назначен"}

# ─────────── ПУБЛИЧНЫЙ ЭНДПОИНТ ДЛЯ ВИДЖЕТА / САЙТА ───────────

WIDGET_TOKEN = "nxcrm_UgCTeFkTijVuLD4PFVwx1b-mGYgf7hyemBUfW4BEHcQ"

class PublicLead(BaseModel):
    name: str
    phone: str
    pipeline_id: int = 1
    client_type: str = "физ"
    telegram: Optional[str] = None
    message: Optional[str] = None  # дополнительное поле из формы


@app.post("/public/leads")
async def public_create_lead(request: Request, db: Session = Depends(get_db)):
    """
    Публичный эндпоинт — принимает заявки с сайта по API-токену.
    Авторизация через заголовок: X-CRM-Token: nxcrm_...
    Не требует Basic Auth.
    """
    token = request.headers.get("X-CRM-Token", "")
    if not secrets.compare_digest(token, WIDGET_TOKEN):
        return JSONResponse(status_code=403, content={"error": "Неверный токен"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=422, content={"error": "Неверный JSON"})

    name = (body.get("name") or "").strip()
    phone = (body.get("phone") or "").strip()
    pipeline_id = int(body.get("pipeline_id", 1))
    client_type = body.get("client_type", "физ")
    telegram = body.get("telegram") or None

    if not name or not phone:
        return JSONResponse(status_code=422, content={"error": "name и phone обязательны"})

    existing = db.query(LeadDB).filter(LeadDB.phone == phone).first()
    if existing:
        return {"message": "Лид уже существует", "phone": phone}

    pipeline = db.query(PipelineDB).filter(PipelineDB.pipeline_id == pipeline_id).first()
    status = pipeline.stages.split("|")[0] if pipeline else "Новая заявка"

    lead = LeadDB(
        name=name, phone=phone, status=status,
        pipeline_id=pipeline_id, client_type=client_type,
        telegram=telegram,
    )
    db.add(lead)
    db.commit()
    return {"message": "Заявка принята", "status": status}


# ─────────── WIDGET.JS (встраиваемый скрипт для сайта) ───────────

from fastapi.responses import Response

WIDGET_JS = r"""
(function() {
  var token = document.currentScript.getAttribute('data-token');
  var pipelineId = parseInt(document.currentScript.getAttribute('data-pipeline') || '1');
  var apiBase = document.currentScript.src.replace('/widget.js', '');

  // Inject CSS
  var style = document.createElement('style');
  style.textContent = [
    '#nxcrm-btn{position:fixed;bottom:24px;right:24px;z-index:9999;background:#6D28D9;color:#fff;border:none;border-radius:50px;padding:12px 22px;font-size:14px;font-weight:600;cursor:pointer;box-shadow:0 4px 20px rgba(109,40,217,.4);font-family:inherit;transition:all .2s;}',
    '#nxcrm-btn:hover{background:#8B5CF6;transform:translateY(-2px);}',
    '#nxcrm-overlay{display:none;position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.6);align-items:center;justify-content:center;padding:20px;}',
    '#nxcrm-overlay.open{display:flex;}',
    '#nxcrm-modal{background:#111118;border:1px solid #242430;border-radius:16px;width:100%;max-width:400px;padding:28px;font-family:inherit;}',
    '#nxcrm-modal h3{color:#F9FAFB;font-size:18px;margin:0 0 6px;}',
    '#nxcrm-modal p{color:#6B7280;font-size:13px;margin:0 0 20px;}',
    '#nxcrm-modal input{width:100%;padding:10px 14px;background:#18181F;border:1px solid #242430;border-radius:8px;color:#F9FAFB;font-size:14px;margin-bottom:12px;box-sizing:border-box;font-family:inherit;outline:none;}',
    '#nxcrm-modal input:focus{border-color:#6D28D9;}',
    '#nxcrm-modal button{width:100%;padding:11px;background:#6D28D9;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;font-family:inherit;}',
    '#nxcrm-modal button:hover{background:#8B5CF6;}',
    '#nxcrm-close{float:right;cursor:pointer;color:#6B7280;font-size:20px;border:none;background:none;line-height:1;}',
    '#nxcrm-ok{display:none;text-align:center;color:#10B981;font-size:15px;padding:10px 0;}'
  ].join('');
  document.head.appendChild(style);

  // Button
  var btn = document.createElement('button');
  btn.id = 'nxcrm-btn';
  btn.textContent = '📩 Оставить заявку';
  document.body.appendChild(btn);

  // Modal
  var overlay = document.createElement('div');
  overlay.id = 'nxcrm-overlay';
  overlay.innerHTML = '<div id="nxcrm-modal"><button id="nxcrm-close" onclick="document.getElementById(\'nxcrm-overlay\').classList.remove(\'open\')">✕</button><h3>Оставить заявку</h3><p>Мы свяжемся с вами в ближайшее время</p><div id="nxcrm-ok">✅ Заявка принята! Мы скоро свяжемся.</div><input id="nxcrm-name" type="text" placeholder="Ваше имя *" /><input id="nxcrm-phone" type="tel" placeholder="Телефон *" /><input id="nxcrm-tg" type="text" placeholder="Telegram (необязательно)" /><button onclick="nxcrmSubmit()">Отправить заявку</button></div>';
  document.body.appendChild(overlay);

  btn.onclick = function() { overlay.classList.add('open'); };
  overlay.onclick = function(e) { if (e.target === overlay) overlay.classList.remove('open'); };

  window.nxcrmSubmit = function() {
    var name = document.getElementById('nxcrm-name').value.trim();
    var phone = document.getElementById('nxcrm-phone').value.trim();
    var tg = document.getElementById('nxcrm-tg').value.trim();
    if (!name || !phone) { alert('Заполните имя и телефон'); return; }
    fetch(apiBase + '/public/leads', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-CRM-Token': token },
      body: JSON.stringify({ name: name, phone: phone, telegram: tg || null, pipeline_id: pipelineId })
    }).then(function(r) { return r.json(); }).then(function(d) {
      document.getElementById('nxcrm-ok').style.display = 'block';
      document.getElementById('nxcrm-name').style.display = 'none';
      document.getElementById('nxcrm-phone').style.display = 'none';
      document.getElementById('nxcrm-tg').style.display = 'none';
      document.querySelector('#nxcrm-modal button:last-child').style.display = 'none';
      setTimeout(function() { overlay.classList.remove('open'); }, 2500);
    }).catch(function() { alert('Ошибка отправки. Попробуйте позже.'); });
  };
})();
"""

@app.get("/widget.js", include_in_schema=False)
def serve_widget():
    """Возвращает встраиваемый JS-виджет для сайтов."""
    return Response(content=WIDGET_JS, media_type="application/javascript")
