
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import asyncio
import urllib.request
import uuid
import json
import os
import datetime

from sqlalchemy import create_engine, Column, Integer, String, JSON as SQLAlchemyJSON, DateTime, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from pywebpush import webpush, WebPushException

# --- データベース設定 ---
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./reports.db"

if DATABASE_URL.startswith("postgresql"):
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
else:
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def new_id() -> str:
    return uuid.uuid4().hex

def now() -> datetime.datetime:
    return datetime.datetime.utcnow()

# --- 報告システム Models ---
class MainReportModel(Base):
    __tablename__ = "reports"
    __table_args__ = {'extend_existing': True}
    id = Column(String, primary_key=True, index=True)
    data = Column(SQLAlchemyJSON)
    updated_at = Column(DateTime, default=now)

# --- 連絡システム Models ---
class ContactArea(Base):
    __tablename__ = "contact_areas"
    id = Column(String, primary_key=True, default=new_id)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=now)

class ContactReport(Base):
    __tablename__ = "contact_reports"
    id = Column(String, primary_key=True, default=new_id)
    area_id = Column(String, ForeignKey("contact_areas.id"), index=True)
    staff_name = Column(String, nullable=False)
    staff_token = Column(String, unique=True, index=True, default=new_id)
    message = Column(String, nullable=False)
    status = Column(String, default="open")
    acknowledged_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=now)

class ContactMessage(Base):
    __tablename__ = "contact_messages"
    id = Column(String, primary_key=True, default=new_id)
    report_id = Column(String, ForeignKey("contact_reports.id"), index=True)
    sender_role = Column(String, nullable=False)  # 'staff' or 'leader' or 'master'
    sender_name = Column(String, nullable=False)
    text = Column(String, nullable=False)
    created_at = Column(DateTime, default=now)

class ContactPushSubscription(Base):
    __tablename__ = "contact_push_subscriptions"
    id = Column(String, primary_key=True, default=new_id)
    area_id = Column(String, ForeignKey("contact_areas.id"), index=True) # None means Master
    endpoint = Column(String, unique=True, index=True)
    p256dh = Column(String, nullable=False)
    auth = Column(String, nullable=False)
    alert_repeat_enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=now)

Base.metadata.create_all(bind=engine)

# --- VAPID / Web Push 設定 ---
VAPID_PRIVATE_KEY_PATH = os.environ.get("VAPID_PRIVATE_KEY_PATH", "vapid_private_key.pem")
VAPID_CLAIM_EMAIL = os.environ.get("VAPID_CLAIM_EMAIL", "mailto:example@example.com")

def send_push_to_leaders(db, area_id: str, title: str, body: str, url: str = "/", repeat_only: bool = False):
    query = db.query(ContactPushSubscription).filter(
        (ContactPushSubscription.area_id == area_id) | (ContactPushSubscription.area_id == None)
    )
    if repeat_only:
        query = query.filter(ContactPushSubscription.alert_repeat_enabled.is_(True))
    subs = query.all()
    payload = json.dumps({"title": title, "body": body, "url": url})
    dead_ids = []
    for sub in subs:
        try:
            webpush(
                subscription_info={
                    "endpoint": sub.endpoint,
                    "keys": {"p256dh": sub.p256dh, "auth": sub.auth},
                },
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY_PATH,
                vapid_claims={"sub": VAPID_CLAIM_EMAIL},
                timeout=5,
            )
        except WebPushException as e:
            status_code = e.response.status_code if e.response is not None else None
            if status_code in (404, 410):
                dead_ids.append(sub.id)
            else:
                print(f"Push error: {e}")
        except Exception as e:
            print(f"Push send error (network): {e}")
    if dead_ids:
        db.query(ContactPushSubscription).filter(ContactPushSubscription.id.in_(dead_ids)).delete(
            synchronize_session=False
        )
        db.commit()

# --- アラートループ ---
ALERT_REPEAT_SECONDS = 12
ACTIVE_ALERT_TASKS: dict[str, asyncio.Task] = {}

async def alert_repeat_loop(report_id: str, area_id: str, area_name: str):
    try:
        while True:
            await asyncio.sleep(ALERT_REPEAT_SECONDS)
            db = SessionLocal()
            try:
                report = db.query(ContactReport).filter(ContactReport.id == report_id).first()
                if not report or report.acknowledged_at is not None:
                    return
                await asyncio.to_thread(
                    send_push_to_leaders,
                    db,
                    area_id,
                    title=f"【対応待ち】{area_name}",
                    body=f"{report.staff_name}: {report.message}",
                    url=f"/contact/leader-7x9a?report={report_id}",
                    repeat_only=True,
                )
            finally:
                db.close()
    except asyncio.CancelledError:
        pass
    finally:
        ACTIVE_ALERT_TASKS.pop(report_id, None)

def start_alert_loop(report_id: str, area_id: str, area_name: str):
    existing = ACTIVE_ALERT_TASKS.get(report_id)
    if existing and not existing.done():
        return
    ACTIVE_ALERT_TASKS[report_id] = asyncio.create_task(
        alert_repeat_loop(report_id, area_id, area_name)
    )

def stop_alert_loop(report_id: str):
    task = ACTIVE_ALERT_TASKS.pop(report_id, None)
    if task and not task.done():
        task.cancel()

# --- FastAPI設定 ---
app = FastAPI()

KEEP_ALIVE_TASK = None
async def keep_alive_loop(host_url: str):
    while True:
        try:
            await asyncio.sleep(600)
            now_jst = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
            if now_jst.hour == 0:
                global KEEP_ALIVE_TASK
                KEEP_ALIVE_TASK = None
                break
            ping_url = f"{host_url}/ping"
            req = urllib.request.Request(ping_url)
            def do_ping():
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.status
            await asyncio.to_thread(do_ping)
        except Exception:
            pass

@app.get("/ping")
async def ping():
    return {"status": "alive"}

@app.middleware("http")
async def keep_alive_middleware(request: Request, call_next):
    global KEEP_ALIVE_TASK
    if request.url.path != "/ping" and KEEP_ALIVE_TASK is None:
        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("host", request.url.netloc)
        if host:
            host_url = f"{scheme}://{host}"
            KEEP_ALIVE_TASK = asyncio.create_task(keep_alive_loop(host_url))
    response = await call_next(request)
    return response

app.mount("/static", StaticFiles(directory="static"), name="static")

# --- WebSocket 接続管理 ---
class ConnectionManager:
    def __init__(self):
        self.main_reports: list[WebSocket] = []
        self.contact_leader_rooms: dict[str, list[WebSocket]] = {}
        self.contact_staff_rooms: dict[str, list[WebSocket]] = {}
        self.contact_master_room: list[WebSocket] = []

    async def connect_main(self, ws: WebSocket):
        await ws.accept()
        self.main_reports.append(ws)
        db = SessionLocal()
        try:
            reports = db.query(MainReportModel).all()
            data_map = {r.id: r.data for r in reports}
            await ws.send_text(json.dumps({"type": "init", "data": data_map}))
        finally:
            db.close()

    def disconnect_main(self, ws: WebSocket):
        if ws in self.main_reports:
            self.main_reports.remove(ws)

    async def broadcast_main(self, message: str):
        dead = []
        for ws in self.main_reports:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect_main(ws)

    async def connect_leader(self, area_id: str, ws: WebSocket):
        await ws.accept()
        self.contact_leader_rooms.setdefault(area_id, []).append(ws)

    def disconnect_leader(self, area_id: str, ws: WebSocket):
        if area_id in self.contact_leader_rooms and ws in self.contact_leader_rooms[area_id]:
            self.contact_leader_rooms[area_id].remove(ws)

    async def connect_staff(self, report_id: str, ws: WebSocket):
        await ws.accept()
        self.contact_staff_rooms.setdefault(report_id, []).append(ws)

    def disconnect_staff(self, report_id: str, ws: WebSocket):
        if report_id in self.contact_staff_rooms and ws in self.contact_staff_rooms[report_id]:
            self.contact_staff_rooms[report_id].remove(ws)

    async def connect_master(self, ws: WebSocket):
        await ws.accept()
        self.contact_master_room.append(ws)

    def disconnect_master(self, ws: WebSocket):
        if ws in self.contact_master_room:
            self.contact_master_room.remove(ws)

    async def broadcast_contact(self, area_id: str, report_id: str, message: dict):
        dead = []
        targets = []
        if area_id:
            targets.extend(self.contact_leader_rooms.get(area_id, []))
        if report_id:
            targets.extend(self.contact_staff_rooms.get(report_id, []))
        targets.extend(self.contact_master_room)
        targets = list(set(targets))
        
        msg_str = json.dumps(message)
        for ws in targets:
            try:
                await ws.send_text(msg_str)
            except Exception:
                dead.append(ws)
        
        for ws in dead:
            if area_id: self.disconnect_leader(area_id, ws)
            if report_id: self.disconnect_staff(report_id, ws)
            self.disconnect_master(ws)

manager = ConnectionManager()

# --- ページ配信 (報告システム) ---
@app.get("/")
async def get_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/report")
async def get_report():
    with open("static/report.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# --- ページ配信 (連絡システム) ---
@app.get("/contact/sw.js")
async def get_contact_sw():
    with open("static/contact/sw.js", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read(), media_type="application/javascript")

@app.get("/contact/staff")
async def get_contact_staff():
    with open("static/contact/staff.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/contact/leader-7x9a")
async def get_contact_leader():
    with open("static/contact/leader.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/contact/master-8b2y")
async def get_contact_master():
    with open("static/contact/master.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# --- 報告システム WebSocket ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect_main(websocket)
    try:
        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            report_id = str(data.get("id"))
            db = SessionLocal()
            try:
                report_obj = db.query(MainReportModel).filter(MainReportModel.id == report_id).first()
                if report_obj:
                    report_obj.data = data
                    report_obj.updated_at = datetime.datetime.utcnow()
                else:
                    report_obj = MainReportModel(id=report_id, data=data)
                    db.add(report_obj)
                db.commit()
            except Exception as e:
                print(f"DB Error: {e}")
            finally:
                db.close()
            await manager.broadcast_main(data_str)
    except WebSocketDisconnect:
        manager.disconnect_main(websocket)

# --- 連絡システム Pydantic ---
class CreateAreaRequest(BaseModel):
    name: str

class CreateReportRequest(BaseModel):
    area_id: str
    staff_name: str
    message: str

class AddMessageRequest(BaseModel):
    text: str

class LeaderReplyRequest(BaseModel):
    sender_name: str
    text: str

class SubscribeRequest(BaseModel):
    area_id: str = None
    endpoint: str
    keys: dict
    alert_repeat_enabled: bool = True

# --- 連絡システム API ---
@app.get("/api/contact/areas")
async def list_contact_areas():
    db = SessionLocal()
    try:
        areas = db.query(ContactArea).order_by(ContactArea.created_at.desc()).all()
        return [{"id": a.id, "name": a.name} for a in areas]
    finally:
        db.close()

@app.post("/api/contact/areas")
async def create_contact_area(req: CreateAreaRequest):
    db = SessionLocal()
    try:
        area = ContactArea(name=req.name)
        db.add(area)
        db.commit()
        db.refresh(area)
        return {"id": area.id, "name": area.name}
    finally:
        db.close()

@app.get("/api/contact/reports")
async def list_contact_reports(area_id: str = None):
    db = SessionLocal()
    try:
        q = db.query(ContactReport).order_by(ContactReport.created_at.desc())
        if area_id:
            q = q.filter(ContactReport.area_id == area_id)
        reports = q.all()
        
        areas = {a.id: a.name for a in db.query(ContactArea).all()}
        
        return [
            {
                "id": r.id,
                "area_id": r.area_id,
                "area_name": areas.get(r.area_id, "不明"),
                "staff_name": r.staff_name,
                "message": r.message,
                "status": r.status,
                "acknowledged_at": r.acknowledged_at.isoformat() if r.acknowledged_at else None,
                "created_at": r.created_at.isoformat(),
            }
            for r in reports
        ]
    finally:
        db.close()

def get_messages_payload(db, report_id: str):
    msgs = db.query(ContactMessage).filter(ContactMessage.report_id == report_id).order_by(ContactMessage.created_at.asc()).all()
    return [
        {
            "id": m.id,
            "sender_role": m.sender_role,
            "sender_name": m.sender_name,
            "text": m.text,
            "created_at": m.created_at.isoformat(),
        }
        for m in msgs
    ]

@app.get("/api/contact/reports/{report_id}")
async def get_contact_report_thread(report_id: str, token: str = None):
    db = SessionLocal()
    try:
        report = db.query(ContactReport).filter(ContactReport.id == report_id).first()
        if not report:
            raise HTTPException(status_code=404, detail="報告が見つかりません")
        # リーダーやマスターは token なしで閲覧できる
        if token and report.staff_token != token:
            raise HTTPException(status_code=403, detail="権限がありません")
            
        area = db.query(ContactArea).filter(ContactArea.id == report.area_id).first()
        return {
            "id": report.id,
            "area_id": report.area_id,
            "area_name": area.name if area else "不明",
            "staff_name": report.staff_name,
            "message": report.message,
            "status": report.status,
            "acknowledged_at": report.acknowledged_at.isoformat() if report.acknowledged_at else None,
            "created_at": report.created_at.isoformat(),
            "messages": get_messages_payload(db, report.id),
        }
    finally:
        db.close()

@app.post("/api/contact/staff/reports")
async def staff_create_report(req: CreateReportRequest):
    db = SessionLocal()
    try:
        area = db.query(ContactArea).filter(ContactArea.id == req.area_id).first()
        if not area:
            raise HTTPException(status_code=404, detail="エリアが見つかりません")

        report = ContactReport(area_id=area.id, staff_name=req.staff_name, message=req.message)
        db.add(report)
        db.commit()
        db.refresh(report)

        payload = {
            "type": "new_report",
            "report_id": report.id,
            "area_id": area.id
        }
        await manager.broadcast_contact(area.id, None, payload)
        
        await asyncio.to_thread(
            send_push_to_leaders,
            db,
            area.id,
            title=f"新しい報告（{area.name}）",
            body=f"{report.staff_name}: {report.message}",
            url=f"/contact/leader-7x9a?report={report.id}&area={area.id}",
        )
        start_alert_loop(report.id, area.id, area.name)

        return {"report_id": report.id, "staff_token": report.staff_token}
    finally:
        db.close()

@app.post("/api/contact/staff/reports/{report_id}/messages")
async def add_staff_message(report_id: str, token: str, req: AddMessageRequest):
    db = SessionLocal()
    try:
        report = db.query(ContactReport).filter(ContactReport.id == report_id).first()
        if not report or report.staff_token != token:
            raise HTTPException(status_code=404, detail="報告が見つかりません")
        area = db.query(ContactArea).filter(ContactArea.id == report.area_id).first()

        msg = ContactMessage(
            report_id=report.id,
            sender_role="staff",
            sender_name=report.staff_name,
            text=req.text,
        )
        db.add(msg)
        
        payload = {
            "type": "message",
            "report_id": report.id,
            "area_id": report.area_id
        }
        await manager.broadcast_contact(report.area_id, report.id, payload)
        
        await asyncio.to_thread(
            send_push_to_leaders,
            db,
            area.id,
            title=f"追記あり（{area.name}）",
            body=f"{report.staff_name}: {req.text}",
            url=f"/contact/leader-7x9a?report={report.id}&area={area.id}",
        )
        report.acknowledged_at = None
        db.commit()
        start_alert_loop(report.id, area.id, area.name)
        return {"ok": True}
    finally:
        db.close()

@app.post("/api/contact/reports/{report_id}/acknowledge")
async def acknowledge_contact_report(report_id: str):
    db = SessionLocal()
    try:
        report = db.query(ContactReport).filter(ContactReport.id == report_id).first()
        if not report:
            raise HTTPException(status_code=404, detail="報告が見つかりません")
        report.acknowledged_at = now()
        db.commit()
        stop_alert_loop(report.id)
        
        payload = {"type": "acknowledged", "report_id": report.id, "area_id": report.area_id}
        await manager.broadcast_contact(report.area_id, report.id, payload)
        return {"ok": True}
    finally:
        db.close()

@app.post("/api/contact/reports/{report_id}/reply")
async def leader_master_reply(report_id: str, req: LeaderReplyRequest):
    db = SessionLocal()
    try:
        report = db.query(ContactReport).filter(ContactReport.id == report_id).first()
        if not report:
            raise HTTPException(status_code=404, detail="報告が見つかりません")

        msg = ContactMessage(
            report_id=report.id,
            sender_role="leader",
            sender_name=req.sender_name,
            text=req.text,
        )
        db.add(msg)
        
        if report.acknowledged_at is None:
            report.acknowledged_at = now()
            stop_alert_loop(report.id)
            
        db.commit()
        
        payload = {"type": "message", "report_id": report.id, "area_id": report.area_id}
        await manager.broadcast_contact(report.area_id, report.id, payload)
        return {"ok": True}
    finally:
        db.close()

@app.post("/api/contact/subscribe")
async def contact_subscribe(req: SubscribeRequest):
    db = SessionLocal()
    try:
        existing = db.query(ContactPushSubscription).filter(ContactPushSubscription.endpoint == req.endpoint).first()
        if existing:
            existing.area_id = req.area_id
            existing.p256dh = req.keys.get("p256dh", "")
            existing.auth = req.keys.get("auth", "")
            existing.alert_repeat_enabled = req.alert_repeat_enabled
        else:
            sub = ContactPushSubscription(
                area_id=req.area_id,
                endpoint=req.endpoint,
                p256dh=req.keys.get("p256dh", ""),
                auth=req.keys.get("auth", ""),
                alert_repeat_enabled=req.alert_repeat_enabled,
            )
            db.add(sub)
        db.commit()
        return {"ok": True}
    finally:
        db.close()

# --- 連絡システム WebSocket ---
@app.websocket("/ws/contact/staff/{report_id}")
async def ws_contact_staff(websocket: WebSocket, report_id: str, token: str):
    db = SessionLocal()
    try:
        report = db.query(ContactReport).filter(ContactReport.id == report_id).first()
        if not report or report.staff_token != token:
            await websocket.close(code=4404)
            return
    finally:
        db.close()
    await manager.connect_staff(report_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_staff(report_id, websocket)

@app.websocket("/ws/contact/leader/{area_id}")
async def ws_contact_leader(websocket: WebSocket, area_id: str):
    await manager.connect_leader(area_id, websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_leader(area_id, websocket)

@app.websocket("/ws/contact/master")
async def ws_contact_master(websocket: WebSocket):
    await manager.connect_master(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_master(websocket)
