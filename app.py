from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import asyncio
import urllib.request
from sqlalchemy import create_engine, Column, Integer, String, JSON as SQLAlchemyJSON, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json
import os
import datetime

# --- データベース設定 ---
# Renderなどの環境変数 DATABASE_URL を優先、なければローカルの SQLite を使用
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    # SQLAlchemy互換のために postgres:// を postgresql:// に置換
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./reports.db"

if DATABASE_URL and DATABASE_URL.startswith("postgresql"):
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
    )
else:
    engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ReportModel(Base):
    __tablename__ = "reports"
    id = Column(String, primary_key=True, index=True) # 以前の Date.now() 互換のため String
    data = Column(SQLAlchemyJSON) # 報告データ全体をJSONとして保存
    updated_at = Column(DateTime, default=datetime.datetime.utcnow)

Base.metadata.create_all(bind=engine)

# --- FastAPI設定 ---
app = FastAPI()

KEEP_ALIVE_TASK = None

async def keep_alive_loop(host_url: str):
    print(f"Starting keep-alive loop for {host_url}")
    while True:
        try:
            # 10分待機 (600秒)
            await asyncio.sleep(600)
            
            # 日本時間(JST)の現在時刻を取得
            now_jst = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
            
            # 0時台（0:00〜0:59）ならループを終了してスリープを許可する
            if now_jst.hour == 0:
                print("深夜0時を回ったため、スリープ防止ループを停止します。")
                global KEEP_ALIVE_TASK
                KEEP_ALIVE_TASK = None
                break
                
            # 自分自身にアクセス (Ping)
            ping_url = f"{host_url}/ping"
            req = urllib.request.Request(ping_url)
            with urllib.request.urlopen(req) as response:
                print(f"Self-ping sent to {ping_url}. Status: {response.status}")
                
        except Exception as e:
            print(f"Keep-alive error: {e}")

@app.get("/ping")
async def ping():
    return {"status": "alive"}

@app.middleware("http")
async def keep_alive_middleware(request: Request, call_next):
    global KEEP_ALIVE_TASK
    # /ping への定期リクエスト自体ではループを起動しないようにする
    if request.url.path != "/ping" and KEEP_ALIVE_TASK is None:
        # Render等のリバースプロキシ環境を考慮して public URL を構築
        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("host", request.url.netloc)
        if host:
            host_url = f"{scheme}://{host}"
            KEEP_ALIVE_TASK = asyncio.create_task(keep_alive_loop(host_url))
            
    response = await call_next(request)
    return response

# 静的ファイルの配信設定
app.mount("/static", StaticFiles(directory="static"), name="static")

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        
        # 接続時にDBから全データを取得して送信
        db = SessionLocal()
        try:
            reports = db.query(ReportModel).all()
            data_map = {r.id: r.data for r in reports}
            await websocket.send_text(json.dumps({
                "type": "init",
                "data": data_map
            }))
        finally:
            db.close()
        
        print(f"Client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        print(f"Client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        print(f"Broadcasting: {message}")
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                print(f"Error sending to client: {e}")
                dead.append(connection)
        for connection in dead:
            self.active_connections.remove(connection)

manager = ConnectionManager()

@app.get("/")
async def get():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.get("/report")
async def get_report():
    with open("static/report.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            
            # 受信したデータをDBに保存
            report_id = str(data.get("id"))
            db = SessionLocal()
            try:
                report_obj = db.query(ReportModel).filter(ReportModel.id == report_id).first()
                if report_obj:
                    report_obj.data = data
                    report_obj.updated_at = datetime.datetime.utcnow()
                else:
                    report_obj = ReportModel(id=report_id, data=data)
                    db.add(report_obj)
                db.commit()
            except Exception as e:
                print(f"DB Error: {e}")
            finally:
                db.close()
            
            # 受信したデータを全員にブロードキャスト
            await manager.broadcast(data_str)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"WebSocket Error: {e}")
        manager.disconnect(websocket)
