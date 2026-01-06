import json
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, String, Float, DateTime, Boolean, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from kafka import KafkaConsumer
import threading

# ============ LOGGING SETUP ============
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============ DATABASE SETUP ============
DATABASE_URL = "postgresql://postgres:password@monitoring_postgres:5432/postgres"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ============ DATABASE MODELS ============
class PanelStatus(Base):
    __tablename__ = "panel_status"
    
    id = Column(Integer, primary_key=True, index=True)
    panel_id = Column(String, unique=True, index=True, nullable=False)
    is_broken = Column(Boolean, default=False)
    avg_efficiency = Column(Float, nullable=True)
    avg_temperature = Column(Float, nullable=True)
    avg_irradiance = Column(Float, nullable=True)
    last_updated = Column(DateTime, default=datetime.utcnow)
    broken_since = Column(DateTime, nullable=True)
    reason = Column(String, nullable=True)

# Create tables
Base.metadata.create_all(bind=engine)

# ============ PYDANTIC MODELS ============
class PanelStatusResponse(BaseModel):
    panel_id: str
    is_broken: bool
    avg_efficiency: Optional[float]
    avg_temperature: Optional[float]
    avg_irradiance: Optional[float]
    last_updated: datetime
    broken_since: Optional[datetime]
    reason: Optional[str]
    
    class Config:
        from_attributes = True

# ============ KAFKA CONSUMER ============
class KafkaConsumerThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
        self.consumer = None
        
    def run(self):
        """Consume messages from Kafka and process them"""
        try:
            self.consumer = KafkaConsumer(
                'kafka_solar_window_stats',
                bootstrap_servers=['kafka:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='monitoring_group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_records=100
            )
            
            logger.info("Kafka consumer started successfully")
            
            while self.running:
                try:
                    messages = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, records in messages.items():
                        for message in records:
                            self.process_message(message.value)
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
            
    def process_message(self, data: dict):
        """Process incoming Kafka message and detect broken panels"""
        try:
            db = SessionLocal()
            
            panel_id = data.get('panel_id')
            avg_efficiency = data.get('avg_efficiency')
            avg_temp = data.get('avg_temp')
            avg_irradiance = data.get('avg_irradiance')
            
            if not panel_id:
                return
            
            # Check if panel exists in database
            panel = db.query(PanelStatus).filter(PanelStatus.panel_id == panel_id).first()
            
            if not panel:
                panel = PanelStatus(panel_id=panel_id)
                db.add(panel)
            
            # Update panel data
            panel.avg_efficiency = avg_efficiency
            panel.avg_temperature = avg_temp
            panel.avg_irradiance = avg_irradiance
            panel.last_updated = datetime.utcnow()
            
            # ============ BROKEN PANEL DETECTION LOGIC ============
            is_broken = False
            reason = None
            
            # Detection Rule 1: Very low or zero efficiency
            if avg_efficiency is not None and avg_efficiency < 5.0:
                is_broken = True
                reason = f"Low efficiency: {avg_efficiency:.2f}%"
            
            # Detection Rule 2: Zero or very low irradiance with no power output
            # (but this could also mean night time, so we combine with efficiency)
            elif avg_irradiance is not None and avg_irradiance > 100 and avg_efficiency is not None and avg_efficiency < 10:
                is_broken = True
                reason = f"Low efficiency ({avg_efficiency:.2f}%) despite adequate irradiance ({avg_irradiance:.0f} W/m²)"
            
            # Detection Rule 3: Temperature anomaly (too high)
            elif avg_temp is not None and avg_temp > 85:
                is_broken = True
                reason = f"Temperature anomaly: {avg_temp:.1f}°C"
            
            # Detection Rule 4: Consistent low irradiance detection (broken sensor or covered panel)
            elif avg_irradiance is not None and avg_irradiance < 10 and avg_temp is not None and avg_temp > 25:
                # Panel is hot but getting no sun - likely broken
                is_broken = True
                reason = f"No irradiance detected despite warm temperature ({avg_temp:.1f}°C)"
            
            # Update broken status
            if is_broken and not panel.is_broken:
                panel.is_broken = True
                panel.broken_since = datetime.utcnow()
                panel.reason = reason
                logger.warning(f"Panel {panel_id} marked as broken: {reason}")
                
            elif not is_broken and panel.is_broken:
                # Panel recovered
                panel.is_broken = False
                panel.broken_since = None
                panel.reason = None
                logger.info(f"Panel {panel_id} recovered")
            
            db.commit()
            
        except Exception as e:
            logger.error(f"Error processing panel {panel_id}: {e}")
            db.rollback()
        finally:
            db.close()
    
    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()

# ============ GLOBAL KAFKA CONSUMER ============
kafka_consumer_thread: Optional[KafkaConsumerThread] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown event handler"""
    global kafka_consumer_thread
    
    # Startup
    logger.info("Starting monitoring service...")
    kafka_consumer_thread = KafkaConsumerThread()
    kafka_consumer_thread.start()
    
    yield
    
    # Shutdown
    logger.info("Stopping monitoring service...")
    if kafka_consumer_thread:
        kafka_consumer_thread.stop()

# ============ FASTAPI APP ============
app = FastAPI(title="Solar Panel Monitoring", description="Monitor and detect broken solar panels", lifespan=lifespan)

# ============ API ENDPOINTS ============

@app.get("/api/panels", response_model=List[PanelStatusResponse])
async def get_all_panels(skip: int = 0, limit: int = 100):
    """Get all panels with their status"""
    db = SessionLocal()
    try:
        panels = db.query(PanelStatus).offset(skip).limit(limit).all()
        return panels
    finally:
        db.close()

@app.get("/api/panels/{panel_id}", response_model=PanelStatusResponse)
async def get_panel(panel_id: str):
    """Get specific panel details"""
    db = SessionLocal()
    try:
        panel = db.query(PanelStatus).filter(PanelStatus.panel_id == panel_id).first()
        if not panel:
            raise HTTPException(status_code=404, detail="Panel not found")
        return panel
    finally:
        db.close()

@app.get("/api/broken-panels", response_model=List[PanelStatusResponse])
async def get_broken_panels():
    """Get all broken panels"""
    db = SessionLocal()
    try:
        panels = db.query(PanelStatus).filter(PanelStatus.is_broken == True).all()
        return panels
    finally:
        db.close()

@app.get("/api/statistics")
async def get_statistics():
    """Get overall statistics"""
    db = SessionLocal()
    try:
        total_panels = db.query(PanelStatus).count()
        broken_panels = db.query(PanelStatus).filter(PanelStatus.is_broken == True).count()
        working_panels = total_panels - broken_panels
        
        avg_efficiency = None
        avg_temp = None
        
        panels = db.query(PanelStatus).all()
        if panels:
            efficiencies = [p.avg_efficiency for p in panels if p.avg_efficiency is not None]
            temps = [p.avg_temperature for p in panels if p.avg_temperature is not None]
            
            if efficiencies:
                avg_efficiency = sum(efficiencies) / len(efficiencies)
            if temps:
                avg_temp = sum(temps) / len(temps)
        
        return {
            "total_panels": total_panels,
            "broken_panels": broken_panels,
            "working_panels": working_panels,
            "average_efficiency": avg_efficiency,
            "average_temperature": avg_temp
        }
    finally:
        db.close()

@app.get("/")
async def serve_dashboard():
    """Serve the main dashboard"""
    return FileResponse("monitoring/dashboard.html", media_type="text/html")

logger.info("Monitoring FastAPI app initialized")