from typing import Optional
from pydantic import BaseModel


class SleepPhases(BaseModel):
    deep_minutes: Optional[int] = None
    light_minutes: Optional[int] = None
    rem_minutes: Optional[int] = None
    awake_minutes: Optional[int] = None


class SleepRecord(BaseModel):
    date: str
    total_duration_minutes: Optional[int] = None
    phases: Optional[SleepPhases] = None
    sleep_start: Optional[str] = None
    sleep_end: Optional[str] = None
    quality_score: Optional[int] = None


class HRVRecord(BaseModel):
    date: str
    avg_sleep_hrv: Optional[float] = None    # Nacht-Durchschnitt RMSSD (ms)
    baseline: Optional[float] = None          # sleepHrvBase — rolling baseline
    standard_deviation: Optional[float] = None  # sleepHrvSd
    interval_list: Optional[list[int]] = None   # sleepHrvIntervalList — percentile bands


class StoredAuth(BaseModel):
    access_token: str
    user_id: str
    region: str
    timestamp: int  # Unix milliseconds
