from typing import Optional
from pydantic import BaseModel


class SleepPhases(BaseModel):
    deep_minutes: Optional[int] = None
    light_minutes: Optional[int] = None
    rem_minutes: Optional[int] = None
    awake_minutes: Optional[int] = None
    nap_minutes: Optional[int] = None    # shortSleepTime — daytime naps


class SleepRecord(BaseModel):
    date: str
    total_duration_minutes: Optional[int] = None
    phases: Optional[SleepPhases] = None
    avg_hr: Optional[int] = None
    min_hr: Optional[int] = None
    max_hr: Optional[int] = None
    quality_score: Optional[int] = None  # -1 = not computed


class HRVRecord(BaseModel):
    date: str
    avg_sleep_hrv: Optional[float] = None    # Nacht-Durchschnitt RMSSD (ms)
    baseline: Optional[float] = None          # sleepHrvBase — rolling baseline
    standard_deviation: Optional[float] = None  # sleepHrvSd
    interval_list: Optional[list[int]] = None   # sleepHrvIntervalList — percentile bands


class DailyRecord(BaseModel):
    date: str
    avg_sleep_hrv: Optional[float] = None
    baseline: Optional[float] = None
    interval_list: Optional[list[int]] = None
    rhr: Optional[int] = None                      # resting heart rate (bpm)
    training_load: Optional[int] = None
    training_load_ratio: Optional[float] = None    # acute/chronic ratio
    tired_rate: Optional[float] = None
    ati: Optional[float] = None                    # acute training index
    cti: Optional[float] = None                    # chronic training index
    performance: Optional[int] = None              # performance index (-1 = no data)
    distance: Optional[float] = None               # daily distance (m)
    duration: Optional[int] = None                 # daily duration (s)
    vo2max: Optional[int] = None                   # only from /analyse/query
    lthr: Optional[int] = None                     # lactate threshold HR (bpm)
    ltsp: Optional[int] = None                     # lactate threshold pace (s/km)
    stamina_level: Optional[float] = None          # base fitness
    stamina_level_7d: Optional[float] = None       # 7-day fitness trend


class ActivitySummary(BaseModel):
    activity_id: str
    name: Optional[str] = None
    sport_type: Optional[int] = None
    sport_name: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: Optional[int] = None
    distance_meters: Optional[float] = None
    avg_hr: Optional[int] = None
    max_hr: Optional[int] = None
    calories: Optional[int] = None
    training_load: Optional[int] = None
    avg_power: Optional[int] = None
    normalized_power: Optional[int] = None
    elevation_gain: Optional[int] = None


class StoredAuth(BaseModel):
    access_token: str
    user_id: str
    region: str
    timestamp: int  # Unix milliseconds
    mobile_access_token: Optional[str] = None   # token for apieu.coros.com (sleep data)
    mobile_login_payload: Optional[dict] = None  # encrypted login body for auto-refresh
