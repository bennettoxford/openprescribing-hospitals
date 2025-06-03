import logging
from django.core.cache import cache
from datetime import datetime

logger = logging.getLogger(__name__)

MAINTENANCE_MODE_KEY = "system_maintenance_mode"
MAINTENANCE_START_KEY = "maintenance_mode_start_time"

def enable_maintenance_mode():
    try:
        cache.set(MAINTENANCE_MODE_KEY, True)
        cache.set(MAINTENANCE_START_KEY, datetime.now().isoformat())
        logger.info("Maintenance mode enabled")
        return True
    except Exception as e:
        logger.error(f"Failed to enable maintenance mode: {e}")
        return False

def disable_maintenance_mode():
    """Disable maintenance mode"""
    try:
        cache.delete(MAINTENANCE_MODE_KEY)
        cache.delete(MAINTENANCE_START_KEY)
        logger.info("Maintenance mode disabled successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to disable maintenance mode: {e}")
        return False

def is_maintenance_mode():
    """Check if maintenance mode is currently enabled"""
    return cache.get(MAINTENANCE_MODE_KEY, False)

def get_maintenance_status():
    """Get detailed maintenance mode status"""
    is_enabled = cache.get(MAINTENANCE_MODE_KEY, False)
    start_time_str = cache.get(MAINTENANCE_START_KEY)
    
    status = {
        "enabled": is_enabled
    }
    
    if start_time_str:
        try:
            start_time = datetime.fromisoformat(start_time_str)
            status["started_at"] = start_time_str
            status["duration"] = str(datetime.now() - start_time)
        except ValueError:
            pass
    
    return status 