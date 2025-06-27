import logging
from django.utils import timezone

logger = logging.getLogger(__name__)

def enable_maintenance_mode():
    """Enable maintenance mode"""
    try:
        from viewer.models import SystemMaintenance
        
        maintenance = SystemMaintenance.get_instance()
        maintenance.enabled = True
        maintenance.started_at = timezone.now()
        maintenance.save()
        
        logger.info("Maintenance mode enabled")
        return True
    except Exception as e:
        logger.error(f"Failed to enable maintenance mode: {e}")
        return False

def disable_maintenance_mode():
    """Disable maintenance mode"""
    try:
        from viewer.models import SystemMaintenance
        
        maintenance = SystemMaintenance.get_instance()
        maintenance.enabled = False
        maintenance.started_at = None
        maintenance.save()
        
        logger.info("Maintenance mode disabled successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to disable maintenance mode: {e}")
        return False

def is_maintenance_mode():
    """Check if maintenance mode is currently enabled"""
    try:
        from viewer.models import SystemMaintenance
        maintenance = SystemMaintenance.get_instance()
        return maintenance.enabled
    except Exception as e:
        logger.error(f"Error checking maintenance mode: {e}")
        return False

def get_maintenance_status():
    """Get detailed maintenance mode status"""
    try:
        from viewer.models import SystemMaintenance
        maintenance = SystemMaintenance.get_instance()
        
        status = {
            "enabled": maintenance.enabled
        }
        
        if maintenance.enabled and maintenance.started_at:
            status["started_at"] = maintenance.started_at.isoformat()
            
            duration = maintenance.get_duration()
            if duration:
                status["duration"] = str(duration)
        
        return status
    except Exception as e:
        logger.error(f"Error getting maintenance status: {e}")
        return {"enabled": False, "error": str(e)} 