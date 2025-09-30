from django.http import HttpResponse
from django.template.loader import render_to_string
from pipeline.utils.maintenance import is_maintenance_mode
import logging

logger = logging.getLogger(__name__)


class MaintenanceModeMixin:
    """
    Mixin to protect views during maintenance mode.
    """
    def dispatch(self, request, *args, **kwargs):
        if is_maintenance_mode():
            if not (request.user.is_authenticated and request.user.is_staff):
                return self._maintenance_response(request)
        
        return super().dispatch(request, *args, **kwargs)
    
    def _maintenance_response(self, request):
        """Return maintenance mode response"""
        try:
            html_content = render_to_string('maintenance.html', {}, request=request)
            return HttpResponse(
                html_content,
                status=503,
                content_type='text/html'
            )
        except Exception as e:
            logger.error(f"Failed to render maintenance template: {e}", exc_info=True)
            
            return HttpResponse(
                "<h1>System Maintenance</h1>"
                "<p>We're currently updating our data. Please check back shortly.</p>",
                status=503,
                content_type='text/html'
            )
