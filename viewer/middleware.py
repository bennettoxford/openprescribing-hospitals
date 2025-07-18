from django.http import HttpResponse
from django.template.loader import render_to_string
from pipeline.utils.maintenance import is_maintenance_mode
import logging

logger = logging.getLogger(__name__)

class MaintenanceModeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if (request.user.is_authenticated and request.user.is_staff) or \
           request.path.startswith('/admin/') or \
           request.path.startswith('/static/') or \
           request.path.startswith('/media/'):
            return self.get_response(request)

        if is_maintenance_mode():
            restricted_paths = [
                '/analyse/',
                '/measures/',
                '/submission-history/',
                '/product-lookup/',
            ]
            
            is_restricted = any(
                request.path == path or request.path.startswith(path)
                for path in restricted_paths
            )
            
            if is_restricted:
                return self.maintenance_response(request)

        return self.get_response(request)

    def maintenance_response(self, request):
        """Return maintenance mode response for restricted pages"""
        try:
            html_content = render_to_string('maintenance.html', {}, request=request)
            return HttpResponse(
                html_content,
                status=503,
                content_type='text/html'
            )
        except Exception as e:
            logger.error(f"Failed to render maintenance template: {str(e)}", exc_info=True)
            
            return HttpResponse(
                "<h1>System Maintenance</h1>"
                "<p>We're currently updating our data. Please check back shortly.</p>",
                status=503,
                content_type='text/html'
            )
