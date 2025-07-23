from django.template.response import TemplateResponse

def error_handler(request, error_code, error_name, error_message, status_code, exception=None):
    return TemplateResponse(
        request,
        "error.html",
        status=status_code,
        context={
            "error_code": error_code,
            "error_name": error_name,
            "error_message": error_message,
        },
    )

def bad_request(request, exception=None):
    return error_handler(
        request, "400", "Bad request", 
        "Your request could not be processed due to invalid or malformed data.", 
        400, exception
    )

def csrf_failure(request, reason=""):
    return error_handler(
        request, "CSRF", "Security verification failed",
        "Your request could not be completed due to a security verification failure. Please refresh the page and try again.",
        403
    )

def page_not_found(request, exception=None):
    return error_handler(
        request, "404", "Page not found",
        "Sorry, the page you're looking for doesn't exist or has been moved.",
        404, exception
    )

def permission_denied(request, exception=None):
    return error_handler(
        request, "403", "Access forbidden",
        "You don't have permission to access this resource.",
        403, exception
    )

def server_error(request):
    return error_handler(
        request, "500", "Something went wrong",
        "We're experiencing a technical issue.",
        500
    )
