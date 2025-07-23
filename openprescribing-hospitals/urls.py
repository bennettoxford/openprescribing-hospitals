from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from viewer.views import bad_request, page_not_found, permission_denied, server_error

urlpatterns = [
    path("admin/", admin.site.urls),
    path("__reload__/", include("django_browser_reload.urls")),
    path("", include("viewer.urls", namespace="viewer")),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

handler400 = bad_request
handler403 = permission_denied
handler404 = page_not_found
handler500 = server_error
