from django.views.generic import TemplateView
from django.utils.safestring import mark_safe
from django.core.serializers.json import DjangoJSONEncoder
import json

from ..mixins import MaintenanceModeMixin


class ProductLookupView(MaintenanceModeMixin, TemplateView):
    template_name = "product_lookup.html"
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update({
            'products': mark_safe(json.dumps([], cls=DjangoJSONEncoder)),
            'search_term': ''
        })

        return context

