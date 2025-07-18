from django.views.generic import TemplateView
from django.utils.safestring import mark_safe
from django.core.serializers.json import DjangoJSONEncoder
import json

class ProductDetailsView(TemplateView):
    template_name = "product_details.html"
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update({
            'products': mark_safe(json.dumps([], cls=DjangoJSONEncoder)),
            'search_term': ''
        })

        return context

