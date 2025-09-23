from django.views.generic import TemplateView

class AlertsView(TemplateView):
    template_name = "alerts.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context
