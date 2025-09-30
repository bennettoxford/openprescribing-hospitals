from django.views.generic import TemplateView
import json

from ..mixins import MaintenanceModeMixin
from ..utils import get_organisation_data


class AnalyseView(MaintenanceModeMixin, TemplateView):
    template_name = "analyse.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        org_data = get_organisation_data()

        context['org_data'] = json.dumps({
            'orgs': org_data['orgs'],
            'org_codes': org_data['org_codes'],
            'predecessorMap': org_data['predecessor_map'],
        }, default=str)

        return context