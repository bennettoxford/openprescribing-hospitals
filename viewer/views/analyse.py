from django.views.generic import TemplateView
import json

from ..models import Organisation

class AnalyseView(TemplateView):
    template_name = "analyse.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
      
        all_orgs = Organisation.objects.all().order_by('ods_name')
        predecessor_map = {}
        org_list = []

        for org in all_orgs:
            org_name = org.ods_name
            org_list.append(org_name)
            
            if org.successor:
                successor_name = org.successor.ods_name
                if successor_name not in predecessor_map:
                    predecessor_map[successor_name] = []
                predecessor_map[successor_name].append(org_name)

        context['org_data'] = json.dumps({
            'items': org_list,
            'predecessorMap': predecessor_map
        }, default=str)
        
        return context