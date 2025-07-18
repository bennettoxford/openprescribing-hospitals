from django.views.generic import TemplateView
from django.core.cache import cache

class PapersListView(TemplateView):
    template_name = "papers_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        cached_data = cache.get('bennett_papers_data')
        if cached_data is not None:
            context.update(cached_data)
            return context
        
        context.update({
            'all_papers': [],
            'papers_by_status': {},
            'error': 'No cached data available. Please run refresh_content_cache management command.'
        })
        return context