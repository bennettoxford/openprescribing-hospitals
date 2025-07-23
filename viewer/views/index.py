from django.views.generic import TemplateView
from django.core.cache import cache
from datetime import datetime

class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        latest_posts = []

        cached_blog_data = cache.get('bennett_blog_data')
        if cached_blog_data is not None and 'all_posts' in cached_blog_data:
            latest_posts.extend([{**post, 'type': 'blog'} for post in cached_blog_data['all_posts']])
        
        cached_papers_data = cache.get('bennett_papers_data')
        if cached_papers_data is not None and 'all_papers' in cached_papers_data:
            latest_posts.extend([{**paper, 'type': 'paper'} for paper in cached_papers_data['all_papers']])


        latest_posts.sort(
            key=lambda x: datetime.strptime(x['date'], '%d %B %Y'),
            reverse=True
        )
        
        context['latest_posts'] = latest_posts[:3]
        return context
