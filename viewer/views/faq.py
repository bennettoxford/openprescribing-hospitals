from django.views.generic import TemplateView
import re
from markdown2 import Markdown
from django.utils.text import slugify
import os

class FAQView(TemplateView):
    template_name = "faq.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        def internal_link_preprocessor(text):
            """Convert internal links like 'about/#section' to full URLs"""
            def replace_link(match):
                link_text = match.group(1)
                internal_path = match.group(2)
                
                if not internal_path.startswith('/'):
                    internal_path = f'/{internal_path}'
                    
                return f'[{link_text}]({internal_path})'

            pattern = r'\[(.*?)\]\(((?!http|mailto)[^)]+)\)'
            return re.sub(pattern, replace_link, text)

        markdowner = Markdown(
            extras=['header-ids', 'metadata']
        )
        
        faq_sections = []
        toc_items = []
        faq_dir = 'viewer/content/faq'
        
        try:
            md_files = sorted([f for f in os.listdir(faq_dir) if f.endswith('.md')])
            
            for md_file in md_files:
                try:
                    with open(os.path.join(faq_dir, md_file), 'r', encoding='utf-8') as f:
                        content = f.read()
                        processed_content = internal_link_preprocessor(content)
                        html_content = markdowner.convert(processed_content)
                        title = markdowner.metadata.get('title', '')
                        
                        # Add to TOC with slugified anchor
                        if title:
                            toc_items.append({
                                'title': title,
                                'anchor': slugify(title)
                            })
                        
                        faq_sections.append({
                            'title': title,
                            'content': html_content
                        })
                except Exception as e:
                    faq_sections.append({
                        'title': f'Error in {md_file}',
                        'content': f'<p>Error loading section: {str(e)}</p>'
                    })
            
            context['faq_content'] = faq_sections
            context['toc_items'] = toc_items
            
        except FileNotFoundError:
            context['faq_content'] = [{'title': 'Error', 'content': '<p>FAQ directory not found.</p>'}]
            context['toc_items'] = []
        except Exception as e:
            context['faq_content'] = [{'title': 'Error', 'content': f'<p>Error loading FAQ content: {str(e)}</p>'}]
            context['toc_items'] = []
        
        return context

