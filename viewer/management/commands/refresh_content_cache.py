from django.core.management.base import BaseCommand
from django.core.cache import cache
from datetime import datetime
import requests
import logging

logger = logging.getLogger(__name__)

BLOG_URL = 'https://www.bennett.ox.ac.uk/blog/index.json'
PAPER_URL = 'https://www.bennett.ox.ac.uk/papers/index.json'


class Command(BaseCommand):
    help = 'Refreshes the cached blog posts and papers from Bennett Institute'

    def handle(self, *args, **options):
        self.stdout.write('Refreshing content cache...')
        blog_success = self.refresh_blog_posts()
        paper_success = self.refresh_papers()

        if blog_success:
            self.stdout.write(self.style.SUCCESS('Successfully cached blog posts'))
        if paper_success:
            self.stdout.write(self.style.SUCCESS('Successfully cached papers'))

    def fetch_json(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def refresh_blog_posts(self):
        try:
            data = self.fetch_json(BLOG_URL)
            if 'posts' not in data:
                raise ValueError("No 'posts' field in API response")

            selected_posts = [
                post for post in data['posts']
                if (post.get('categories') or []) and 'OpenPrescribing Hospitals' in (post.get('categories') or [])
            ]

            all_posts = []
            posts_by_tag = {}

            for post in selected_posts:
                date_str = post.get('date', '')
                try:
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %z')
                    formatted_date = parsed_date.strftime('%d %B %Y')
                except Exception as ex:
                    logger.error(f"Error parsing date for blog post: {ex}")
                    formatted_date = ''

                formatted_post = {
                    'title': post.get('title', ''),
                    'date': formatted_date,
                    'summary': post.get('summary', ''),
                    'link': post.get('link', ''),
                    'authors': ', '.join(filter(None, post.get('authors', []))) or 'Unknown',
                    'image': post.get('image', ''),
                    'tags': post.get('tags') or []
                }
                all_posts.append(formatted_post)

                if formatted_post['tags']:
                    for tag in formatted_post['tags']:
                        posts_by_tag.setdefault(tag, []).append(formatted_post)

            untagged_posts = [post for post in all_posts if not post['tags']]
            if untagged_posts:
                posts_by_tag['Other'] = untagged_posts

            cache_data = {
                'all_posts': all_posts,
                'posts_by_tag': dict(sorted(posts_by_tag.items(), key=lambda x: len(x[1]), reverse=True)),
                'error': None,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            cache.set('bennett_blog_data', cache_data, None)
            return True

        except Exception as e:
            logger.error(f"Error fetching blog posts: {str(e)}")
            self.stdout.write(self.style.ERROR(f'Failed to cache blog posts: {str(e)}'))
            return False

    def refresh_papers(self):
        try:
            data = self.fetch_json(PAPER_URL)
            if 'papers' not in data:
                raise ValueError("No 'papers' field in API response")

            hospital_papers = [
                paper for paper in data['papers']
                if (paper.get('categories') or []) and 'OpenPrescribing' in (paper.get('categories') or []) and
                   'oph' in (paper.get('tags') or [])
            ]

            all_papers = []
            papers_by_status = {}

            for paper in hospital_papers:
                date_str = paper.get('date', '')
                try:
                    parsed_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                    formatted_date = parsed_date.strftime('%d %B %Y')
                except Exception as ex:
                    logger.error(f"Error parsing date for paper: {ex}")
                    formatted_date = ''

                status = paper.get('status', 'unknown')
                if status == 'preprint':
                    journal_value = paper.get('preprint_server', '')
                elif status == 'published':
                    journal_value = paper.get('journal', '')
                else:
                    journal_value = ''

                formatted_paper = {
                    'title': paper.get('title', ''),
                    'date': formatted_date,
                    'journal': journal_value,
                    'description': paper.get('description', ''),
                    'permalink': paper.get('permalink', ''),
                    'doi': paper.get('doi', ''),
                    'status': status,
                }
                all_papers.append(formatted_paper)

                papers_by_status.setdefault(status, []).append(formatted_paper)

            cache_data = {
                'all_papers': all_papers,
                'papers_by_status': papers_by_status,
                'error': None,
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            cache.set('bennett_papers_data', cache_data, None)
            return True

        except Exception as e:
            logger.error(f"Error fetching papers: {str(e)}")
            self.stdout.write(self.style.ERROR(f'Failed to cache papers: {str(e)}'))
            return False 