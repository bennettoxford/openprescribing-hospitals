import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from markdown2 import Markdown
from django.views.generic import TemplateView
from rest_framework.decorators import api_view
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.response import Response
from django.http import JsonResponse
from django.db.models.functions import Coalesce
from django.db.models import F, Value, Min, Max
from django.utils.safestring import mark_safe
from django.db.models import Exists, OuterRef, Q, Subquery, OuterRef
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.contrib.auth.views import LoginView as AuthLoginView
from django.shortcuts import redirect
from django.contrib.postgres.aggregates import ArrayAgg
import os
import re
from django.utils.text import slugify
from django.core.cache import cache
from typing import List, Set
from django.template.response import TemplateResponse

from .forms import LoginForm
from .models import (
    Organisation,
    VMP,
    IngredientQuantity,
    Ingredient,
    VTM,
    Measure,
    MeasureVMP,
    PrecomputedMeasure,
    PrecomputedMeasureAggregated,
    PrecomputedPercentile,
    OrgSubmissionCache,
    DataStatus,
    SCMDQuantity,
    DDDQuantity,
    MeasureTag,
    Dose
)
from .utils import safe_float


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

class LoginView(AuthLoginView):
    template_name = 'login.html'
    form_class = LoginForm

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

class MeasuresListView(TemplateView):
    template_name = "measures_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        measures = Measure.objects.prefetch_related('tags').order_by('draft', 'name')
        measure_tags = MeasureTag.objects.all()
        
        markdowner = Markdown()
        for measure in measures:
            measure.why_it_matters = markdowner.convert(measure.why_it_matters)
        
        for tag in measure_tags:
            if tag.description:
                tag.description = markdowner.convert(tag.description)
        
        one_month_ago = datetime.now().date() - timedelta(days=30)
        for measure in measures:
            measure.is_new = measure.first_published and measure.first_published >= one_month_ago
        
        context["measures"] = measures
        context["measure_tags"] = measure_tags
        return context


class MeasureItemView(TemplateView):
    template_name = "measure_item.html"

    def dispatch(self, request, *args, **kwargs):
        try:
            slug = kwargs.get("slug")
            measure = self.get_measure(slug)
            if measure.draft:
                if request.user.is_authenticated:
                    return super().dispatch(request, *args, **kwargs)
                else:
                    return redirect('viewer:measures_list')
        except Exception:
            pass
        
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")

        try:
            measure = self.get_measure(slug)
            context.update(self.get_measure_context(measure))
            context.update(self.get_precomputed_data(measure))
            
            is_new = False
            if measure.first_published:
                one_month_ago = datetime.now().date() - timedelta(days=30)
                is_new = measure.first_published >= one_month_ago
            
            context['is_new'] = is_new
        except Exception as e:
            context["error"] = str(e)

        return context

    def get_measure(self, slug):
        return Measure.objects.prefetch_related('tags').get(slug=slug)

    def get_measure_context(self, measure):
        markdowner = Markdown()

        measure_vmps = MeasureVMP.objects.filter(
            measure=measure
        ).select_related('vmp').values(
            'vmp__code',
            'vmp__name',
            'type',
            'unit'
        )
        
        denominator_vmps = [
            {
                'name': vmp['vmp__name'],
                'code': vmp['vmp__code'],
                'unit': vmp['unit']
            }
            for vmp in measure_vmps
            if vmp['type'] == 'denominator'
        ]
        
        numerator_vmps = [
            {
                'name': vmp['vmp__name'],
                'code': vmp['vmp__code'],
                'unit': vmp['unit']
            }
            for vmp in measure_vmps
            if vmp['type'] == 'numerator'
        ]

        tags_data = [
            {
                'name': tag.name,
                'description': markdowner.convert(tag.description) if tag.description else None,
                'colour': tag.colour
            }
            for tag in measure.tags.all()
        ]
    
        return {
            "measure_name": measure.name,
            "measure_name_short": measure.short_name,
            "is_draft": measure.draft,
            "why_it_matters": markdowner.convert(measure.why_it_matters),
            "how_is_it_calculated": markdowner.convert(measure.how_is_it_calculated),
            "measure_description": markdowner.convert(measure.description),
            "tags": tags_data,
            "denominator_vmps": json.dumps(denominator_vmps, cls=DjangoJSONEncoder),
            "numerator_vmps": json.dumps(numerator_vmps, cls=DjangoJSONEncoder),
        }

    def get_precomputed_data(self, measure):
       

        org_measures = PrecomputedMeasure.objects.filter(
            measure=measure
        ).select_related('organisation')
        
        aggregated_measures = PrecomputedMeasureAggregated.objects.filter(
            measure=measure
        )
        
        percentiles = PrecomputedPercentile.objects.filter(
            measure=measure
        )

        context = {}
        context.update(self.get_org_data(org_measures))
        context.update(self.get_aggregated_data(aggregated_measures))
        context.update(self.get_percentile_data(percentiles))

        return context

    def get_org_data(self, org_measures):
        total_orgs = Organisation.objects.count()

        current_orgs = Organisation.objects.filter(
            successor__isnull=True
        ).values('ods_code', 'ods_name').order_by('ods_name')
        
        measure_orgs = set(org_measures.values_list('organisation__ods_code', flat=True).distinct())
        
        predecessor_map = {}
        predecessor_to_successor = {}
 
        for org in Organisation.objects.exclude(successor__isnull=True).values(
            'ods_code', 'ods_name', 'successor__ods_code', 'successor__ods_name'
        ):
            successor_name = org['successor__ods_name']
            predecessor_name = org['ods_name']
            
            
            if successor_name not in predecessor_map:
                predecessor_map[successor_name] = []
            predecessor_map[successor_name].append(predecessor_name)
            predecessor_to_successor[predecessor_name] = successor_name
       
        org_data = {
            'data': {},
            'predecessor_map': predecessor_map
        }
        
        available_orgs = set()
        
        for org in current_orgs:
            org_name = org['ods_name']
            is_available = org['ods_code'] in measure_orgs
            org_data['data'][org_name] = {
                'available': is_available,
                'data': []
            }
            if is_available:
                available_orgs.add(org_name)
        
        for successor, predecessors in predecessor_map.items():
            if successor in org_data['data'] and org_data['data'][successor]['available']:
                for predecessor in predecessors:
                    org_data['data'][predecessor] = {
                        'available': True,
                        'data': []
                    }
                    available_orgs.add(predecessor)
        
        for measure in org_measures.values(
            'organisation__ods_code', 'organisation__ods_name', 'month', 'quantity', 'numerator', 'denominator'
        ):
            org_name = measure['organisation__ods_name']
            if org_name in org_data['data']:
                org_data['data'][org_name]['data'].append({
                    'month': measure['month'],
                    'quantity': measure['quantity'],
                    'numerator': measure['numerator'],
                    'denominator': measure['denominator']
                })
        
        return {
            "trusts_included": {
                "included": len(available_orgs),  # Updated to use total available orgs including predecessors
                "total": total_orgs
            },
            "org_data": json.dumps(org_data, cls=DjangoJSONEncoder),
        }

    def get_aggregated_data(self, aggregated_measures):
        region_data = defaultdict(lambda: {'name': '', 'data': []})
        icb_data = defaultdict(lambda: {'name': '', 'data': []})
        national_data = {'name': 'National', 'data': []}

        for measure in aggregated_measures:
            if measure.category == 'region':
                region_data[measure.label]['name'] = measure.label
                region_data[measure.label]['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })
            elif measure.category == 'icb':
                icb_data[measure.label]['name'] = measure.label
                icb_data[measure.label]['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })
            elif measure.category == 'national':
                national_data['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })

        region_list = list(region_data.values())
        icb_list = list(icb_data.values())

        return {
            "region_data": json.dumps(region_list, cls=DjangoJSONEncoder),
            "icb_data": json.dumps(icb_list, cls=DjangoJSONEncoder),
            "national_data": json.dumps(national_data, cls=DjangoJSONEncoder),
        }

    def get_percentile_data(self, percentiles):
        percentiles_list = list(percentiles.values())
        return {
            "percentile_data": json.dumps(percentiles_list, cls=DjangoJSONEncoder),
        }

@csrf_protect
@api_view(["POST"])
def filtered_quantities(request):
    search_items = request.data.get("names", None)
    ods_names = request.data.get("ods_names", None)
    quantity_type = request.data.get("quantity_type", None)
    
    if not all([search_items, ods_names, quantity_type]) or quantity_type == '--':
        return Response({"error": "Missing required parameters"}, status=400)


    vmp_ids = set()
    query = Q()
    for item in search_items:
        try:
            code, item_type = item.split('|')
            if item_type == 'vmp':
                query |= Q(code=code)
            elif item_type == 'vtm':
                query |= Q(vtm__vtm=code)
            elif item_type == 'ingredient':
                query |= Q(ingredients__code=code)
        except ValueError:
            return Response({"error": f"Invalid search item format: {item}"}, status=400)
    
    vmp_ids = set(VMP.objects.filter(query).values_list('id', flat=True))
    
    if not vmp_ids:
        return Response({"error": "No valid VMPs found"}, status=400)

    try:
        base_vmps = VMP.objects.filter(
            id__in=vmp_ids
        ).select_related('vtm').annotate(
            ingredient_names=ArrayAgg('ingredients__name', distinct=True),
            ingredient_codes=ArrayAgg('ingredients__code', distinct=True)
        ).values(
            'id', 'code', 'name', 'vtm__name',
            'ingredient_names', 'ingredient_codes'
        )

        response_data = []
        for vmp in base_vmps:
            response_item = {
                'vmp__code': vmp['code'],
                'vmp__name': vmp['name'],
                'vmp__vtm__name': vmp['vtm__name'],
                'ingredient_names': vmp['ingredient_names'],
                'ingredient_codes': vmp['ingredient_codes'],
                'organisation__ods_code': None,
                'organisation__ods_name': None,
                'organisation__region': None,
                'organisation__icb': None,
                'data': []
            }
            response_data.append(response_item)

        quantity_model = {
            "VMP Quantity": SCMDQuantity,
            "Ingredient Quantity": IngredientQuantity,
            "Daily Defined Doses": DDDQuantity
        }.get(quantity_type)

        if quantity_model:
            quantity_data = quantity_model.objects.filter(
                vmp_id__in=vmp_ids,
                organisation__ods_name__in=ods_names
            ).select_related('organisation')

            for item in quantity_data:
                response_item = {
                    'vmp__code': item.vmp.code,
                    'vmp__name': item.vmp.name,
                    'vmp__vtm__name': item.vmp.vtm.name if item.vmp.vtm else None,
                    'ingredient_names': [ing.name for ing in item.vmp.ingredients.all()],
                    'ingredient_codes': [ing.code for ing in item.vmp.ingredients.all()],
                    'organisation__ods_code': item.organisation.ods_code,
                    'organisation__ods_name': item.organisation.ods_name,
                    'organisation__region': item.organisation.region,
                    'organisation__icb': item.organisation.icb,
                    'data': item.data
                }
                response_data.append(response_item)

        return Response(response_data)

    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return Response({"error": "An error occurred while processing the request"}, status=500)


class OrgsSubmittingDataView(TemplateView):
    template_name = 'org_submissions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        regions_with_icbs = {}
        for org in Organisation.objects.filter(successor__isnull=True).order_by('region', 'icb'):
            if org.region not in regions_with_icbs:
                regions_with_icbs[org.region] = set()
            if org.icb:
                regions_with_icbs[org.region].add(org.icb)
        
        hierarchy = [
            {
                'region': region,
                'icbs': sorted(list(icbs))
            }
            for region, icbs in sorted(regions_with_icbs.items())
        ]
        
        context['regions_hierarchy'] = json.dumps(hierarchy)
        
        # Get latest dates for each file type
        latest_dates = {}
        for file_type in ['final']:
            latest = DataStatus.objects.filter(
                file_type=file_type
            ).aggregate(
                latest_date=Max('year_month')
            )['latest_date']
            if latest:
                latest_dates[file_type] = latest.strftime("%B %Y")
            else:
                latest_dates[file_type] = None
        
        context['latest_dates'] = json.dumps(latest_dates)

        # Step 1: Collect data
        org_data = defaultdict(lambda: {'successor': None, 'submissions': {}, 'predecessors': []})
        all_dates = set()
        
        for cache in OrgSubmissionCache.objects.select_related('organisation', 'successor').order_by('organisation__ods_name', 'month'):
            org_name = cache.organisation.ods_name
            org_data[org_name]['successor'] = cache.successor.ods_name if cache.successor else None
            month_str = cache.month.isoformat() if isinstance(cache.month, date) else str(cache.month)
            org_data[org_name]['submissions'][month_str] = {
                'has_submitted': cache.has_submitted,
                'vmp_count': cache.vmp_count or 0
            }
            all_dates.add(month_str)

        # Step 2: Build predecessor relationships
        for org_name, data in org_data.items():
            if data['successor']:
                org_data[data['successor']]['predecessors'].append(org_name)

        # Step 3: Restructure data
        restructured_data = []
        processed_orgs = set()

        def build_org_hierarchy(org_name):
            org_entry = {
                'name': org_name,
                'data': org_data[org_name]['submissions'],
                'predecessors': []
            }
            processed_orgs.add(org_name)

            # Sort predecessors to ensure consistent ordering
            sorted_predecessors = sorted(org_data[org_name]['predecessors'])
            for pred in sorted_predecessors:
                if pred not in processed_orgs:
                    org_entry['predecessors'].append(build_org_hierarchy(pred))

            return org_entry

        # Process current organizations (those without successors) first
        current_orgs = sorted([org for org, data in org_data.items() if not data['successor']])
        for org in current_orgs:
            if org not in processed_orgs:
                restructured_data.append(build_org_hierarchy(org))

        # Process any remaining organizations
        for org in sorted(org_data.keys()):
            if org not in processed_orgs:
                restructured_data.append(build_org_hierarchy(org))

        latest_date = max(all_dates) if all_dates else None
        for org_entry in restructured_data:
            org_entry['latest_submission'] = org_entry['data'].get(latest_date, False)

        for org_entry in restructured_data:
            org = Organisation.objects.filter(ods_name=org_entry['name']).first()
            if org.successor:
                org_entry['region'] = org.successor.region
                org_entry['icb'] = org.successor.icb
            else:
                org_entry['region'] = org.region
                org_entry['icb'] = org.icb
            
            for pred in org_entry['predecessors']:
                pred_org = Organisation.objects.filter(ods_name=pred['name']).first()
                if pred_org.successor:
                    pred['region'] = pred_org.successor.region
                    pred['icb'] = pred_org.successor.icb
                else:
                    pred['region'] = pred_org.region
                    pred['icb'] = pred_org.icb

        context['org_data_json'] = mark_safe(json.dumps(restructured_data))

        if all_dates:
            earliest_date = min(all_dates)
            latest_date = max(all_dates)
            
            context['earliest_date'] = datetime.strptime(earliest_date, "%Y-%m-%d").strftime("%B %Y")
            context['latest_date'] = datetime.strptime(latest_date, "%Y-%m-%d").strftime("%B %Y")
        else:
            context['earliest_date'] = None
            context['latest_date'] = None

        return context


@api_view(["POST"])
def filtered_vmp_count(request):
    search_items = request.data.get("names", [])
    
    # Initialize empty sets for codes and display names
    vmp_codes = set()
    display_names = {}

    print(search_items)
    # Process each search item
    for item in search_items:
        code, item_type = item.split('|')
        
        if item_type == 'vtm':
            # Get all VMPs for this VTM
            vtm_vmps = VMP.objects.filter(vtm__vtm=code).values_list('code', flat=True)
            vmp_codes.update(vtm_vmps)
            # Get VTM display name
            vtm = VTM.objects.filter(vtm=code).first()
            if vtm:
                display_names[f"{code}|vtm"] = f"{vtm.name} ({code})"

        elif item_type == 'vmp':
            vmp_codes.add(code)
            # Get VMP display name
            vmp = VMP.objects.filter(code=code).first()
            if vmp:
                display_names[f"{code}|vmp"] = f"{vmp.name} ({code})"

        elif item_type == 'ingredient':
            # Get all VMPs containing this ingredient
            ingredient_vmps = VMP.objects.filter(ingredients__code=code).values_list('code', flat=True)
            vmp_codes.update(ingredient_vmps)
            # Get ingredient display name
            ingredient = Ingredient.objects.filter(code=code).first()
            if ingredient:
                display_names[f"{code}|ingredient"] = f"{ingredient.name} ({code})"

    # Count unique VMPs
    vmp_count = len(vmp_codes)
    
    return Response({
        "vmp_count": vmp_count,
        "display_names": display_names
    })


class ContactView(TemplateView):
    template_name = "contact.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context
    
@api_view(["GET"])
def search_items(request):
    search_type = request.GET.get('type', 'product')
    search_term = request.GET.get('term', '').lower()
    
    if search_type == 'product':
        # First get matching VMPs (both with and without VTMs)
        matching_vmps = VMP.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).select_related('vtm')

        # Organize VMPs by VTM
        vmp_by_vtm = {}
        standalone_vmps = []
        
        for vmp in matching_vmps:
            if vmp.vtm:
                vtm_key = vmp.vtm.vtm  # Using VTM code as key
                if vtm_key not in vmp_by_vtm:
                    vmp_by_vtm[vtm_key] = {
                        'vtm': vmp.vtm,
                        'vmps': []
                    }
                vmp_by_vtm[vtm_key]['vmps'].append(vmp)
            else:
                standalone_vmps.append(vmp)

        # Get additional VTMs that match the search term
        additional_vtms = VTM.objects.filter(
            Q(name__icontains=search_term) | 
            Q(vtm__icontains=search_term)
        ).exclude(vtm__in=vmp_by_vtm.keys())

        # Build results
        results = []
        
        # Add VTMs with their VMPs
        for vtm_data in vmp_by_vtm.values():
            vtm = vtm_data['vtm']
            results.append({
                'code': vtm.vtm,
                'name': vtm.name,
                'type': 'vtm',
                'isExpanded': False,
                'display_name': f"{vtm.name} ({vtm.vtm})",
                'vmps': [{
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp',
                    'display_name': f"{vmp.name} ({vmp.code})"
                } for vmp in vtm_data['vmps']]
            })

        # Add additional VTMs with their VMPs
        for vtm in additional_vtms:
            vmps = vtm.vmps.all()
            results.append({
                'code': vtm.vtm,
                'name': vtm.name,
                'type': 'vtm',
                'isExpanded': False,
                'display_name': f"{vtm.name} ({vtm.vtm})",
                'vmps': [{
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp',
                    'display_name': f"{vmp.name} ({vmp.code})"
                } for vmp in vmps]
            })

        # Add standalone VMPs
        results.extend([{
            'code': vmp.code,
            'name': vmp.name,
            'type': 'vmp',
            'display_name': f"{vmp.name} ({vmp.code})"
        } for vmp in standalone_vmps])

        return JsonResponse({'results': results})
    
    elif search_type == 'ingredient':
        items = Ingredient.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).values('name', 'code').distinct().order_by('name')[:50]
        return JsonResponse({
            'results': [{
                'code': item['code'],
                'name': item['name'],
                'type': 'ingredient'
            } for item in items]
        })
    

    
    return JsonResponse({'results': []})


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


class AboutView(TemplateView):
    template_name = "about.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

@method_decorator(login_required, name='dispatch')
class ProductDetailsView(TemplateView):
    template_name = "product_details.html"
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update({
            'products': mark_safe(json.dumps([], cls=DjangoJSONEncoder)),
            'search_term': ''
        })

        return context

@csrf_protect
@login_required
@api_view(["POST"])
def product_details_api(request):
    try:
        search_items = request.data.get("names", [])
        if not search_items:
            return Response({"error": "No products selected"}, status=400)
        
        vmp_ids = get_vmp_ids_from_search_items(search_items)
        if not vmp_ids:
            return Response({"error": "No valid VMPs found"}, status=400)
        
        products = build_product_details(vmp_ids)
        return Response(products)
        
    except ValueError as e:
        return Response({"error": str(e)}, status=400)
    except Exception as e:
        return Response({"error": "An error occurred while processing the request"}, status=500)

def get_vmp_ids_from_search_items(search_items: List[str]) -> Set[int]:
    """
    Extract VMP IDs from search items based on type.
    
    Args:
        search_items: List of search items in format "code|type"
        
    Returns:
        Set of VMP IDs
        
    Raises:
        ValueError: If search item format is invalid
    """
    query = Q()
    for item in search_items:
        try:
            code, item_type = item.split('|')
            if item_type == 'vmp':
                query |= Q(code=code)
            elif item_type == 'vtm':
                query |= Q(vtm__vtm=code)
            elif item_type == 'ingredient':
                query |= Q(ingredients__code=code)
        except ValueError:
            raise ValueError(f"Invalid search item format: {item}")
    
    return set(VMP.objects.filter(query).values_list('id', flat=True))

def build_product_details(vmp_ids):
    """Build detailed product information for given VMP IDs."""
    vmps = VMP.objects.filter(id__in=vmp_ids).select_related('vtm').prefetch_related(
        'ingredients', 'ddds', 'ont_form_routes', 'who_routes', 'atcs',
        'ingredient_strengths__ingredient', 'calculation_logic'
    )
    
    products = []
    for vmp in vmps:
        product_data = build_single_product_data(vmp)
        products.append(product_data)
    
    return products

def build_single_product_data(vmp):
    """Build detailed data for a single VMP."""
    ingredient_names = ", ".join([i.name for i in vmp.ingredients.all()])

    ddd_value = None
    ddd = vmp.ddds.first()
    if ddd:
        ddd_value = {
            'ddd': safe_float(ddd.ddd),
            'unit_type': ddd.unit_type,
            'route': ddd.who_route.name
        }

    ddd_info = None
    if ddd_value and ddd_value['ddd'] is not None:
        ddd_info = f"{ddd_value['ddd']} {ddd_value['unit_type']}"

    # Check for existence of quantities and get their units
    has_scmd_quantity = SCMDQuantity.objects.filter(
        vmp=vmp,
        data__0__0__isnull=False
    ).exists()

    # Get SCMD units information if SCMD quantity data exists
    scmd_units = []
    if has_scmd_quantity:
        scmd_quantity_obj = SCMDQuantity.objects.filter(vmp=vmp).first()
        if scmd_quantity_obj and scmd_quantity_obj.data:
            units_set = set()
            for entry in scmd_quantity_obj.data:
                if len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            scmd_units = sorted(list(units_set))

    # Get dose data and units
    has_dose = Dose.objects.filter(
        vmp=vmp,
        data__0__0__isnull=False
    ).exists()
    
    dose_units = []
    if has_dose:
        dose_obj = Dose.objects.filter(vmp=vmp).first()
        if dose_obj and dose_obj.data:
            units_set = set()
            for entry in dose_obj.data:
                if len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            dose_units = sorted(list(units_set))

    # Get ingredient quantity data and units
    has_ingredient_quantities = IngredientQuantity.objects.filter(
        vmp=vmp,
        data__0__0__isnull=False
    ).exists()
    
    ingredient_units = []
    if has_ingredient_quantities:
        ingredient_obj = IngredientQuantity.objects.filter(vmp=vmp).first()
        if ingredient_obj and ingredient_obj.data:
            units_set = set()
            for entry in ingredient_obj.data:
                if len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            ingredient_units = sorted(list(units_set))

    has_ddd_quantity = DDDQuantity.objects.filter(
        vmp=vmp,
        data__0__0__isnull=False
    ).exists()

    # Get calculation logic for each quantity type
    dose_logic = None
    ddd_logic = None
    ingredient_logic = []

    for calc_logic in vmp.calculation_logic.select_related('ingredient'):
        if calc_logic.logic_type == 'dose':
            dose_logic = {
                'logic': calc_logic.logic,
                'unit_dose_uom': vmp.unit_dose_uom,
                'udfs': safe_float(vmp.udfs),
                'udfs_uom': vmp.udfs_uom
            }
        elif calc_logic.logic_type == 'ddd':
            ddd_logic = {
                'logic': calc_logic.logic,
                'ingredient': calc_logic.ingredient.name if calc_logic.ingredient else None
            }
        elif calc_logic.logic_type == 'ingredient':
            # Get strength information for this ingredient
            strength_info = None
            if calc_logic.ingredient:
                strength = vmp.ingredient_strengths.filter(ingredient=calc_logic.ingredient).first()
                if strength:
                    strength_info = {
                        'numerator_value': safe_float(strength.strnt_nmrtr_val),
                        'numerator_uom': strength.strnt_nmrtr_uom_name,
                        'denominator_value': safe_float(strength.strnt_dnmtr_val),
                        'denominator_uom': strength.strnt_dnmtr_uom_name,
                        'basis_of_strength_type': strength.basis_of_strength_type,
                        'basis_of_strength_name': strength.basis_of_strength_name
                    }
            
            ingredient_logic.append({
                'ingredient': calc_logic.ingredient.name if calc_logic.ingredient else None,
                'logic': calc_logic.logic,
                'strength_info': strength_info
            })


    return {
        'vmp_name': vmp.name,
        'vmp_code': vmp.code,
        'vtm_name': vmp.vtm.name if vmp.vtm else None,
        'vtm_code': vmp.vtm.vtm if vmp.vtm else None,
        'routes': [route.name for route in vmp.ont_form_routes.all()],
        'who_routes': [route.name for route in vmp.who_routes.all()] if ddd_info else [],
        'atc_codes': [atc.code for atc in vmp.atcs.all()],
        'ingredient_names': ingredient_names,
        'ddd_info': ddd_info,
        'df_ind': vmp.df_ind,
        'has_scmd_quantity': has_scmd_quantity,
        'scmd_units': scmd_units,
        'has_dose': has_dose,
        'dose_units': dose_units,
        'has_ddd_quantity': has_ddd_quantity,
        'has_ingredient_quantities': has_ingredient_quantities,
        'ingredient_units': ingredient_units,
        'dose_logic': dose_logic,
        'ddd_logic': ddd_logic,
        'ingredient_logic': ingredient_logic
    }

class BlogListView(TemplateView):
    template_name = "blog_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        cached_data = cache.get('bennett_blog_data')
        if cached_data is not None:
            context.update(cached_data)
            return context
        
        context.update({
            'all_posts': [],
            'posts_by_tag': {},
            'error': 'No cached data available. Please run refresh_content_cache management command.'
        })
        return context

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

def error_handler(request, error_code, error_name, error_message, status_code, exception=None):
    return TemplateResponse(
        request,
        "error.html",
        status=status_code,
        context={
            "error_code": error_code,
            "error_name": error_name,
            "error_message": error_message,
        },
    )

def bad_request(request, exception=None):
    return error_handler(
        request, "400", "Bad request", 
        "Your request could not be processed due to invalid or malformed data.", 
        400, exception
    )

def csrf_failure(request, reason=""):
    return error_handler(
        request, "CSRF", "Security verification failed",
        "Your request could not be completed due to a security verification failure. Please refresh the page and try again.",
        403
    )

def page_not_found(request, exception=None):
    return error_handler(
        request, "404", "Page not found",
        "Sorry, the page you're looking for doesn't exist or has been moved.",
        404, exception
    )

def permission_denied(request, exception=None):
    return error_handler(
        request, "403", "Access forbidden",
        "You don't have permission to access this resource.",
        403, exception
    )

def server_error(request):
    return error_handler(
        request, "500", "Something went wrong",
        "We're experiencing a technical issue.",
        500
    )
