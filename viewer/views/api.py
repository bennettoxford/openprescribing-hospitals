import re
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db.models import Q
from typing import List, Set
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_protect
from django.contrib.postgres.aggregates import ArrayAgg
from ..models import (
    VMP, 
    SCMDQuantity, 
    Dose, 
    IngredientQuantity, 
    DDDQuantity,
    VTM,
    Ingredient,
    ATC,
    Organisation
)
from ..utils import safe_float


@api_view(["GET"])
def search_products(request):
    search_type = request.GET.get('type', 'product')
    search_term = request.GET.get('term', '').lower()
    
    if search_type == 'product':
        # Get VMPs that start with search term (prioritised)
        priority_vmps = VMP.objects.filter(
            Q(name__istartswith=search_term) | 
            Q(code__istartswith=search_term)
        ).select_related('vtm').order_by('name')
        
        # Get VMPs that contain search term but don't start with it
        remaining_vmps = VMP.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).exclude(
            Q(name__istartswith=search_term) | 
            Q(code__istartswith=search_term)
        ).select_related('vtm').order_by('name')

        matching_vmps = list(priority_vmps) + list(remaining_vmps)

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
        priority_vtms = VTM.objects.filter(
            Q(name__istartswith=search_term) | 
            Q(vtm__istartswith=search_term)
        ).exclude(vtm__in=vmp_by_vtm.keys()).order_by('name')
        
        remaining_vtms = VTM.objects.filter(
            Q(name__icontains=search_term) | 
            Q(vtm__icontains=search_term)
        ).exclude(
            Q(name__istartswith=search_term) | 
            Q(vtm__istartswith=search_term)
        ).exclude(vtm__in=vmp_by_vtm.keys()).order_by('name')
        
        additional_vtms = list(priority_vtms) + list(remaining_vtms)

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
                'vmps': [{
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp'
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
                'vmps': [{
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp'
                } for vmp in vmps]
            })

        results.extend([{
            'code': vmp.code,
            'name': vmp.name,
            'type': 'vmp'
        } for vmp in standalone_vmps])

        def sort_key(item):
            name = item['name'].lower()
            starts_with_term = name.startswith(search_term)
            return (not starts_with_term, name)
        
        results.sort(key=sort_key)

        return JsonResponse({'results': results})
    
    elif search_type == 'ingredient':

        priority_ingredients = Ingredient.objects.filter(
            Q(name__istartswith=search_term) | 
            Q(code__istartswith=search_term)
        ).distinct().order_by('name')
        
        remaining_ingredients = Ingredient.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).exclude(
            Q(name__istartswith=search_term) | 
            Q(code__istartswith=search_term)
        ).distinct().order_by('name')
        
        all_ingredients = (list(priority_ingredients) + 
                          list(remaining_ingredients))[:50]
        
        results = []
        for ingredient in all_ingredients:
            ingredient_vmps = VMP.objects.filter(
                ingredients__code=ingredient.code
            ).select_related('vtm').values('code', 'name')
            
            vmp_list = list(ingredient_vmps)
            
            results.append({
                'code': ingredient.code,
                'name': ingredient.name,
                'type': 'ingredient',
                'vmp_count': len(vmp_list),
                'vmps': vmp_list
            })
        
        return JsonResponse({'results': results})
    
    elif search_type == 'atc':
        priority_atcs = ATC.objects.filter(
            Q(name__istartswith=search_term) | 
            Q(code__istartswith=search_term)
        ).order_by('name')
        
        remaining_atcs = ATC.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).exclude(
            Q(name__istartswith=search_term) | 
            Q(code__istartswith=search_term)
        ).order_by('name')

        all_atcs = (list(priority_atcs) + 
                   list(remaining_atcs))[:50]
        
        results = []
        
        def build_hierarchy_path(atc_obj):
            """Build the full hierarchy path for an ATC code"""
            path_parts = []
            if atc_obj.level_1:
                path_parts.append(atc_obj.level_1)
            if atc_obj.level_2:
                path_parts.append(atc_obj.level_2)
            if atc_obj.level_3:
                path_parts.append(atc_obj.level_3)
            if atc_obj.level_4:
                path_parts.append(atc_obj.level_4)
            if atc_obj.level_5:
                path_parts.append(atc_obj.level_5)
            return path_parts
        
        def get_parent_path(code):
            """Get the hierarchy path for parent codes"""
            parent_codes = []
            if len(code) >= 1:
                parent_codes.append(code[:1])
            if len(code) >= 3:
                parent_codes.append(code[:3])
            if len(code) >= 4:
                parent_codes.append(code[:4])
            if len(code) >= 5:
                parent_codes.append(code[:5])
            return parent_codes
        
        for atc in all_atcs:
            code_len = len(atc.code)
            if code_len == 1:
                level = 1
                level_name = atc.level_1
            elif code_len == 3:
                level = 2
                level_name = atc.level_2
            elif code_len == 4:
                level = 3
                level_name = atc.level_3
            elif code_len == 5:
                level = 4
                level_name = atc.level_4
            elif code_len == 7:
                level = 5
                level_name = atc.level_5
            else:
                continue
            
            atc_vmps = atc.get_vmps().select_related('vtm').values('code', 'name')
            vmp_list = list(atc_vmps)
            vmp_count = len(vmp_list)

            hierarchy_path = build_hierarchy_path(atc)

            parent_codes = get_parent_path(atc.code)
            parent_path = []
            if parent_codes:
                parent_atcs = ATC.objects.filter(
                    code__in=parent_codes
                ).order_by('code')
                for parent in parent_atcs:
                    if parent.code != atc.code:
                        parent_hierarchy = build_hierarchy_path(parent)
                        if parent_hierarchy:
                            parent_path.append({
                                'code': parent.code,
                                'name': parent_hierarchy[-1],
                                'level': (len(parent.code) 
                                         if len(parent.code) <= 5 else 5)
                            })
            
            results.append({
                'code': atc.code,
                'name': level_name or atc.name,
                'full_name': atc.name,
                'type': 'atc',
                'level': level,
                'vmp_count': vmp_count,
                'vmps': vmp_list,
                'hierarchy_path': hierarchy_path,
                'parent_path': parent_path,
            })

        return JsonResponse({'results': results})

    return JsonResponse({'results': []})



@csrf_protect
@api_view(["POST"])
def select_quantity_type(request):
    """
    Select the most appropriate quantity type for analysis based on selected products.
    """
    search_items = request.data.get("names", [])
    
    if not search_items:
        return Response({"error": "No products provided"}, status=400)
    
    try:
        vmp_ids = get_vmp_ids_from_search_items(search_items)

        if not vmp_ids:
            return Response({"error": "No valid VMPs found"}, status=400)


        product_info = get_product_info(vmp_ids)
        recommended_quantity_types, reasoning = determine_quantity_type_from_products(product_info)
 
        selected_quantity_type = recommended_quantity_types[0] if recommended_quantity_types else None

        return Response({
            "selected_quantity_type": selected_quantity_type,
            "recommended_quantity_types": recommended_quantity_types,
            "reasoning": reasoning,
            "vmp_count": len(vmp_ids),
            "products": product_info
        })

    except ValueError as e:
        return Response({"error": str(e)}, status=400)
    except Exception as e:
        return Response({"error": "An error occurred while selecting quantity type"}, status=500)


def determine_quantity_type_from_products(products):
    """
    Determine quantity type following the decision tree logic.
    
    Returns:
        tuple: (quantity_type, reasoning)
    """
    if len(products) == 1:
        if all_products_have_dose_quantity(products):
            return ["SCMD Quantity", "Unit Dose Quantity"], "Single product selected - all have unit dose quantity, so can use SCMD quantity or unit dose quantity"
        else:
            return ["SCMD Quantity"], "Single product selected - using SCMD quantity"
    
    if all_products_have_same_single_ingredient(products):
        if all_products_have_same_ingredient_quantity_units(products):
            if all_products_have_ddd_quantity(products):
                return ["Defined Daily Dose Quantity", "Ingredient Quantity"], "All products have same ingredient with same units and DDD available - using DDD quantity or ingredient quantity"
            else:
                return ["Ingredient Quantity"], "All products have same ingredient with same units - using ingredient quantity"
        else:
            return ["Defined Daily Dose Quantity"], "All products have same ingredient but different units - using DDD quantity"

    else:
        return ["Defined Daily Dose Quantity"], "Multiple products with different ingredients"


def all_products_have_same_single_ingredient(products):
    """Check if all products have exactly the same single ingredient."""
    if not products:
        return False
    
    # Check if all products have exactly one ingredient
    for product in products:
        ingredient_names = product.get('ingredient_names', [])
        if not ingredient_names or len(ingredient_names) != 1:
            return False
    
    # Check if all products have the same ingredient
    first_ingredient = products[0]['ingredient_names'][0]
    return all(
        product['ingredient_names'][0] == first_ingredient 
        for product in products
    )


def all_products_have_same_ingredient_quantity_units(products):
    """Check if all products have the same ingredient quantity units."""
    if not products:
        return False
    
    reference_units = None
    for product in products:
        if product['quantity_availability']['ingredient']['available']:
            reference_units = set(product['quantity_availability']['ingredient']['units'])
            break
    
    if reference_units is None:
        return False
    
    for product in products:
        if product['quantity_availability']['ingredient']['available']:
            product_units = set(product['quantity_availability']['ingredient']['units'])
            if product_units != reference_units:
                return False
        else:
            return False
    
    return True


def all_products_have_ddd_quantity(products):
    """Check if all products have DDD quantity available."""
    return all(
        product['quantity_availability']['ddd']['available'] 
        for product in products
    )


def all_products_have_dose_quantity(products):
    """Check if all products have dose quantity available."""
    return all(
        product["quantity_availability"]["dose"]["available"] for product in products
    )


def get_product_info(vmp_ids):
    """
    Get:
    - Basic VMP details (code, name, VTM)
    - Ingredients (names and codes)
    - Quantity availability flags and units for each quantity type
    """
    vmps_data = VMP.objects.filter(
        id__in=vmp_ids
    ).select_related('vtm').annotate(
        ingredient_names=ArrayAgg('ingredients__name', distinct=True),
        ingredient_codes=ArrayAgg('ingredients__code', distinct=True)
    ).values(
        'id', 'code', 'name', 'vtm__name', 'vtm__vtm',
        'ingredient_names', 'ingredient_codes'
    )
    
    vmp_info = {vmp['id']: vmp for vmp in vmps_data}
    
    for vmp_id in vmp_ids:
        if vmp_id in vmp_info:
            vmp_info[vmp_id].update({
                'has_scmd_quantity': False,
                'scmd_units': [],
                'has_dose_quantity': False,
                'dose_units': [],
                'has_ingredient_quantity': False,
                'ingredient_quantity_units': [],
                'has_ddd_quantity': False,
                'ddd_units': []
            })
    
    _populate_scmd_quantity_info(vmp_ids, vmp_info)
    _populate_dose_quantity_info(vmp_ids, vmp_info)
    _populate_ingredient_quantity_info(vmp_ids, vmp_info)
    _populate_ddd_quantity_info(vmp_ids, vmp_info)
    
    products = []
    for vmp_id in vmp_ids:
        if vmp_id in vmp_info:
            vmp = vmp_info[vmp_id]
            products.append({
                'vmp_id': vmp_id,
                'code': vmp['code'],
                'name': vmp['name'],
                'vtm_name': vmp['vtm__name'],
                'vtm_code': vmp['vtm__vtm'],
                'ingredient_names': vmp['ingredient_names'] or [],
                'ingredient_codes': vmp['ingredient_codes'] or [],
                'quantity_availability': {
                    'scmd': {
                        'available': vmp['has_scmd_quantity'],
                        'units': vmp['scmd_units']
                    },
                    'dose': {
                        'available': vmp['has_dose_quantity'],
                        'units': vmp['dose_units']
                    },
                    'ingredient': {
                        'available': vmp['has_ingredient_quantity'],
                        'units': vmp['ingredient_quantity_units']
                    },
                    'ddd': {
                        'available': vmp['has_ddd_quantity'],
                        'units': vmp['ddd_units']
                    }
                }
            })
    
    return products


def _populate_scmd_quantity_info(vmp_ids, vmp_info):
    """Populate SCMD quantity information"""
    scmd_quantities = SCMDQuantity.objects.filter(
        vmp_id__in=vmp_ids
    ).values('vmp_id', 'data')
    
    for scmd in scmd_quantities:
        vmp_id = scmd['vmp_id']
        if vmp_id not in vmp_info or not scmd['data']:
            continue
            
        has_valid_data = any(
            entry and len(entry) >= 2 and entry[1] is not None 
            for entry in scmd['data']
        )
        
        if has_valid_data:
            vmp_info[vmp_id]['has_scmd_quantity'] = True
            
            units_set = set()
            for entry in scmd['data']:
                if entry and len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            vmp_info[vmp_id]['scmd_units'] = sorted(list(units_set))


def _populate_dose_quantity_info(vmp_ids, vmp_info):
    """Populate Dose quantity information"""
    dose_quantities = Dose.objects.filter(
        vmp_id__in=vmp_ids,
        data__0__0__isnull=False
    ).values('vmp_id', 'data')
    
    for dose in dose_quantities:
        vmp_id = dose['vmp_id']
        if vmp_id not in vmp_info:
            continue
            
        vmp_info[vmp_id]['has_dose_quantity'] = True
        
        if dose['data']:
            units_set = set()
            for entry in dose['data']:
                if entry and len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            vmp_info[vmp_id]['dose_units'] = sorted(list(units_set))


def _populate_ingredient_quantity_info(vmp_ids, vmp_info):
    """Populate Ingredient quantity information"""
    ingredient_quantities = IngredientQuantity.objects.filter(
        vmp_id__in=vmp_ids,
        data__0__0__isnull=False
    ).values('vmp_id', 'data')
    
    for ingredient_qty in ingredient_quantities:
        vmp_id = ingredient_qty['vmp_id']
        if vmp_id not in vmp_info:
            continue
            
        vmp_info[vmp_id]['has_ingredient_quantity'] = True
        
        if ingredient_qty['data']:
            units_set = set()
            for entry in ingredient_qty['data']:
                if entry and len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            vmp_info[vmp_id]['ingredient_quantity_units'] = sorted(list(units_set))


def _populate_ddd_quantity_info(vmp_ids, vmp_info):
    """Populate DDD quantity information"""
    ddd_quantities = DDDQuantity.objects.filter(
        vmp_id__in=vmp_ids,
        data__0__0__isnull=False
    ).values('vmp_id', 'data')
    
    for ddd_qty in ddd_quantities:
        vmp_id = ddd_qty['vmp_id']
        if vmp_id not in vmp_info:
            continue
            
        vmp_info[vmp_id]['has_ddd_quantity'] = True
        
        if ddd_qty['data']:
            units_set = set()
            for entry in ddd_qty['data']:
                if entry and len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            vmp_info[vmp_id]['ddd_units'] = sorted(list(units_set))


@csrf_protect
@api_view(["POST"])
def get_quantity_data(request):
    search_items = request.data.get("names", None)
    ods_names = request.data.get("ods_names", None)
    quantity_type = request.data.get("quantity_type", None)
    
    if not all([search_items, quantity_type]):
        return Response({"error": "Missing required parameters"}, status=400)

    vmp_ids = set()
    query = Q()
    for item in search_items:
        try:
            code = item["code"]
            item_type = item["type"]
            if item_type == 'vmp':
                query |= Q(code=code)
            elif item_type == 'vtm':
                query |= Q(vtm__vtm=code)
            elif item_type == 'ingredient':
                query |= Q(ingredients__code=code)
            elif item_type == 'atc':
                query |= Q(atcs__code__startswith=code)
        except (KeyError, TypeError):
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
            "SCMD Quantity": SCMDQuantity,
            "Unit Dose Quantity": Dose,
            "Ingredient Quantity": IngredientQuantity,
            "Defined Daily Dose Quantity": DDDQuantity
        }.get(quantity_type)

        if quantity_model:
            if ods_names:
                quantity_data = quantity_model.objects.filter(
                    vmp_id__in=vmp_ids,
                    organisation__ods_name__in=ods_names
                ).select_related('organisation')
            else:
                quantity_data = quantity_model.objects.filter(
                    vmp_id__in=vmp_ids
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
                    'organisation__region': item.organisation.region.name if item.organisation.region else None,
                    'organisation__icb': item.organisation.icb.name if item.organisation.icb else None,
                    'data': item.data
                }
                response_data.append(response_item)
        return Response(response_data)

    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return Response({"error": "An error occurred while processing the request"}, status=500)


@csrf_protect
@api_view(["POST"])
def get_product_details(request):
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

def get_vmp_ids_from_search_items(search_items: List[dict]) -> Set[int]:
    """
    Extract VMP IDs from search items based on type.
    
    Args:
        search_items: List of search items in format {"code": str, "type": str}
        
    Returns:
        Set of VMP IDs
        
    Raises:
        ValueError: If search item format is invalid
    """
    query = Q()
    
    for item in search_items:
        try:
            code = item["code"]
            item_type = item["type"]
            if item_type == 'vmp':
                query |= Q(code=code)
            elif item_type == 'vtm':
                query |= Q(vtm__vtm=code)
            elif item_type == 'ingredient':
                query |= Q(ingredients__code=code)
            elif item_type == 'atc':
                query |= Q(atcs__code__startswith=code)
        except (KeyError, TypeError):
            raise ValueError(f"Invalid search item format: {item}")
    
    return set(VMP.objects.filter(query).values_list('id', flat=True))

def build_product_details(vmp_ids):
    """Build detailed product information for given VMP IDs."""
    vmps = VMP.objects.filter(id__in=vmp_ids).select_related('vtm').prefetch_related(
        'ingredients',
        'ddds__who_route', 
        'ont_form_routes',
        'who_routes',
        'atcs',
        'ingredient_strengths__ingredient',
        'calculation_logic__ingredient'
    )
    
    quantity_data = get_quantity_data_batch(vmp_ids)
    
    products = []
    for vmp in vmps:
        product_data = build_single_product_data(vmp, quantity_data.get(vmp.id, {}))
        products.append(product_data)
    
    return products

def get_quantity_data_batch(vmp_ids):
    """Batch fetch all quantity data"""
    quantity_data = {}
    
    for vmp_id in vmp_ids:
        quantity_data[vmp_id] = {
            'has_scmd_quantity': False,
            'scmd_units': [],
            'has_dose': False,
            'dose_units': [],
            'has_ingredient_quantities': False,
            'ingredient_units': [],
            'has_ddd_quantity': False
        }
    
    scmd_quantities = SCMDQuantity.objects.filter(
        vmp_id__in=vmp_ids
    ).values('vmp_id', 'data')
    
    for scmd in scmd_quantities:
        vmp_id = scmd['vmp_id']
        if scmd['data']:
            has_valid_data = any(
                entry and len(entry) >= 1 and entry[1] is not None 
                for entry in scmd['data']
            )
            if has_valid_data:
                quantity_data[vmp_id]['has_scmd_quantity'] = True
                
                units_set = set()
                for entry in scmd['data']:
                    if entry and len(entry) >= 3 and entry[2]:
                        units_set.add(entry[2])
                quantity_data[vmp_id]['scmd_units'] = sorted(list(units_set))
    
    dose_quantities = Dose.objects.filter(
        vmp_id__in=vmp_ids,
        data__0__0__isnull=False
    ).values('vmp_id', 'data')
    
    for dose in dose_quantities:
        vmp_id = dose['vmp_id']
        quantity_data[vmp_id]['has_dose'] = True
        
        if dose['data']:
            units_set = set()
            for entry in dose['data']:
                if len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            quantity_data[vmp_id]['dose_units'] = sorted(list(units_set))
    
    ingredient_quantities = IngredientQuantity.objects.filter(
        vmp_id__in=vmp_ids,
        data__0__0__isnull=False
    ).values('vmp_id', 'data')
    
    for ingredient_qty in ingredient_quantities:
        vmp_id = ingredient_qty['vmp_id']
        quantity_data[vmp_id]['has_ingredient_quantities'] = True
        
        if ingredient_qty['data']:
            units_set = set()
            for entry in ingredient_qty['data']:
                if len(entry) >= 3 and entry[2]:
                    units_set.add(entry[2])
            quantity_data[vmp_id]['ingredient_units'] = sorted(list(units_set))
    
    ddd_quantities = DDDQuantity.objects.filter(
        vmp_id__in=vmp_ids,
        data__0__0__isnull=False
    ).values_list('vmp_id', flat=True)
    
    for vmp_id in ddd_quantities:
        quantity_data[vmp_id]['has_ddd_quantity'] = True
    
    return quantity_data

def build_single_product_data(vmp, quantity_data):
    """Build detailed data for a single VMP."""
    ingredient_logic_map = {}

    for calc_logic in vmp.calculation_logic.filter(logic_type='ingredient'):
        if calc_logic.ingredient:
            strength_info = None
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
            
            ingredient_logic_map[calc_logic.ingredient.id] = {
                'ingredient': calc_logic.ingredient.name,
                'logic': calc_logic.logic,
                'strength_info': strength_info
            }
        else:
            ingredient_logic_map['no_ingredients'] = {
                'ingredient': None,
                'logic': calc_logic.logic,
                'strength_info': None
            }

    ingredient_logic = []
    ingredient_names_list = []
    
    if vmp.ingredients.exists():
        for ingredient in vmp.ingredients.all():
            ingredient_names_list.append(ingredient.name)
            
            if ingredient.id in ingredient_logic_map:
                ingredient_logic.append(ingredient_logic_map[ingredient.id])
            else:
                strength_info = None
                strength = vmp.ingredient_strengths.filter(ingredient=ingredient).first()
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
                    'ingredient': ingredient.name,
                    'logic': None,
                    'strength_info': strength_info
                })
    else:
        if 'no_ingredients' in ingredient_logic_map:
            ingredient_logic.append(ingredient_logic_map['no_ingredients'])

    dose_logic = None
    ddd_logic = None
    
    for calc_logic in vmp.calculation_logic.all():
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

    ddd_values = []
    for ddd in vmp.ddds.all():
        ddd_values.append({
            'value': ddd.ddd,
            'unit': ddd.unit_type,
            'route': ddd.who_route.name
        })

    ddd_info = None
    if ddd_values:
        if len(ddd_values) == 1:
            ddd = ddd_values[0]
            ddd_info = f"{ddd['value']} {ddd['unit']}"
        else:
            ddd_strings = []
            for ddd in ddd_values:
                ddd_strings.append(f"{ddd['value']} {ddd['unit']}")
            ddd_info = " | ".join(ddd_strings)

    return {
        'vmp_name': vmp.name,
        'vmp_code': vmp.code,
        'vtm_name': vmp.vtm.name if vmp.vtm else None,
        'vtm_code': vmp.vtm.vtm if vmp.vtm else None,
        'routes': [route.name for route in vmp.ont_form_routes.all()],
        'who_routes': [route.name for route in vmp.who_routes.all()] if ddd_logic else [],
        'atc_codes': [atc.code for atc in vmp.atcs.all()],
        'ingredient_names': ", ".join(ingredient_names_list),
        'ddd_info': ddd_info,
        'df_ind': vmp.df_ind,
        'has_scmd_quantity': quantity_data.get('has_scmd_quantity', False),
        'scmd_units': quantity_data.get('scmd_units', []),
        'has_dose': quantity_data.get('has_dose', False),
        'dose_units': quantity_data.get('dose_units', []),
        'has_ddd_quantity': quantity_data.get('has_ddd_quantity', False),
        'has_ingredient_quantities': quantity_data.get('has_ingredient_quantities', False),
        'ingredient_units': quantity_data.get('ingredient_units', []),
        'dose_logic': dose_logic,
        'ddd_logic': ddd_logic,
        'ingredient_logic': ingredient_logic
    }



def validate_and_sanitize_codes(codes_list, max_length, code_type, regex_pattern=None, numeric_only=False, errors=None):

    """Validate and sanitize a list of codes"""
    if errors is None:
        errors = []

    sanitized = []
    for code in codes_list:
        code = code.strip()
        if not code:
            continue

        if len(code) > max_length:
            errors.append(f"{code_type} code '{code}' exceeds maximum length of {max_length} characters")
            continue

        if numeric_only and not code.isdigit():
            errors.append(f"{code_type} code '{code}' must be numeric")
            continue

        if regex_pattern and not re.match(regex_pattern, code):
            errors.append(f"{code_type} code '{code}' does not match required format")
            continue

        sanitized.append(code)

    return sanitized

@csrf_protect
@api_view(["GET"])
def validate_analysis_params(request):
    """
    Validate URL parameters for analysis and return enriched data for UI population.

    Expected query parameters:
    - vmps: comma-separated VMP codes
    - vtms: comma-separated VTM codes
    - ingredients: comma-separated ingredient codes
    - atcs: comma-separated ATC codes
    - trusts: comma-separated trust ODS codes
    - quantity: quantity type code (scmd, dose, ingredient, DDD)

    Returns:
    - valid_products: List of valid product objects with names and types
    - valid_trusts: List of valid trust objects with names
    - quantity_type: Validated quantity type with display name
    - errors: List of validation errors
    """

    errors = []
    valid_products = []
    valid_trusts = []
    quantity_type = None
    vmp_ids = set()
    available_vmp_codes = set()
    mode = None
    show_percentiles = False
    excluded_vmps = []

    ATC_REGEX = r'^[A-Z](?:[0-9]{2})?[A-Z]?[A-Z]?(?:[0-9]{2})?$'

    product_params = {
        'vmps': {'max_length': 20, 'type': 'VMP', 'numeric_only': True},
        'vtms': {'max_length': 20, 'type': 'VTM', 'numeric_only': True},
        'ingredients': {'max_length': 20, 'type': 'Ingredient', 'numeric_only': True},
        'atcs': {'max_length': 7, 'type': 'ATC', 'regex': ATC_REGEX}
    }

    product_codes = {}

    for param, config in product_params.items():
        codes_str = request.GET.get(param, '')
        if codes_str:
            raw_codes = [code.strip() for code in codes_str.split(',') if code.strip()]
            validated_codes = validate_and_sanitize_codes(
                raw_codes,
                config['max_length'],
                config['type'],
                config.get('regex'),
                config.get('numeric_only', False),
                errors
            )
            if validated_codes:
                product_codes[param] = validated_codes

    if not product_codes:
        errors.append("At least one product parameter (vmps, vtms, ingredients, or atcs) must be provided")

    if product_codes:
        vmp_ids = set()
        invalid_codes_by_type = {}

        for param, codes in product_codes.items():
            if param == 'vmps':
                vmps_qs = VMP.objects.filter(code__in=codes).values('id', 'code', 'name')
                vmps = list(vmps_qs)
                found_codes = {vmp['code'] for vmp in vmps}
                invalid_codes = set(codes) - found_codes
                if invalid_codes:
                    invalid_codes_by_type['vmps'] = list(invalid_codes)

                for vmp in vmps:
                    vmp_ids.add(vmp['id'])
                    available_vmp_codes.add(str(vmp['code']))
                    valid_products.append({
                        'code': vmp['code'],
                        'name': vmp['name'],
                        'type': 'vmp',
                        'label': vmp['name']
                    })
            elif param == 'vtms':
                vtms_qs = VTM.objects.filter(vtm__in=codes).values('vtm', 'name')
                vtms = list(vtms_qs)
                found_codes = {vtm['vtm'] for vtm in vtms}
                invalid_codes = set(codes) - found_codes
                if invalid_codes:
                    invalid_codes_by_type['vtms'] = list(invalid_codes)

                for vtm in vtms:
                    vtm_vmps_qs = VMP.objects.filter(vtm__vtm=vtm['vtm']).values('id', 'code', 'name')
                    vtm_vmps_list = list(vtm_vmps_qs)
                    for vmp in vtm_vmps_list:
                        vmp_ids.add(vmp['id'])
                        available_vmp_codes.add(str(vmp['code']))
                    vtm_vmps = [{'code': v['code'], 'name': v['name']} for v in vtm_vmps_list]
                    valid_products.append({
                        'code': vtm['vtm'],
                        'name': vtm['name'],
                        'type': 'vtm',
                        'label': vtm['name'],
                        'vmps': vtm_vmps
                    })
            elif param == 'ingredients':
                ingredients_qs = Ingredient.objects.filter(code__in=codes).values('code', 'name')
                ingredients = list(ingredients_qs)
                found_codes = {ingredient['code'] for ingredient in ingredients}
                invalid_codes = set(codes) - found_codes
                if invalid_codes:
                    invalid_codes_by_type['ingredients'] = list(invalid_codes)

                for ingredient in ingredients:
                    ingredient_vmps_qs = VMP.objects.filter(ingredients__code=ingredient['code']).values('id', 'code', 'name')
                    ingredient_vmps_list = list(ingredient_vmps_qs)
                    for vmp in ingredient_vmps_list:
                        vmp_ids.add(vmp['id'])
                        available_vmp_codes.add(str(vmp['code']))
                    ingredient_vmps = [{'code': v['code'], 'name': v['name']} for v in ingredient_vmps_list]
                    valid_products.append({
                        'code': ingredient['code'],
                        'name': ingredient['name'],
                        'type': 'ingredient',
                        'label': ingredient['name'],
                        'vmps': ingredient_vmps
                    })
            elif param == 'atcs':
                atcs_qs = ATC.objects.filter(code__in=codes).values('code', 'name')
                atcs = list(atcs_qs)
                found_codes = {atc['code'] for atc in atcs}
                invalid_codes = set(codes) - found_codes
                if invalid_codes:
                    invalid_codes_by_type['atcs'] = list(invalid_codes)

                for atc in atcs:
                    atc_vmps_qs = VMP.objects.filter(atcs__code__startswith=atc['code']).values('id', 'code', 'name')
                    atc_vmps_list = list(atc_vmps_qs)
                    for vmp in atc_vmps_list:
                        vmp_ids.add(vmp['id'])
                        available_vmp_codes.add(str(vmp['code']))
                    atc_vmps = [{'code': v['code'], 'name': v['name']} for v in atc_vmps_list]
                    valid_products.append({
                        'code': atc['code'],
                        'name': atc['name'],
                        'type': 'atc',
                        'label': atc['name'],
                        'vmps': atc_vmps
                    })

        for param, invalid_codes in invalid_codes_by_type.items():
            code_type = {'vmps': 'VMP', 'vtms': 'VTM', 'ingredients': 'Ingredient', 'atcs': 'ATC'}[param]
            if len(invalid_codes) == 1:
                errors.append(f"{code_type} code '{invalid_codes[0]}' not found")
            else:
                codes_list = ', '.join(f"'{code}'" for code in invalid_codes)
                errors.append(f"{code_type} codes not found: {codes_list}")

        if not valid_products:
            if not invalid_codes_by_type:
                errors.append("No valid products found for the provided codes")

    trusts_str = request.GET.get('trusts', '')
    if trusts_str:
        raw_trust_codes = [code.strip() for code in trusts_str.split(',') if code.strip()]
        if raw_trust_codes and raw_trust_codes != ['all']:
            validated_trust_codes = validate_and_sanitize_codes(raw_trust_codes, 3, 'Trust', errors=errors)
            if validated_trust_codes:
                valid_trusts = Organisation.objects.filter(ods_code__in=validated_trust_codes).values('ods_code', 'ods_name')
                valid_trusts = list(valid_trusts)

                found_trust_codes = {trust['ods_code'] for trust in valid_trusts}
                invalid_trust_codes = set(validated_trust_codes) - found_trust_codes

                if invalid_trust_codes:
                    if len(invalid_trust_codes) == 1:
                        errors.append(f"Trust code '{list(invalid_trust_codes)[0]}' not found")
                    else:
                        codes_list = ', '.join(f"'{code}'" for code in invalid_trust_codes)
                        errors.append(f"Trust codes not found: {codes_list}")

    MAX_TRUST_SELECTION_LIMIT = 10
    if len(valid_trusts) > MAX_TRUST_SELECTION_LIMIT:
        valid_trusts = valid_trusts[:MAX_TRUST_SELECTION_LIMIT]
        errors.append(f"Maximum of {MAX_TRUST_SELECTION_LIMIT} trusts allowed")

    quantity_code = request.GET.get('quantity', '').strip().lower()
    if quantity_code:
        quantity_map = {
            'scmd': 'SCMD Quantity',
            'dose': 'Unit Dose Quantity',
            'ingredient': 'Ingredient Quantity',
            'ddd': 'Defined Daily Dose Quantity'
        }

        if quantity_code in quantity_map:
            quantity_type = {
                'code': quantity_code,
                'name': quantity_map[quantity_code]
            }
        else:
            errors.append(f"Invalid quantity type: {quantity_code}")

    mode_param = request.GET.get('mode', '').strip()
    if mode_param:
        allowed_modes = {
            'trust': 'trust',
            'region': 'region',
            'icb': 'icb',
            'national': 'national',
            'product': 'product',
            'product_group': 'productGroup',
            'ingredient': 'ingredient',
            'unit': 'unit'
        }
        normalized_mode_key = re.sub(r'[-\s]+', '_', mode_param.lower())
        canonical_mode = allowed_modes.get(normalized_mode_key)
        if canonical_mode:
            mode = canonical_mode
        else:
            errors.append(f"Invalid mode: {mode_param}")

    show_percentiles_param = request.GET.get('show_percentiles', '').strip().lower()
    if show_percentiles_param:
        if show_percentiles_param not in {'true', 'false'}:
            errors.append(f"Invalid show_percentiles value: {show_percentiles_param}")
        else:
            show_percentiles = show_percentiles_param == 'true'

            if show_percentiles:
                if mode != 'trust':
                    errors.append("show_percentiles can only be true when mode is set to 'trust'")
                    show_percentiles = False
                elif not valid_trusts:
                    errors.append("show_percentiles can only be true when at least one valid trust is selected")
                    show_percentiles = False

    excluded_vmps_param = request.GET.get('excluded_vmps', '').strip()
    if excluded_vmps_param:
        raw_excluded_codes = [code.strip() for code in excluded_vmps_param.split(',') if code.strip()]
        validated_excluded_codes = validate_and_sanitize_codes(
            raw_excluded_codes,
            30,
            'Excluded VMP',
            numeric_only=True,
            errors=errors
        )
        validated_excluded_codes = list(dict.fromkeys(validated_excluded_codes))

        available_code_strings = {str(code) for code in available_vmp_codes}
        invalid_excluded_codes = [code for code in validated_excluded_codes if code not in available_code_strings]

        if invalid_excluded_codes:
            if len(invalid_excluded_codes) == 1:
                errors.append(f"Excluded VMP code '{invalid_excluded_codes[0]}' is not part of the selected products")
            else:
                codes_list = ', '.join(f"'{code}'" for code in invalid_excluded_codes)
                errors.append(f"Excluded VMP codes are not part of the selected products: {codes_list}")

        excluded_vmps = sorted({code for code in validated_excluded_codes if code in available_code_strings})

    # Remove duplicates from valid_products
    seen_codes = set()
    unique_products = []
    for product in valid_products:
        key = (product['code'], product['type'])
        if key not in seen_codes:
            seen_codes.add(key)
            unique_products.append(product)
    valid_products = unique_products

    # this is number of items selected, which may represent more than 20 products
    MAX_PRODUCT_SELECTION_LIMIT = 20
    original_count = len(valid_products)

    if original_count > MAX_PRODUCT_SELECTION_LIMIT:
        valid_products = valid_products[:MAX_PRODUCT_SELECTION_LIMIT]
        errors.append(f"Maximum of {MAX_PRODUCT_SELECTION_LIMIT} items allowed")

    return Response({
        'valid_products': valid_products,
        'valid_trusts': valid_trusts,
        'quantity_type': quantity_type,
        'mode': mode,
        'show_percentiles': show_percentiles,
        'excluded_vmps': excluded_vmps,
        'errors': errors,
        'vmp_count': len(vmp_ids)
    })
