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
    ATC
)
from ..utils import safe_float


@api_view(["GET"])
def search_products(request):
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
        ingredients = Ingredient.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).distinct().order_by('name')[:50]
        
        results = []
        for ingredient in ingredients:
            vmp_count = VMP.objects.filter(ingredients__code=ingredient.code).count()
            
            results.append({
                'code': ingredient.code,
                'name': ingredient.name,
                'type': 'ingredient',
                'vmp_count': vmp_count
            })
        
        return JsonResponse({'results': results})
    
    elif search_type == 'atc':
        matching_atcs = ATC.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).order_by('code')[:50]
        
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
        
        for atc in matching_atcs:
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
            
            # Get VMP count for this ATC code and all its children
            vmp_count = atc.get_vmps().count()

            hierarchy_path = build_hierarchy_path(atc)

            parent_codes = get_parent_path(atc.code)
            parent_path = []
            if parent_codes:
                parent_atcs = ATC.objects.filter(code__in=parent_codes).order_by('code')
                for parent in parent_atcs:
                    if parent.code != atc.code:
                        parent_hierarchy = build_hierarchy_path(parent)
                        if parent_hierarchy:
                            parent_path.append({
                                'code': parent.code,
                                'name': parent_hierarchy[-1],
                                'level': len(parent.code) if len(parent.code) <= 5 else 5
                            })
            
            results.append({
                'code': atc.code,
                'name': level_name or atc.name,
                'full_name': atc.name,
                'type': 'atc',
                'level': level,
                'vmp_count': vmp_count,
                'hierarchy_path': hierarchy_path,
                'parent_path': parent_path,
                'display_name': f"{level_name or atc.name} ({atc.code})"
            })

        return JsonResponse({'results': results})

    return JsonResponse({'results': []})


@api_view(["POST"])
def vmp_count(request):
    search_items = request.data.get("names", [])
    
    vmp_codes = set()
    display_names = {}
    vmp_details = {}

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
            ingredient_vmps = VMP.objects.filter(ingredients__code=code).select_related('vtm')
            vmp_list = []
            for vmp in ingredient_vmps:
                vmp_codes.add(vmp.code)
                vmp_list.append({
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp'
                })
            
            ingredient = Ingredient.objects.filter(code=code).first()
            if ingredient:
                display_names[f"{code}|ingredient"] = f"{ingredient.name} ({code})"
                vmp_details[f"{code}|ingredient"] = vmp_list
        
        elif item_type == 'atc':
            # Get all VMPs with ATC codes that start with this code
            atc_vmps = VMP.objects.filter(atcs__code__startswith=code).select_related('vtm')
            vmp_list = []
            for vmp in atc_vmps:
                vmp_codes.add(vmp.code)
                vmp_list.append({
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp'
                })
            
            atc = ATC.objects.filter(code=code).first()
            if atc:
                display_names[f"{code}|atc"] = f"{atc.name} ({code})"
                vmp_details[f"{code}|atc"] = vmp_list

    # Count unique VMPs
    vmp_count = len(vmp_codes)
    
    return Response({
        "vmp_count": vmp_count,
        "display_names": display_names,
        "vmp_details": vmp_details
    })

@csrf_protect
@api_view(["POST"])
def get_quantity_data(request):
    search_items = request.data.get("names", None)
    ods_names = request.data.get("ods_names", None)
    quantity_type = request.data.get("quantity_type", None)
    
    if not all([search_items, quantity_type]) or quantity_type == '--':
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
            elif item_type == 'atc':
                query |= Q(atcs__code__startswith=code)
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
                    'organisation__region': item.organisation.region,
                    'organisation__icb': item.organisation.icb,
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
            elif item_type == 'atc':
                query |= Q(atcs__code__startswith=code)
        except ValueError:
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
