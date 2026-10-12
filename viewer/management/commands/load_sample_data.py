import csv
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from viewer.management.commands.update_org_submission_cache import (
    update_org_submission_cache,
)
from viewer.models import (
    AMP,
    ATC,
    CalculationLogic,
    CancerAlliance,
    DDD,
    DDDQuantity,
    DataStatus,
    Dose,
    ICB,
    Ingredient,
    IngredientQuantity,
    IngredientQuantityUnit,
    IndicativeCost,
    Measure,
    OntFormRoute,
    Organisation,
    PrecomputedMeasureAggregated,
    PrecomputedPercentile,
    Region,
    SCMDQuantity,
    TrustAdmission,
    TrustType,
    VMP,
    VMPIngredientStrength,
    VMPQuantityUnit,
    VTM,
    WHORoute,
)

SAMPLE_DIR = Path(__file__).resolve().parents[3] / "sample_data"

# These published measures select products that are in the sample extract.
SAMPLE_MEASURES = (
    "paracetamol_iv",
    "abx_iv",
)


def _rows(filename):
    path = SAMPLE_DIR / filename
    if not path.is_file():
        raise CommandError(f"Sample file {path} is missing.")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _text(value):
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return text


def _float(value):
    text = _text(value)
    if text is None:
        return None
    return float(text)


def _bool(value):
    text = _text(value)
    if text is None:
        return False
    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise CommandError(f"Sample value {value!r} is not true or false.")


def _date(value):
    text = _text(value)
    if text is None:
        raise CommandError("A sample row is missing a date.")
    return datetime.strptime(text, "%Y-%m-%d").date()


def _pipes(value):
    text = _text(value)
    if text is None:
        return []
    return [part for part in text.split("|") if part]


def _lookup(mapping, key, label):
    if key not in mapping:
        raise CommandError(f"{label} {key} is not in the sample files.")
    return mapping[key]


def _series(rows, key_names, month_name, value_name, months, unit_name=None):
    month_index = {month: index for index, month in enumerate(months)}
    width = len(months)
    grouped = {}
    for row in rows:
        value = _float(row[value_name])
        if value is None:
            continue
        month = _date(row[month_name])
        if month not in month_index:
            raise CommandError(
                f"Month {month.isoformat()} is not in data_status.csv."
            )
        key = tuple(_text(row[name]) for name in key_names)
        missing = [name for name, part in zip(key_names, key) if part is None]
        if missing:
            raise CommandError(f"A sample row is missing {', '.join(missing)}.")
        entry = grouped.get(key)
        if entry is None:
            entry = {"data": [0.0] * width, "unit": None}
            grouped[key] = entry
        entry["data"][month_index[month]] = value
        if unit_name and entry["unit"] is None:
            entry["unit"] = _text(row[unit_name])
    return grouped


def _clear_sample_tables():
    """Delete the tables this command fills, then the loaders insert the extract.

    Quantity rows protect their unit rows, so the quantity tables go first.
    """
    for model in (
        PrecomputedMeasureAggregated,
        PrecomputedPercentile,
        SCMDQuantity,
        Dose,
        IngredientQuantity,
        DDDQuantity,
        IndicativeCost,
        TrustAdmission,
        VMP,
        AMP,
        ATC,
        OntFormRoute,
        WHORoute,
        Ingredient,
        VTM,
        Organisation,
        ICB,
        Region,
        TrustType,
        CancerAlliance,
        DataStatus,
    ):
        model.objects.all().delete()


class Command(BaseCommand):
    help = "Replace the local database tables with the BigQuery sample extract."

    def handle(self, *args, **options):
        if not settings.DEBUG:
            raise CommandError(
                "load_sample_data replaces the database tables. "
                "Run it only when DEBUG is true."
            )
        with transaction.atomic():
            _clear_sample_tables()
            months = self._load_months()
            who_routes = self._load_who_routes()
            atcs = self._load_atcs()
            ingredients = self._load_ingredients()
            vtms = self._load_vtms()
            ont_form_routes = self._load_ont_form_routes(who_routes)
            amps = self._load_amps()
            vmps = self._load_vmps(
                vtms, who_routes, atcs, ingredients, ont_form_routes, amps
            )
            self._load_strengths(vmps, ingredients)
            self._load_ddds(vmps, who_routes)
            self._load_logic(vmps, ingredients)
            orgs = self._load_organisations()
            self._load_quantities(months, vmps, orgs, ingredients)
            self._load_admissions(orgs)
            self._import_and_compute_measures()
        self.stdout.write(self.style.SUCCESS("Loaded the sample extract."))

    def _import_and_compute_measures(self):
        update_org_submission_cache()
        for slug in SAMPLE_MEASURES:
            call_command("import_measures", slug)
            if not Measure.objects.filter(slug=slug).exists():
                raise CommandError(f"Measure {slug} did not import.")
            call_command("get_measure_vmps", slug)
            call_command("compute_measures", slug)

    def _load_months(self):
        months = []
        rows = []
        for row in _rows("data_status.csv"):
            month = _date(row["year_month"])
            months.append(month)
            rows.append(DataStatus(year_month=month, file_type=row["file_type"]))
        DataStatus.objects.bulk_create(rows)
        return months

    def _load_who_routes(self):
        routes = {}
        for row in _rows("who_routes.csv"):
            code = _text(row["who_route_code"])
            routes[code] = WHORoute(
                code=code,
                name=_text(row["who_route_description"]),
            )
        WHORoute.objects.bulk_create(routes.values())
        return routes

    def _load_atcs(self):
        atcs = {}
        for row in _rows("atc.csv"):
            code = _text(row["atc_code"])
            atcs[code] = ATC(
                code=code,
                name=_text(row["atc_name"]),
                level_1=_text(row["anatomical_main_group"]),
                level_2=_text(row["therapeutic_subgroup"]),
                level_3=_text(row["pharmacological_subgroup"]),
                level_4=_text(row["chemical_subgroup"]),
                level_5=_text(row["chemical_substance"]),
            )
        ATC.objects.bulk_create(atcs.values())
        return atcs

    def _load_ingredients(self):
        ingredients = {}
        for row in _rows("product_ingredients.csv"):
            code = _text(row["ingredient_code"])
            if code in ingredients:
                continue
            ingredients[code] = Ingredient(
                code=code,
                name=_text(row["ingredient_name"]),
            )
        Ingredient.objects.bulk_create(ingredients.values())
        return ingredients

    def _load_vtms(self):
        vtms = {}
        for row in _rows("products.csv"):
            code = _text(row["vtm_code"])
            if code in vtms:
                continue
            vtms[code] = VTM(vtm=code, name=_text(row["vtm_name"]))
        VTM.objects.bulk_create(vtms.values())
        return vtms

    def _load_ont_form_routes(self, who_routes):
        mapping = {
            _text(row["dmd_ontformroute"]): _text(row["who_route"])
            for row in _rows("route_mapping.csv")
        }
        routes = {}
        for row in _rows("product_routes.csv"):
            name = _text(row["route_name"])
            if name in routes:
                continue
            who_code = mapping.get(name)
            who_route = who_routes.get(who_code) if who_code else None
            routes[name] = OntFormRoute(name=name, who_route=who_route)
        OntFormRoute.objects.bulk_create(routes.values())
        return routes

    def _load_amps(self):
        amps = {}
        for row in _rows("product_amps.csv"):
            code = _text(row["amp_code"])
            amps[code] = AMP(
                code=code,
                name=_text(row["amp_name"]),
                avail_restrict=_text(row["avail_restrict"]),
            )
        AMP.objects.bulk_create(amps.values())
        return amps

    def _load_vmps(self, vtms, who_routes, atcs, ingredients, ont_form_routes, amps):
        product_atcs = {}
        for row in _rows("product_atc.csv"):
            product_atcs.setdefault(_text(row["vmp_code"]), []).append(
                _lookup(atcs, _text(row["atc_code"]), "ATC")
            )

        product_ingredients = {}
        for row in _rows("product_ingredients.csv"):
            product_ingredients.setdefault(_text(row["vmp_code"]), []).append(
                _lookup(ingredients, _text(row["ingredient_code"]), "Ingredient")
            )

        product_routes = {}
        for row in _rows("product_routes.csv"):
            product_routes.setdefault(_text(row["vmp_code"]), []).append(
                _lookup(ont_form_routes, _text(row["route_name"]), "Route")
            )

        product_amps = {}
        for row in _rows("product_amps.csv"):
            product_amps.setdefault(_text(row["vmp_code"]), []).append(
                _lookup(amps, _text(row["amp_code"]), "AMP")
            )

        product_rows = _rows("products.csv")
        vmps = {}
        for row in product_rows:
            code = _text(row["vmp_code"])
            vmps[code] = VMP(
                code=code,
                name=_text(row["vmp_name"]),
                vtm=_lookup(vtms, _text(row["vtm_code"]), "VTM"),
                bnf_code=_text(row["bnf_code"]),
                df_ind=_text(row["df_ind"]) or "Not applicable",
                udfs=_float(row["udfs"]),
                udfs_uom=_text(row["udfs_uom"]),
                unit_dose_uom=_text(row["unit_dose_uom"]),
                special=_bool(row["special"]),
            )
        VMP.objects.bulk_create(vmps.values())
        for row in product_rows:
            code = _text(row["vmp_code"])
            vmp = vmps[code]
            vmp.ingredients.set(product_ingredients.get(code, []))
            vmp.atcs.set(product_atcs.get(code, []))
            vmp.ont_form_routes.set(product_routes.get(code, []))
            vmp.amps.set(product_amps.get(code, []))
            vmp.who_routes.set(
                [
                    _lookup(who_routes, route_code, "WHO route")
                    for route_code in _pipes(row["who_route_codes"])
                ]
            )
        return vmps

    def _load_strengths(self, vmps, ingredients):
        rows = []
        for row in _rows("product_ingredients.csv"):
            rows.append(
                VMPIngredientStrength(
                    vmp=_lookup(vmps, _text(row["vmp_code"]), "VMP"),
                    ingredient=_lookup(
                        ingredients, _text(row["ingredient_code"]), "Ingredient"
                    ),
                    strnt_nmrtr_val=_float(row["strnt_nmrtr_val"]),
                    strnt_nmrtr_uom_name=_text(row["strnt_nmrtr_uom_name"]),
                    strnt_dnmtr_val=_float(row["strnt_dnmtr_val"]),
                    strnt_dnmtr_uom_name=_text(row["strnt_dnmtr_uom_name"]),
                )
            )
        VMPIngredientStrength.objects.bulk_create(rows)

    def _load_ddds(self, vmps, who_routes):
        rows = []
        for row in _rows("ddd.csv"):
            rows.append(
                DDD(
                    vmp=_lookup(vmps, _text(row["vmp_code"]), "VMP"),
                    ddd=_float(row["ddd"]),
                    unit_type=_text(row["unit_type"]),
                    who_route=_lookup(who_routes, _text(row["who_route"]), "WHO route"),
                )
            )
        DDD.objects.bulk_create(rows)

    def _load_logic(self, vmps, ingredients):
        rows = []
        for row in _rows("dose_logic.csv"):
            rows.append(
                CalculationLogic(
                    vmp=_lookup(vmps, _text(row["vmp_code"]), "VMP"),
                    logic_type="dose",
                    ingredient=None,
                    logic=_text(row["logic"]),
                )
            )
        for row in _rows("ingredient_logic.csv"):
            rows.append(
                CalculationLogic(
                    vmp=_lookup(vmps, _text(row["vmp_code"]), "VMP"),
                    logic_type="ingredient",
                    ingredient=_lookup(
                        ingredients, _text(row["ingredient_code"]), "Ingredient"
                    ),
                    logic=_text(row["logic"]),
                )
            )
        CalculationLogic.objects.bulk_create(rows)

    def _load_organisations(self):
        trust_rows = _rows("trusts.csv")
        regions = {}
        icbs = {}
        trust_types = {}
        alliances = {}
        for row in trust_rows:
            region_code = _text(row["region_code"])
            if region_code and region_code not in regions:
                regions[region_code] = Region(
                    code=region_code,
                    name=_text(row["region"]),
                )
            trust_type_name = _text(row["trust_type"])
            if trust_type_name and trust_type_name not in trust_types:
                trust_types[trust_type_name] = TrustType(name=trust_type_name)
            alliance_code = _text(row["cancer_alliance_code"])
            if alliance_code and alliance_code not in alliances:
                alliances[alliance_code] = CancerAlliance(
                    code=alliance_code,
                    name=_text(row["cancer_alliance_name"]),
                )
        Region.objects.bulk_create(regions.values())
        TrustType.objects.bulk_create(trust_types.values())
        CancerAlliance.objects.bulk_create(alliances.values())
        for row in trust_rows:
            icb_code = _text(row["icb_code"])
            if icb_code and icb_code not in icbs:
                icbs[icb_code] = ICB(
                    code=icb_code,
                    name=_text(row["icb"]),
                    region=regions[_text(row["region_code"])],
                )
        ICB.objects.bulk_create(icbs.values())

        orgs = {}
        successor_codes = {}
        for row in trust_rows:
            code = _text(row["ods_code"])
            orgs[code] = Organisation(
                ods_code=code,
                ods_name=_text(row["ods_name"]),
                region=regions.get(_text(row["region_code"])),
                icb=icbs.get(_text(row["icb_code"])),
                trust_type=trust_types.get(_text(row["trust_type"])),
                cancer_alliance=alliances.get(_text(row["cancer_alliance_code"])),
                in_shelford_group=_bool(row["in_shelford_group"]),
                successor=None,
            )
            successor_list = _pipes(row["successors"])
            successor_codes[code] = successor_list[0] if successor_list else None
        Organisation.objects.bulk_create(orgs.values())

        with_successor = []
        for code, successor_code in successor_codes.items():
            successor = orgs.get(successor_code) if successor_code else None
            if successor is None:
                continue
            orgs[code].successor = successor
            with_successor.append(orgs[code])
        if with_successor:
            Organisation.objects.bulk_update(with_successor, ["successor"])
        return orgs

    def _load_quantities(self, months, vmps, orgs, ingredients):
        dose_rows = _rows("dose.csv")
        scmd_series = _series(
            dose_rows,
            ("vmp_code", "ods_code"),
            "year_month",
            "scmd_quantity",
            months,
            "scmd_basis_unit_name",
        )
        dose_series = _series(
            dose_rows,
            ("vmp_code", "ods_code"),
            "year_month",
            "dose_quantity",
            months,
            "dose_unit",
        )
        ingredient_series = _series(
            _rows("ingredient_quantity.csv"),
            ("vmp_code", "ingredient_code", "ods_code"),
            "year_month",
            "ingredient_quantity_basis",
            months,
            "ingredient_basis_unit",
        )
        ddd_series = _series(
            _rows("ddd_quantity.csv"),
            ("vmp_code", "ods_code"),
            "year_month",
            "ddd_quantity",
            months,
        )
        cost_series = _series(
            _rows("indicative_cost.csv"),
            ("vmp_code", "ods_code"),
            "year_month",
            "indicative_cost",
            months,
        )

        self._store_vmp_quantities(scmd_series, SCMDQuantity, vmps, orgs, "scmd")
        self._store_vmp_quantities(dose_series, Dose, vmps, orgs, "dose")
        self._store_ingredient_quantities(ingredient_series, vmps, orgs, ingredients)
        self._store_plain_quantities(ddd_series, vmps, orgs, DDDQuantity)
        self._store_plain_quantities(cost_series, vmps, orgs, IndicativeCost)

    def _store_vmp_quantities(self, series, quantity_model, vmps, orgs, quantity_type):
        units = {}
        quantities = []
        for (vmp_code, ods_code), entry in series.items():
            if not entry["unit"]:
                continue
            vmp = _lookup(vmps, vmp_code, "VMP")
            org = _lookup(orgs, ods_code, "Trust")
            unit_key = (quantity_type, vmp_code)
            unit = units.get(unit_key)
            if unit is None:
                unit = VMPQuantityUnit(
                    quantity_type=quantity_type,
                    vmp=vmp,
                    unit=entry["unit"],
                )
                units[unit_key] = unit
            quantities.append(
                quantity_model(
                    vmp=vmp,
                    organisation=org,
                    quantity_unit=unit,
                    data=entry["data"],
                )
            )
        VMPQuantityUnit.objects.bulk_create(units.values(), batch_size=1000)
        quantity_model.objects.bulk_create(quantities, batch_size=1000)

    def _store_ingredient_quantities(self, series, vmps, orgs, ingredients):
        units = {}
        quantities = []
        for (vmp_code, ingredient_code, ods_code), entry in series.items():
            if not entry["unit"]:
                continue
            vmp = _lookup(vmps, vmp_code, "VMP")
            ingredient = _lookup(ingredients, ingredient_code, "Ingredient")
            org = _lookup(orgs, ods_code, "Trust")
            unit_key = (vmp_code, ingredient_code)
            unit = units.get(unit_key)
            if unit is None:
                unit = IngredientQuantityUnit(
                    ingredient=ingredient,
                    vmp=vmp,
                    unit=entry["unit"],
                )
                units[unit_key] = unit
            quantities.append(
                IngredientQuantity(
                    vmp=vmp,
                    ingredient=ingredient,
                    organisation=org,
                    quantity_unit=unit,
                    data=entry["data"],
                )
            )
        IngredientQuantityUnit.objects.bulk_create(units.values(), batch_size=1000)
        IngredientQuantity.objects.bulk_create(quantities, batch_size=1000)

    def _store_plain_quantities(self, series, vmps, orgs, model):
        objects = []
        for (vmp_code, ods_code), entry in series.items():
            objects.append(
                model(
                    vmp=_lookup(vmps, vmp_code, "VMP"),
                    organisation=_lookup(orgs, ods_code, "Trust"),
                    data=entry["data"],
                )
            )
        model.objects.bulk_create(objects, batch_size=1000)

    def _load_admissions(self, orgs):
        rows = []
        for row in _rows("admissions.csv"):
            count = _float(row["count"])
            if count is None:
                continue
            rows.append(
                TrustAdmission(
                    organisation=_lookup(orgs, _text(row["ods_code"]), "Trust"),
                    period=_date(row["period"]),
                    count=int(count),
                )
            )
        TrustAdmission.objects.bulk_create(rows, batch_size=1000)
