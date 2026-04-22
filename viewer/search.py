import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache

from django.db.models import Count, Max, Q

from .models import ATC, Ingredient, VMP


MAX_PRODUCT_RESULTS = 200
MAX_INGREDIENT_RESULTS = 50
MAX_ATC_RESULTS = 50
MAX_ANALYSIS_VMP_COUNT = 250

_PUNCTUATION_RE = re.compile(r"""['".,\/#!$%\^&\*;:{}=\-_`~()]""")
_WHITESPACE_RE = re.compile(r"\s+")

# ATC codes are up to 7 characters across 5 levels:
#   len 1 -> level 1, len 3 -> 2, len 4 -> 3, len 5 -> 4, len 7 -> 5.
_ATC_LEVEL_BY_CODE_LEN = {1: 1, 3: 2, 4: 3, 5: 4, 7: 5}


def normalise_string(s):
    """Normalise for search: lowercase, strip accents/punctuation, collapse whitespace."""
    if s is None or not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = _PUNCTUATION_RE.sub("", s)
    s = _WHITESPACE_RE.sub(" ", s)
    return s.strip()


def tokenize(s):
    """Normalise then split on whitespace.
    Returns list of non-empty tokens."""
    normalised = normalise_string(s)
    return normalised.split() if normalised else []


def prepare_search_query(raw_term):
    """Return (normalised, tokens)"""
    return normalise_string(raw_term), tokenize(raw_term)


def _matches_all_tokens(search_string, tokens):
    return all(token in search_string for token in tokens)


@dataclass(frozen=True)
class _VMPRow:
    """VMP with normalised VTM/ingredient text"""
    id: int
    code: str
    name: str
    code_norm: str
    name_norm: str
    vtm_id: int | None
    vtm_code: str | None
    vtm_name: str | None
    vtm_code_norm: str
    vtm_name_norm: str
    ingredient_blob_norm: str

    @property
    def search_blob(self):
        return " ".join(
            part
            for part in (
                self.name_norm,
                self.code_norm,
                self.vtm_name_norm,
                self.vtm_code_norm,
                self.ingredient_blob_norm,
            )
            if part
        )

    @property
    def top_label_norm(self):
        return self.vtm_name_norm if self.vtm_id else self.name_norm


def _load_vmp_rows():
    vmps = list(
        VMP.objects.select_related("vtm")
        .prefetch_related("ingredients")
        .only("id", "code", "name", "vtm__id", "vtm__vtm", "vtm__name")
    )
    rows = []
    for vmp in vmps:
        ingredients = list(vmp.ingredients.all())
        ingredient_blob_norm = " ".join(
            normalise_string(f"{ing.name} {ing.code}") for ing in ingredients
        )
        vtm = vmp.vtm
        rows.append(
            _VMPRow(
                id=vmp.id,
                code=vmp.code,
                name=vmp.name,
                code_norm=normalise_string(vmp.code),
                name_norm=normalise_string(vmp.name),
                vtm_id=vtm.id if vtm else None,
                vtm_code=vtm.vtm if vtm else None,
                vtm_name=vtm.name if vtm else None,
                vtm_code_norm=normalise_string(vtm.vtm) if vtm else "",
                vtm_name_norm=normalise_string(vtm.name) if vtm else "",
                ingredient_blob_norm=ingredient_blob_norm,
            )
        )
    return rows


def _vmp_rows_signature():
    stats = VMP.objects.aggregate(max_id=Max("id"), count=Count("id"))
    return (stats["max_id"], stats["count"])


@lru_cache(maxsize=1)
def _load_vmp_rows_for_signature(signature):
    return _load_vmp_rows()


def _load_vmp_rows_cached():
    return _load_vmp_rows_for_signature(_vmp_rows_signature())


def _prefix_match(value, query_normalised):
    """Prefix match in either direction."""
    if not value or not query_normalised:
        return False
    return value.startswith(query_normalised) or query_normalised.startswith(value)


def _rank_key(row, query_normalised, display_name):
    """Deterministic ranking: exact/prefix on code, then on top-label, then on
    the display label, then name.

    The display-label tiers distinguish a direct prefix match on the label the
    user sees (e.g. "Morphine sulfate 100mg/100ml infusion bags" for the query
    "morphine sulfate") from an infix-only or ingredient-only match on a row
    whose display label does not start with the query (e.g. "Morphine 60mg
    modified-release tablets" matching via its ingredient blob). 
    """
    code = row.code_norm
    vtm_code = row.vtm_code_norm
    top_label = row.top_label_norm
    display_norm = normalise_string(display_name)

    exact_code = int(code == query_normalised or vtm_code == query_normalised)
    exact_top = int(top_label == query_normalised)
    prefix_code = int(
        _prefix_match(code, query_normalised)
        or _prefix_match(vtm_code, query_normalised)
    )
    prefix_top = int(_prefix_match(top_label, query_normalised))
    display_prefix = int(
        bool(display_norm) and display_norm.startswith(query_normalised)
    )
    display_contains = int(
        bool(display_norm) and query_normalised in display_norm
    )

    return (
        -exact_code,
        -exact_top,
        -prefix_code,
        -prefix_top,
        -display_prefix,
        -display_contains,
        display_norm,
    )


def search_product_results(raw_term):
    """Search for VMPs matching the given term based on combined VMP, VTM, and ingredient text."""
    normalised, tokens = prepare_search_query(raw_term)
    if len(normalised) < 3:
        return []

    all_rows = _load_vmp_rows_cached()
    matched_rows = [
        row for row in all_rows if _matches_all_tokens(row.search_blob, tokens)
    ]
    if not matched_rows:
        return []

    total_counts_by_vtm = defaultdict(int)
    for row in all_rows:
        if row.vtm_id is not None:
            total_counts_by_vtm[row.vtm_id] += 1

    matched_by_vtm = defaultdict(list)
    ranked_results = []
    for row in matched_rows:
        if row.vtm_id is not None:
            matched_by_vtm[row.vtm_id].append(row)
        else:
            ranked_results.append(
                (
                    _rank_key(row, normalised, row.name),
                    {"code": row.code, "name": row.name, "type": "vmp"},
                )
            )

    for vtm_id, rows in matched_by_vtm.items():
        ordered_rows = sorted(
            rows,
            key=lambda r: _rank_key(r, normalised, r.name),
        )
        best = ordered_rows[0]
        if len(rows) == total_counts_by_vtm[vtm_id]:
            ranked_results.append(
                (
                    _rank_key(best, normalised, best.vtm_name or ""),
                    {
                        "code": best.vtm_code,
                        "name": best.vtm_name,
                        "type": "vtm",
                        "vmps": [
                            {"code": r.code, "name": r.name, "type": "vmp"}
                            for r in ordered_rows
                        ],
                    },
                )
            )
        else:
            for row in ordered_rows:
                ranked_results.append(
                    (
                        _rank_key(row, normalised, row.name),
                        {"code": row.code, "name": row.name, "type": "vmp"},
                    )
                )

    ranked_results.sort(key=lambda entry: entry[0])
    return [item for _, item in ranked_results[:MAX_PRODUCT_RESULTS]]


def search_ingredient_results(raw_term):
    """Search for VMPs matching the given term based on ingredient name and code."""
    normalised, tokens = prepare_search_query(raw_term)
    if len(normalised) < 3:
        return []

    ingredients = list(Ingredient.objects.only("id", "code", "name"))
    matched = [
        ing
        for ing in ingredients
        if _matches_all_tokens(
            f"{normalise_string(ing.name)} {normalise_string(ing.code)}",
            tokens,
        )
    ]
    matched.sort(key=lambda ing: normalise_string(ing.name))
    matched = matched[:MAX_INGREDIENT_RESULTS]
    if not matched:
        return []

    ingredient_ids = [ing.id for ing in matched]
    vmps_by_ingredient = defaultdict(list)
    seen = defaultdict(set)
    vmp_rows = (
        VMP.objects.filter(ingredients__in=ingredient_ids)
        .values("code", "name", "ingredients__id")
        .order_by("name")
    )
    for row in vmp_rows:
        ing_id = row["ingredients__id"]
        if row["code"] in seen[ing_id]:
            continue
        seen[ing_id].add(row["code"])
        vmps_by_ingredient[ing_id].append({"code": row["code"], "name": row["name"]})

    return [
        {
            "code": ingredient.code,
            "name": ingredient.name,
            "type": "ingredient",
            "vmp_count": len(vmps_by_ingredient.get(ingredient.id, [])),
            "vmps": vmps_by_ingredient.get(ingredient.id, []),
        }
        for ingredient in matched
    ]


def _build_hierarchy_path(atc):
    """Build a list of ATC levels for the given ATC."""
    return [
        level
        for level in (atc.level_1, atc.level_2, atc.level_3, atc.level_4, atc.level_5)
        if level
    ]


def _atc_search_blob(atc):
    return " ".join(
        part
        for part in (
            normalise_string(atc.name or ""),
            normalise_string(atc.code or ""),
            normalise_string(atc.level_1 or ""),
            normalise_string(atc.level_2 or ""),
            normalise_string(atc.level_3 or ""),
            normalise_string(atc.level_4 or ""),
            normalise_string(atc.level_5 or ""),
        )
        if part
    )


def search_atc_results(raw_term):
    """ATC search: all query tokens must match on each row's name, code, and hierarchy level text.

    Each matched ATC is returned with linked VMPs. VMPs are included when their tag equals the
    matched code or a more specific descendant.
    """
    normalised, tokens = prepare_search_query(raw_term)
    if len(normalised) < 3:
        return []

    atcs = list(ATC.objects.all())
    matched = [atc for atc in atcs if _matches_all_tokens(_atc_search_blob(atc), tokens)]
    matched.sort(key=lambda atc: atc.code)
    matched = matched[:MAX_ATC_RESULTS]
    if not matched:
        return []

    atc_codes = [atc.code for atc in matched]
    startswith_q = Q()
    for ac in atc_codes:
        startswith_q |= Q(atcs__code__startswith=ac)
    vmp_rows = (
        VMP.objects.filter(startswith_q)
        .values("code", "name", "atcs__code")
        .order_by("name")
    )
    vmps_by_atc = defaultdict(list)
    seen_by_atc = defaultdict(set)
    for row in vmp_rows:
        matched_atc_code = row["atcs__code"]
        for ac in atc_codes:
            if matched_atc_code.startswith(ac) and row["code"] not in seen_by_atc[ac]:
                seen_by_atc[ac].add(row["code"])
                vmps_by_atc[ac].append({"code": row["code"], "name": row["name"]})

    results = []
    for atc in matched:
        level = _ATC_LEVEL_BY_CODE_LEN.get(len(atc.code))
        if level is None:
            continue
        level_name = getattr(atc, f"level_{level}")
        vmp_list = vmps_by_atc.get(atc.code, [])
        results.append(
            {
                "code": atc.code,
                "name": level_name or atc.name,
                "type": "atc",
                "level": level,
                "vmp_count": len(vmp_list),
                "vmps": vmp_list,
                "hierarchy_path": _build_hierarchy_path(atc),
            }
        )

    return results
