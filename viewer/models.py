from django.db import models
from django.utils.text import slugify
from django.core.validators import RegexValidator
from django.contrib.postgres.fields import ArrayField

class VTM(models.Model):
    vtm = models.CharField(max_length=20, unique=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name
    
    class Meta:
        indexes = [
            models.Index(fields=["vtm"]),
        ]

class WHORoute(models.Model):
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=255, unique=True)

    def __str__(self):
        return self.name
    
    class Meta:
        indexes = [
            models.Index(fields=["code"]),
        ]

class AMP(models.Model):
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=255)
    avail_restrict = models.CharField(
        max_length=255, 
        null=True, 
        help_text="Availability restriction description"
    )

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=["code"]),
        ]

class VMP(models.Model):
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=255)
    vtm = models.ForeignKey(
        VTM, on_delete=models.CASCADE, related_name="vmps", null=True
    )
    ingredients = models.ManyToManyField(
        "Ingredient", related_name="vmps")
    ont_form_routes = models.ManyToManyField("OntFormRoute", related_name="vmps")
    who_routes = models.ManyToManyField("WHORoute", related_name="vmps")
    atcs = models.ManyToManyField("ATC", related_name="vmps")
    amps = models.ManyToManyField("AMP", related_name="vmps")
    bnf_code = models.CharField(max_length=20, null=True)
    df_ind = models.CharField(max_length=20, null=False, default="Not applicable", help_text="Dose form indicator")
    udfs = models.FloatField(null=True, help_text="Unit dose form size")
    udfs_uom = models.CharField(max_length=100, null=True, help_text="Unit dose form size unit of measure")
    unit_dose_uom = models.CharField(max_length=100, null=True, help_text="Unit dose unit of measure")
    special = models.BooleanField(default=False, help_text="Whether this VMP is a special (unlicensed medicine)")

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=["code"]),
            models.Index(fields=["vtm"]),
        ]

class DDD(models.Model):
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name="ddds")
    ddd = models.FloatField()
    unit_type = models.CharField(max_length=255)
    who_route = models.ForeignKey(WHORoute, on_delete=models.CASCADE, related_name="ddds")

    def __str__(self):
        return f"{self.vmp.name} - {self.ddd} {self.unit_type} - {self.who_route.name}"
    
    class Meta:
        indexes = [
            models.Index(fields=["vmp"]),
            models.Index(fields=["vmp", "who_route"]),
        ]

class ATC(models.Model):
    code = models.CharField(
        max_length=7,
        unique=True,
        validators=[
            # ATC codes are up to 7 characters long. They represent up to 5 levels of hierarchy.
            # The first character is a single letter
            # The second level is a 2 character number
            # The third level is a single letter
            # The fourth level is a single letter
            # the fifth level is a 2 character number
            RegexValidator(
                regex=r'^[A-Z](?:[0-9]{2})?[A-Z]?[A-Z]?(?:[0-9]{2})?$',
                message="Invalid ATC code"
            )
        ]
    )
    name = models.CharField(max_length=255, null=True)
    level_1 = models.CharField(max_length=255, null=True)
    level_2 = models.CharField(max_length=255, null=True)
    level_3 = models.CharField(max_length=255, null=True)
    level_4 = models.CharField(max_length=255, null=True)
    level_5 = models.CharField(max_length=255, null=True)
    

    def __str__(self):
        return f"{self.name} ({self.code})"
    
    class Meta:
        indexes = [
            models.Index(fields=["code"], name="atc_code_idx"),
        ]

    def get_vmps(self):
        """
        Get VMPs associated with this ATC code or any of its more specific codes.
        For example, if this is ATC code 'A10', it will return VMPs with ATC codes
        that start with 'A10'.
        """
        return VMP.objects.filter(atcs__code__startswith=self.code).distinct()

class OntFormRoute(models.Model):
    name = models.CharField(max_length=255)
    who_route = models.ForeignKey(WHORoute, on_delete=models.CASCADE, related_name="ont_form_routes", null=True)

    def __str__(self):
        return f"{self.name}"

class Ingredient(models.Model):
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=255, null=False)

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=["code"]),
        ]

class VMPIngredientStrength(models.Model):
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name="ingredient_strengths")
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE, related_name="vmp_strengths")
    
    strnt_nmrtr_val = models.FloatField(null=True, help_text="Strength numerator value")
    strnt_nmrtr_uom_name = models.CharField(max_length=100, null=True, help_text="Strength numerator unit of measure")
    strnt_dnmtr_val = models.FloatField(null=True, help_text="Strength denominator value")
    strnt_dnmtr_uom_name = models.CharField(max_length=100, null=True, help_text="Strength denominator unit of measure")
    basis_of_strength_type = models.IntegerField(null=True, help_text="Type of basis of strength (1=Ingredient Substance, 2=Base Substance)")
    basis_of_strength_name = models.CharField(max_length=255, null=True, help_text="Name of the basis of strength substance")

    class Meta:
        unique_together = ('vmp', 'ingredient')
        indexes = [
            models.Index(fields=["vmp", "ingredient"]),
        ]

    def __str__(self):
        return f"{self.vmp.name} - {self.ingredient.name}"
class Region(models.Model):
    code = models.CharField(max_length=20, unique=True, help_text="Region code")
    name = models.CharField(max_length=255, help_text="Region name")
    
    def __str__(self):
        return f"{self.name} ({self.code})"
    
    class Meta:
        indexes = [
            models.Index(fields=["code"]),
            models.Index(fields=["name"]),
        ]

class ICB(models.Model):
    code = models.CharField(max_length=20, unique=True, help_text="ICB code")
    name = models.CharField(max_length=255, help_text="ICB name")
    region = models.ForeignKey(Region, on_delete=models.CASCADE, related_name="icbs")
    
    def __str__(self):
        return f"{self.name} ({self.code})"
    
    class Meta:
        indexes = [
            models.Index(fields=["code"]),
            models.Index(fields=["name"]),
            models.Index(fields=["region"]),
        ]

class TrustType(models.Model):
    """Trust type classification from ERIC data"""
    name = models.CharField(max_length=255, unique=True, help_text="Trust type name (e.g., ACUTE - TEACHING, COMMUNITY)")
    description = models.TextField(null=True, blank=True, help_text="Optional description of the trust type")
    
    def __str__(self):
        return self.name
    
    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]
class Organisation(models.Model):
    ods_code = models.CharField(max_length=10, unique=True)
    ods_name = models.CharField(max_length=255, null=False)
    region = models.ForeignKey(Region, on_delete=models.CASCADE, related_name="organisations", null=True)
    icb = models.ForeignKey(ICB, on_delete=models.CASCADE, related_name="organisations", null=True)
    trust_type = models.ForeignKey(
        TrustType, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        related_name="organisations",
        help_text="Trust type from ERIC data"
    )
    successor = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="predecessors",
    )

    def __str__(self):
        return f"{self.ods_name} ({self.ods_code})"

    class Meta:
        indexes = [
            models.Index(fields=["ods_name"]),
            models.Index(fields=["ods_code"]),
        ]

    def get_all_predecessor_codes(self):
        """Returns a list of ods_codes for all predecessors of this organisation"""
        return list(self.predecessors.all().values_list('ods_code', flat=True))
    
    def get_all_predecessor_names(self):
        """Returns a list of ods_names for all predecessors of this organisation"""
        return list(self.predecessors.all().values_list('ods_name', flat=True))
    
    def get_all_predecessors(self):
        """Returns a list of Organisation objects for all predecessors of this organisation"""
        return list(self.predecessors.all())


class SCMDQuantity(models.Model):
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name="scmd_quantities")
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="scmd_quantities")
    data = ArrayField(
        ArrayField(
            models.CharField(max_length=255, null=True),
            size=3,  # [year_month, quantity, unit]
        ),
        null=True,
        help_text="Array of [year_month, quantity, unit] entries"
    )
    
    class Meta:
        unique_together = ('vmp', 'organisation')
        indexes = [
            models.Index(fields=["vmp", "organisation"]),
        ]

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name}"

    def get_quantity_for_month(self, year_month):
        """Get quantity and unit for a specific month"""
        date_str = year_month.strftime('%Y-%m-%d')
        for entry in self.data:
            if entry[0] == date_str:
                return {
                    'quantity': float(entry[1]) if entry[1] else None,
                    'unit': entry[2]
                }
        return None

class Dose(models.Model):
    vmp = models.ForeignKey(
        VMP,
        on_delete=models.CASCADE,
        related_name="doses")
    organisation = models.ForeignKey(
        Organisation, on_delete=models.CASCADE, related_name="doses"
    )
    data = ArrayField(
        ArrayField(
            models.CharField(max_length=255, null=True),
            size=3,  # [year_month, quantity, unit]
        ),
        null=True,
        help_text="Array of [year_month, quantity, unit] entries"
    )

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name}"

    class Meta:
        unique_together = ('vmp', 'organisation')
        indexes = [
            models.Index(fields=["vmp", "organisation"]),
        ]

    def get_quantity_for_month(self, year_month):
        """Get quantity and unit for a specific month"""
        date_str = year_month.strftime('%Y-%m-%d')
        for entry in self.data:
            if entry[0] == date_str:
                return {
                    'quantity': float(entry[1]) if entry[1] else None,
                    'unit': entry[2]
                }
        return None

class IngredientQuantity(models.Model):
    ingredient = models.ForeignKey(
        Ingredient,
        on_delete=models.CASCADE,
        related_name="ingredient_quantities")
    vmp = models.ForeignKey(
        VMP, on_delete=models.CASCADE, related_name="ingredient_quantities"
    )
    organisation = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="ingredient_quantities")
    data = ArrayField(
        ArrayField(
            models.CharField(max_length=255, null=True),
            size=3,  # [year_month, quantity, unit]
        ),
        null=True,
        help_text="Array of [year_month, quantity, unit] entries"
    )

    def __str__(self):
        return (
            f"{self.ingredient.name} - {self.vmp.name} - "
            f"{self.organisation.ods_name}"
        )

    class Meta:
        unique_together = ('ingredient', 'vmp', 'organisation')
        indexes = [
            models.Index(fields=["vmp", "organisation", "ingredient"]),
        ]

    def get_quantity_for_month(self, year_month):
        """Get quantity and unit for a specific month"""
        date_str = year_month.strftime('%Y-%m-%d')
        for entry in self.data:
            if entry[0] == date_str:
                return {
                    'quantity': float(entry[1]) if entry[1] else None,
                    'unit': entry[2]
                }
        return None

class DDDQuantity(models.Model):
    vmp = models.ForeignKey(
        VMP, on_delete=models.CASCADE, related_name="ddd_quantities"
    )
    organisation = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="ddd_quantities")
    data = ArrayField(
        ArrayField(
            models.CharField(max_length=255, null=True),
            size=3,  # [year_month, quantity, unit]
        ),
        null=True,
        help_text="Array of [year_month, quantity, unit] entries"
    )

    def __str__(self):
        return (
            f"{self.vmp.name} - {self.organisation.ods_name}"
        )

    class Meta:
        unique_together = ('vmp', 'organisation')
        indexes = [
            models.Index(fields=["vmp", "organisation"]),
        ]

    def get_quantity_for_month(self, year_month):
        """Get quantity and unit for a specific month"""
        date_str = year_month.strftime('%Y-%m-%d')
        for entry in self.data:
            if entry[0] == date_str:
                return {
                    'quantity': float(entry[1]) if entry[1] else None,
                    'unit': entry[2]
                }
        return None


class IndicativeCost(models.Model):
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name="indicative_costs")
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="indicative_costs")
    data = ArrayField(
        ArrayField(
            models.CharField(max_length=255, null=True),
            size=2,  # [year_month, quantity]
        ),
        null=True,
        help_text="Array of [year_month, quantity] entries"
    )
    
    class Meta:
        unique_together = ('vmp', 'organisation')
        indexes = [
            models.Index(fields=["vmp", "organisation"]),
        ]

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name}"

    def get_quantity_for_month(self, year_month):
        """Get quantity and unit for a specific month"""
        date_str = year_month.strftime('%Y-%m-%d')
        for entry in self.data:
            if entry[0] == date_str:
                return {
                    'quantity': float(entry[1]) if entry[1] else None
                }
        return None
    
class MeasureTag(models.Model):
    name = models.CharField(max_length=255)
    description = models.TextField(null=True)
    colour = models.CharField(max_length=255, null=True)

    def __str__(self):
        return f"{self.name}"

class MeasureVMP(models.Model):
    TYPES = [
        ('numerator', 'Numerator'),
        ('denominator', 'Denominator'),
    ]
    
    measure = models.ForeignKey('Measure', on_delete=models.CASCADE, related_name='measure_vmps')
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name='measure_vmps')
    type = models.CharField(max_length=20, choices=TYPES)
    unit = models.CharField(max_length=50, null=True)
    
    class Meta:
        unique_together = ('measure', 'vmp', 'type')
        indexes = [
            models.Index(fields=['measure', 'type']),
            models.Index(fields=['vmp']),
        ]

    def __str__(self):
        return f"{self.measure.name} - {self.vmp.name} ({self.type})"

class Measure(models.Model):
    STATUS_CHOICES = [
        ('in_development', 'In Development'),
        ('preview', 'Preview'),
        ('published', 'Published'),
    ]
    
    QUANTITY_TYPES = [
        ('scmd', 'SCMD Quantity'),
        ('dose', 'Dose'),
        ('ingredient', 'Ingredient Quantity'),
        ('ddd', 'DDD'),
        ('indicative_cost', 'Indicative Cost'),
    ]
    
    VIEW_MODE_CHOICES = [
        ('trust', 'NHS Trust'),
        ('icb', 'ICB'),
        ('region', 'Region'),
        ('national', 'National'),
    ]
    name = models.CharField(max_length=255, unique=True)
    short_name = models.CharField(max_length=255, null=True)
    slug = models.SlugField(unique=True)
    description = models.TextField(null=True)
    why_it_matters = models.TextField()
    how_is_it_calculated = models.TextField(null=True)
    tags = models.ManyToManyField(MeasureTag, related_name="measures")
    status = models.CharField(
        max_length=20, 
        choices=STATUS_CHOICES, 
        default='in_development'
    )
    vmps = models.ManyToManyField(VMP, through='MeasureVMP', related_name='measures')
    quantity_type = models.CharField(max_length=20, choices=QUANTITY_TYPES, default='dose')
    lower_is_better = models.BooleanField(
        null=True,
        blank=True,
        help_text="True if lower values indicate better practice; null for no value judgement"
    )
    authored_by = models.CharField(max_length=255, null=True)
    checked_by = models.CharField(max_length=255, null=True)
    date_reviewed = models.DateField(null=True)
    next_review = models.DateField(null=True)
    first_published = models.DateField(null=True)
    default_view_mode = models.CharField(
        max_length=20, 
        choices=VIEW_MODE_CHOICES, 
        default='trust',
        help_text="Default view mode for this measure"
    )

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name

class PrecomputedMeasure(models.Model):
    measure = models.ForeignKey(Measure, on_delete=models.CASCADE, related_name="precomputed_measures")
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="precomputed_measures")
    month = models.DateField()
    quantity = models.FloatField(null=True)
    numerator = models.FloatField(null=True)
    denominator = models.FloatField(null=True)

    class Meta:
        unique_together = ('measure', 'organisation', 'month')
        indexes = [
            models.Index(fields=['measure', 'organisation', 'month']),
        ]

    def __str__(self):
        return f"{self.measure.name} - {self.organisation.ods_name} - {self.month}"


class OrgSubmissionCache(models.Model):
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE)
    successor = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name='successors', null=True, blank=True)
    month = models.DateField()
    has_submitted = models.BooleanField()
    vmp_count = models.IntegerField(null=True)
    quantity_count = models.IntegerField(null=True)

    class Meta:
        unique_together = ('organisation', 'month')

class PrecomputedMeasureAggregated(models.Model):
    measure = models.ForeignKey(Measure, on_delete=models.CASCADE, related_name="precomputed_measures_aggregated")
    label = models.CharField(max_length=255, null=True)
    month = models.DateField()
    quantity = models.FloatField(null=True)
    numerator = models.FloatField(null=True)
    denominator = models.FloatField(null=True)
    category = models.CharField(max_length=20, choices=[('region', 'Region'), ('icb', 'ICB'), ('national', 'National')])

    class Meta:
        unique_together = ('measure', 'category', 'label', 'month')
        indexes = [
            models.Index(fields=['measure', 'category', 'label', 'month']),
        ]

    def __str__(self):
        return f"{self.measure.name} - {self.category} - {self.label} - {self.month}"


class PrecomputedPercentile(models.Model):
    measure = models.ForeignKey(Measure, on_delete=models.CASCADE, related_name="precomputed_percentiles")
    month = models.DateField()
    percentile = models.IntegerField()
    quantity = models.FloatField(null=True)

    class Meta:
        unique_together = ('measure', 'month', 'percentile')
        indexes = [
            models.Index(fields=['measure', 'month', 'percentile']),
        ]

    def __str__(self):
        return f"{self.measure.name} - {self.month} - {self.percentile}th percentile"

class DataStatus(models.Model):
    year_month = models.DateField(unique=True)
    file_type = models.CharField(max_length=255)

class ContentCache(models.Model):
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        managed = False
        verbose_name = "Content Cache"
        verbose_name_plural = "Content Cache"

class SystemMaintenance(models.Model):
    enabled = models.BooleanField(default=False)
    started_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "System Maintenance"
        verbose_name_plural = "System Maintenance"
    
    def __str__(self):
        status = "ENABLED" if self.enabled else "DISABLED"
        return f"Maintenance Mode: {status}"
    
    @classmethod
    def get_instance(cls):
        try:
            return cls.objects.get(pk=1)
        except cls.DoesNotExist:
            return cls(enabled=False)
    
    def get_duration(self):
        """Get maintenance duration if enabled"""
        if self.enabled and self.started_at:
            from django.utils import timezone
            return timezone.now() - self.started_at
        return None

class CalculationLogic(models.Model):
    LOGIC_TYPES = [
        ('dose', 'Dose'),
        ('ingredient', 'Ingredient Quantity'),
        ('ddd', 'DDD Quantity'),
    ]
    
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name="calculation_logic")
    ingredient = models.ForeignKey(
        Ingredient, 
        on_delete=models.CASCADE, 
        null=True, 
        blank=True,
        help_text="Required for ingredient quantity logic, null for dose/DDD logic or when no ingredients exist"
    )
    logic_type = models.CharField(max_length=20, choices=LOGIC_TYPES)
    logic = models.TextField()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['vmp', 'logic_type'], 
                condition=models.Q(logic_type__in=['dose', 'ddd']),
                name='unique_vmp_logic_type'
            ),
            models.UniqueConstraint(
                fields=['vmp', 'ingredient', 'logic_type'], 
                condition=models.Q(logic_type='ingredient'),
                name='unique_vmp_ingredient_logic'
            ),
        ]

class AWAREAntibiotic(models.Model):
    """WHO AWARE antibiotic classification data"""
    name = models.CharField(max_length=255, unique=True, help_text="Antibiotic name from AWARE list")
    aware_2019 = models.CharField(max_length=50, null=True, help_text="AWARE classification for 2019")
    aware_2024 = models.CharField(max_length=50, null=True, help_text="AWARE classification for 2024")

    def __str__(self):
        return self.name

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]

class AWAREVMPMapping(models.Model):
    """Mapping between AWARE antibiotics and VMPs"""
    
    aware_antibiotic = models.ForeignKey(
        AWAREAntibiotic, 
        on_delete=models.CASCADE, 
        related_name="vmp_mappings"
    )
    vmp = models.ForeignKey(
        VMP, 
        on_delete=models.CASCADE, 
        related_name="aware_mappings"
    )
    vtm = models.ForeignKey(
        VTM, 
        on_delete=models.CASCADE, 
        related_name="aware_mappings",
        help_text="The VTM used to link antibiotic to VMP"
    )

    def __str__(self):
        return f"{self.aware_antibiotic.name} -> {self.vmp.name}"

    class Meta:
        unique_together = ('aware_antibiotic', 'vmp')
        indexes = [
            models.Index(fields=["aware_antibiotic", "vmp"]),
            models.Index(fields=["vmp"]),
            models.Index(fields=["vtm"]),
        ]

class MeasureAnnotation(models.Model):
    """Data annotations for measures - vertical lines with labels on charts"""
    measure = models.ForeignKey(
        Measure, 
        on_delete=models.CASCADE, 
        related_name="annotations"
    )
    date = models.DateField(help_text="Date when the annotation should appear")
    label = models.CharField(max_length=255, help_text="Label to display on the annotation")
    description = models.TextField(
        null=True, 
        blank=True, 
        help_text="Optional description for the annotation"
    )
    colour = models.CharField(
        max_length=7, 
        default="#DC3220", 
        help_text="Colour of the annotation line (hex code)"
    )

    class Meta:
        ordering = ['date']
        indexes = [
            models.Index(fields=['measure', 'date']),
        ]

    def __str__(self):
        return f"{self.measure.name} - {self.label} ({self.date})"
