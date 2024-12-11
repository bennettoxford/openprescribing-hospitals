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

class Route(models.Model):
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=255, unique=True)

    def save(self, *args, **kwargs):
        if not self.slug and self.name:
            self.slug = slugify(self.name)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name
    
    class Meta:
        indexes = [
            models.Index(fields=["code"]),
        ]

class VMP(models.Model):
    code = models.CharField(max_length=30, unique=True)
    name = models.CharField(max_length=255)
    form = models.CharField(max_length=255, null=True)
    vtm = models.ForeignKey(
        VTM, on_delete=models.CASCADE, related_name="vmps", null=True
    )
    ingredients = models.ManyToManyField(
        "Ingredient", related_name="vmps")
    ont_form_routes = models.ManyToManyField("OntFormRoute", related_name="vmps")
    routes = models.ManyToManyField("Route", related_name="vmps")

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=["code"]),
            models.Index(fields=["vtm"]),
        ]

class OntFormRoute(models.Model):
    name = models.CharField(max_length=255)

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

class Organisation(models.Model):
    ods_code = models.CharField(max_length=10, unique=True)
    ods_name = models.CharField(max_length=255, null=False)
    region = models.CharField(max_length=100, null=False)
    icb = models.CharField(max_length=100, null=True)
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

    def get_combined_vmp_count(self, year_month):
        """Returns the combined unique VMP count for this org and its predecessors for a given month"""
        org_codes = [self.ods_code] + self.get_all_predecessor_codes()
        return SCMDQuantity.objects.filter(
            organisation__ods_code__in=org_codes,
            year_month=year_month
        ).values('vmp').distinct().count()

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

class MeasureReason(models.Model):
    reason = models.CharField(max_length=255)
    description = models.TextField(null=True)
    colour = models.CharField(max_length=255, null=True)

    def __str__(self):
        return f"{self.reason}"

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
    QUANTITY_TYPES = [
        ('dose', 'Dose'),
        ('ingredient', 'Ingredient Quantity'),
    ]
    
    name = models.CharField(max_length=255, unique=True)
    short_name = models.CharField(max_length=255, null=True)
    slug = models.SlugField(unique=True, null=True)
    description = models.TextField(null=True)
    why_it_matters = models.TextField()
    how_is_it_calculated = models.TextField(null=True)
    sql_file = models.CharField(max_length=255)
    reason = models.ForeignKey(MeasureReason, on_delete=models.CASCADE, related_name="measures", null=True)
    draft = models.BooleanField(default=True)
    vmps = models.ManyToManyField(VMP, through='MeasureVMP', related_name='measures')
    quantity_type = models.CharField(max_length=20, choices=QUANTITY_TYPES, default='dose')

    def save(self, *args, **kwargs):
        if not self.slug and self.short_name:
            self.slug = slugify(self.short_name)
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
    year_month = models.DateField()
    file_type = models.CharField(max_length=255)
