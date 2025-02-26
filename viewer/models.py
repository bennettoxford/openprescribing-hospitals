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
    atcs = models.ManyToManyField("ATC", related_name="vmps")

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
    route = models.ForeignKey(Route, on_delete=models.CASCADE, related_name="ddds")

    def __str__(self):
        return f"{self.vmp.name} - {self.ddd} {self.unit_type} - {self.route.name}"
    
    class Meta:
        indexes = [
            models.Index(fields=["vmp"]),
            models.Index(fields=["vmp", "route"]),
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
                regex=r'^[A-Z][0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{2}$',
                message="Invalid ATC code"
            )
        ]
    )
    name = models.CharField(max_length=255, null=True)
    level = models.IntegerField(null=True)
    parent = models.ForeignKey('self', null=True, blank=True, on_delete=models.CASCADE, related_name='children')

    def __str__(self):
        return f"{self.name} ({self.code})"
    
    def save(self, *args, **kwargs):
        # First deduce the level from the code length
        if len(self.code) == 7:
            self.level = 5
            parent_code = self.code[:5]
        elif len(self.code) == 5:
            self.level = 4
            parent_code = self.code[:3]
        elif len(self.code) == 3:
            self.level = 3
            parent_code = self.code[:2]
        elif len(self.code) == 2:
            self.level = 2
            parent_code = None
        
        # If we have a parent code, try to find the parent
        if parent_code:
            try:
                self.parent = ATC.objects.get(code=parent_code)
            except ATC.DoesNotExist:
                # Handle the case where parent doesn't exist
                self.parent = None
        else:
            self.parent = None
        
        super().save(*args, **kwargs)
    
    class Meta:
        indexes = [
            models.Index(fields=["code"]),
        ]

    def get_vmps(self, include_children=True):
        """
        Get VMPs associated with this ATC code.
        If include_children is True, also gets VMPs from child ATC codes.
        """
        if include_children:
            return VMP.objects.filter(atcs__code__startswith=self.code).distinct()
        return VMP.objects.filter(atcs__code=self.code).distinct()


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
    QUANTITY_TYPES = [
        ('dose', 'Dose'),
        ('ingredient', 'Ingredient Quantity'),
        ('ddd', 'DDD'),
    ]
    
    name = models.CharField(max_length=255, unique=True)
    short_name = models.CharField(max_length=255, null=True)
    slug = models.SlugField(unique=True)
    description = models.TextField(null=True)
    why_it_matters = models.TextField()
    how_is_it_calculated = models.TextField(null=True)
    tags = models.ManyToManyField(MeasureTag, related_name="measures")
    draft = models.BooleanField(default=True)
    vmps = models.ManyToManyField(VMP, through='MeasureVMP', related_name='measures')
    quantity_type = models.CharField(max_length=20, choices=QUANTITY_TYPES, default='dose')
    authored_by = models.CharField(max_length=255, null=True)
    checked_by = models.CharField(max_length=255, null=True)
    date_reviewed = models.DateField(null=True)
    next_review = models.DateField(null=True)

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
    year_month = models.DateField()
    file_type = models.CharField(max_length=255)

class ContentCache(models.Model):
    last_updated = models.DateTimeField(auto_now=True)
    
    class Meta:
        managed = False
        verbose_name = "Content Cache"
        verbose_name_plural = "Content Cache"
