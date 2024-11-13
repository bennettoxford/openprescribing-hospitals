from django.db import models
from django.utils.text import slugify
from django.core.validators import RegexValidator

class VTM(models.Model):
    vtm = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class VMP(models.Model):
    code = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255)
    form = models.CharField(max_length=255, null=True)
    vtm = models.ForeignKey(
        VTM, on_delete=models.CASCADE, related_name="vmps", null=True
    )
    ingredients = models.ManyToManyField(
        "Ingredient", related_name="vmps", null=True)
    atcs = models.ManyToManyField("ATC", related_name="vmps", null=True)
    ont_form_routes = models.ManyToManyField("OntFormRoute", related_name="vmps", null=True)

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]

class OntFormRoute(models.Model):
    name = models.CharField(max_length=255)

    def __str__(self):
        return f"{self.name}"

class Ingredient(models.Model):
    code = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255, null=False)

    def __str__(self):
        return f"{self.name} ({self.code})"


class Organisation(models.Model):
    ods_code = models.CharField(max_length=10, primary_key=True)
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
        ]

class SCMDQuantity(models.Model):
    year_month = models.DateField()
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name="scmd_quantities")
    quantity = models.FloatField(null=True)
    unit = models.CharField(max_length=50, null=True)
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="scmd_quantities")

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name} - {self.year_month}"
    
    class Meta:
        ordering = ["year_month", "vmp__name", "organisation__ods_name"]
        indexes = [
            models.Index(fields=["vmp"]),
            models.Index(fields=["organisation"]),
            models.Index(fields=["year_month"]),
        ]

class Dose(models.Model):
    year_month = models.DateField()
    vmp = models.ForeignKey(
        VMP,
        on_delete=models.CASCADE,
        related_name="doses")
    quantity = models.FloatField(null=True)
    unit = models.CharField(max_length=50)
    organisation = models.ForeignKey(
        Organisation, on_delete=models.CASCADE, related_name="doses"
    )

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name} - {self.year_month}"

    class Meta:
        ordering = ["year_month", "vmp__name", "organisation__ods_name"]
        indexes = [
            models.Index(fields=["vmp"]),
            models.Index(fields=["organisation"]),
            models.Index(fields=["year_month"]),
        ]


class IngredientQuantity(models.Model):
    year_month = models.DateField()
    ingredient = models.ForeignKey(
        Ingredient,
        on_delete=models.CASCADE,
        related_name="ingredient_quantities")
    vmp = models.ForeignKey(
        VMP, on_delete=models.CASCADE, related_name="ingredient_quantities"
    )
    quantity = models.FloatField(null=True)
    unit = models.CharField(max_length=50, null=True)
    organisation = models.ForeignKey(
        Organisation,
        on_delete=models.CASCADE,
        related_name="ingredient_quantities")

    def __str__(self):
        return (
            f"{self.ingredient.name} - {self.vmp.name} - "
            f"{self.organisation.ods_name} - {self.year_month}"
        )

    class Meta:
        ordering = [
            "year_month",
            "ingredient__name",
            "vmp__name",
            "organisation__ods_name",
        ]
        indexes = [
            models.Index(fields=["vmp"]),
            models.Index(fields=["organisation"]),
            models.Index(fields=["ingredient"]),
            models.Index(fields=["year_month"]),
        ]

class MeasureReason(models.Model):
    reason = models.CharField(max_length=255)
    description = models.TextField(null=True)
    colour = models.CharField(max_length=255, null=True)

    def __str__(self):
        return f"{self.reason}"

class Measure(models.Model):
    name = models.CharField(max_length=255, unique=True)
    short_name = models.CharField(max_length=255, null=True)
    slug = models.SlugField(unique=True, null=True)
    description = models.TextField(null=True)
    why_it_matters = models.TextField()
    how_is_it_calculated = models.TextField(null=True)
    sql_file = models.CharField(max_length=255)
    reason = models.ForeignKey(MeasureReason, on_delete=models.CASCADE, related_name="measures", null=True)
    draft = models.BooleanField(default=True)
    numerator_vmps = models.ManyToManyField(VMP, related_name="measures_numerator")
    denominator_vmps = models.ManyToManyField(VMP, related_name="measures_denominator")

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

    class Meta:
        unique_together = ('organisation', 'month')

class ATC(models.Model):
    code = models.CharField(
        max_length=7,
        primary_key=True,
        validators=[
            RegexValidator(
                regex=r'^[A-Z](\d{2}([A-Z](\d{2}([A-Z](\d{2})?)?)?)?)?$',
                message='Invalid ATC code format',
            )
        ]
    )
    bnf_code = models.CharField(max_length=20, null=True)
    name = models.CharField(max_length=255)
    level = models.IntegerField()
    parent = models.ForeignKey('self', null=True, blank=True, on_delete=models.CASCADE, related_name='children')

    def save(self, *args, **kwargs):
        self.level = len(self.code)
        if self.level > 1:
            self.parent = ATC.objects.get(code=self.code[:-2])
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=['code']),
            models.Index(fields=['level']),
            models.Index(fields=['parent']),
        ]


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
