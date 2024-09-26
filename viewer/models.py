from django.db import models
from django.utils.text import slugify

class VTM(models.Model):
    vtm = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class VMP(models.Model):
    code = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255)
    vtm = models.ForeignKey(
        VTM, on_delete=models.CASCADE, related_name="vmps", null=True
    )
    ingredients = models.ManyToManyField(
        "Ingredient", related_name="vmps", null=True)

    def __str__(self):
        return f"{self.name} ({self.code})"

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
        ]


class Ingredient(models.Model):
    code = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255, null=False)

    def __str__(self):
        return f"{self.name} ({self.code})"


class Organisation(models.Model):
    ods_code = models.CharField(max_length=10, primary_key=True)
    ods_name = models.CharField(max_length=255, null=False)
    region = models.CharField(max_length=100, null=False)
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


class Measure(models.Model):
    name = models.CharField(max_length=255, unique=True)
    short_name = models.CharField(max_length=255, null=True)
    slug = models.SlugField(unique=True, null=True)
    description = models.TextField()
    sql_file = models.CharField(max_length=255)
    category = models.CharField(max_length=255, null=True)
    why = models.CharField(max_length=255, null=True)

    def save(self, *args, **kwargs):
        if not self.slug and self.short_name:
            self.slug = slugify(self.short_name)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.name
