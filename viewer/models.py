from django.db import models


class VTM(models.Model):
    vtm = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return self.name


class VMP(models.Model):
    code = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255)
    vtm = models.ForeignKey(VTM, on_delete=models.CASCADE, related_name='vmps', null=True)
    ingredients = models.ManyToManyField('Ingredient', related_name='vmps', null=True)

    def __str__(self):
        return f"{self.name} ({self.code})"


class Ingredient(models.Model):
    code = models.CharField(max_length=20, primary_key=True)
    name = models.CharField(max_length=255, null=False)

    def __str__(self):
        return f"{self.name} ({self.code})"


class Organisation(models.Model):
    ods_code = models.CharField(max_length=10, primary_key=True)
    ods_name = models.CharField(max_length=255, null=False)
    region = models.CharField(max_length=100, null=False)

    def __str__(self):
        return f"{self.ods_name} ({self.ods_code})"


class Dose(models.Model):
    year_month = models.DateField()
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name='doses')
    quantity = models.FloatField(null=True)
    unit = models.CharField(max_length=50)
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE,
                                     related_name='doses')

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name} - {self.year_month}"

    class Meta:
        ordering = ['year_month', 'vmp__name', 'organisation__ods_name']


class SCMD(models.Model):
    year_month = models.DateField()
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE, related_name='scmds')
    quantity = models.FloatField(null=True)
    unit = models.CharField(max_length=50)
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE,
                                     related_name='scmds')

    def __str__(self):
        return f"{self.vmp.name} - {self.organisation.ods_name} - {self.year_month}"

    class Meta:
        ordering = ['year_month', 'vmp__name', 'organisation__ods_name']


class IngredientQuantity(models.Model):
    year_month = models.DateField()
    ingredient = models.ForeignKey(Ingredient, on_delete=models.CASCADE,
                                   related_name='ingredient_quantities')
    vmp = models.ForeignKey(VMP, on_delete=models.CASCADE,
                            related_name='ingredient_quantities')
    quantity = models.FloatField(null=True)
    unit = models.CharField(max_length=50, null=True)
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE,
                                     related_name='ingredient_quantities')

    def __str__(self):
        return (f"{self.ingredient.name} - {self.vmp.name} - "
                f"{self.organisation.ods_name} - {self.year_month}")

    class Meta:
        ordering = ['year_month', 'ingredient__name', 'vmp__name',
                    'organisation__ods_name']
