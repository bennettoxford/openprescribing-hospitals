from django.db import models


class Organisation(models.Model):
    ods_code = models.CharField(max_length=10, unique=True)
    ods_name = models.CharField(max_length=255)
    region = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.ods_name} ({self.ods_code})"

class Dose(models.Model):
    year_month = models.DateField()
    vmp_code = models.CharField(max_length=20)
    vmp_name = models.CharField(max_length=255)
    SCMD_quantity = models.FloatField()
    SCMD_quantity_basis = models.CharField(max_length=50)
    dose_quantity = models.FloatField()
    converted_udfs = models.FloatField()
    udfs_basis = models.CharField(max_length=50)
    dose_unit = models.CharField(max_length=50)
    df_ind = models.CharField(max_length=50)

    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name='doses')

    def __str__(self):
        return f"{self.vmp_name} - {self.organisation.ods_name} - {self.year_month}"

    class Meta:
        ordering = ['year_month', 'vmp_name', 'organisation__ods_name']
