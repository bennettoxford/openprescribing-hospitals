from django.db import models

# Create your models here.

class Dose(models.Model):
    year_month = models.DateField()
    vmp_code = models.CharField(max_length=20)
    vmp_name = models.CharField(max_length=255)
    ods_code = models.CharField(max_length=10)
    ods_name = models.CharField(max_length=255)
    SCMD_quantity = models.FloatField()
    SCMD_quantity_basis = models.CharField(max_length=50)
    dose_quantity = models.FloatField()
    converted_udfs = models.FloatField()
    udfs_basis = models.CharField(max_length=50)
    dose_unit = models.CharField(max_length=50)
    df_ind = models.CharField(max_length=50)

    def __str__(self):
        return f"{self.vmp_name} - {self.ods_name} - {self.year_month}"

    class Meta:
        ordering = ['year_month', 'vmp_name', 'ods_name']
