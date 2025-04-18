# Generated by Django 5.1 on 2025-04-14 08:24

import django.contrib.postgres.fields
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('viewer', '0016_alter_ontformroute_who_route'),
    ]

    operations = [
        migrations.CreateModel(
            name='IndicativeCost',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('data', django.contrib.postgres.fields.ArrayField(base_field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=255, null=True), size=2), help_text='Array of [year_month, quantity] entries', null=True, size=None)),
                ('organisation', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='indicative_costs', to='viewer.organisation')),
                ('vmp', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='indicative_costs', to='viewer.vmp')),
            ],
            options={
                'indexes': [models.Index(fields=['vmp', 'organisation'], name='viewer_indi_vmp_id_ee0401_idx')],
                'unique_together': {('vmp', 'organisation')},
            },
        ),
    ]
