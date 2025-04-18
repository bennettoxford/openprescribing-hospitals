# Generated by Django 5.1 on 2025-04-04 10:35

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("viewer", "0015_vmp_who_routes"),
    ]

    operations = [
        migrations.AlterField(
            model_name="ontformroute",
            name="who_route",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="ont_form_routes",
                to="viewer.whoroute",
            ),
        ),
    ]
