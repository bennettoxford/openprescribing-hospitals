# Generated by Django 4.2.17 on 2024-12-16 16:43

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("viewer", "0004_vmp_atcs"),
    ]

    operations = [
        migrations.CreateModel(
            name="DDD",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("ddd", models.FloatField()),
                ("unit_type", models.CharField(max_length=255)),
                (
                    "route",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ddds",
                        to="viewer.route",
                    ),
                ),
                (
                    "vmp",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="ddds",
                        to="viewer.vmp",
                    ),
                ),
            ],
            options={
                "indexes": [
                    models.Index(fields=["vmp"], name="viewer_ddd_vmp_id_7c97f1_idx"),
                    models.Index(
                        fields=["vmp", "route"], name="viewer_ddd_vmp_id_13d2b4_idx"
                    ),
                ],
            },
        ),
    ]
