# Generated by Django 4.2.17 on 2025-02-18 10:12

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("viewer", "0008_measure_authored_by_measure_checked_by_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="measure",
            name="sql_file",
        ),
    ]
