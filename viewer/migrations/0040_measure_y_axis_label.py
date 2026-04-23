from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("viewer", "0039_add_shelford_group"),
    ]

    operations = [
        migrations.AddField(
            model_name="measure",
            name="y_axis_label",
            field=models.CharField(
                blank=True,
                help_text="Optional override for chart y-axis label; when unset, the label is derived from quantity type and products",
                max_length=255,
                null=True,
            ),
        ),
    ]
