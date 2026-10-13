import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("viewer", "0043_measuregroup"),
    ]

    operations = [
        migrations.AddField(
            model_name="measuregroup",
            name="flagship_measure",
            field=models.OneToOneField(
                blank=True,
                help_text="Measure shown first on the group card in the measures list.",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="flagship_group",
                to="viewer.measure",
            ),
        ),
    ]
