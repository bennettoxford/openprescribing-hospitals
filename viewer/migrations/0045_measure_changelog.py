from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("viewer", "0044_measuregroup_flagship_measure"),
    ]

    operations = [
        migrations.AddField(
            model_name="measure",
            name="changelog",
            field=models.TextField(
                blank=True,
                help_text="Optional history of measure changes. Each line is '- YYYY-MM-DD: description'.",
                null=True,
            ),
        ),
    ]
