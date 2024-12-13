import os
from django.db import connection
from django.conf import settings
from ..models import Measure


def execute_measure_sql(measure_name):
    try:
        measure = Measure.objects.get(name=measure_name)
    except Measure.DoesNotExist:
        raise ValueError(f"Measure '{measure_name}' not found")

    file_path = os.path.join(
        settings.BASE_DIR,
        "viewer",
        "measures",
        measure.sql_file)

    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"SQL file for measure '{measure_name}' not found")

    with open(file_path, "r") as sql_file:
        sql = sql_file.read()

    with connection.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()

    if result:
        return result

    else:
        return None
