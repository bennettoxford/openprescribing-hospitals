import os
from django.db import connection
from django.conf import settings
from ..models import Measure
import json


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
        result = cursor.fetchone()

    if result:
        return {
            "name": result[0],
            "description": result[1],
            "unit": result[2],
            "values": json.loads(result[3]),
        }
    else:
        return {
            "name": measure.name,
            "description": measure.description,
            "unit": None,
            "values": [],
        }
