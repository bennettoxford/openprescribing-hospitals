import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)


from prefect import flow, get_run_logger

from viewer.management.commands.import_measures import Command as ImportMeasuresCommand
from viewer.management.commands.get_measure_vmps import Command as GetMeasureVMPsCommand
from viewer.management.commands.compute_measures import Command as ComputeMeasuresCommand
from viewer.models import Measure

@flow(name="Generate Measures")
def generate_measures():
    logger = get_run_logger()
    logger.info("Running measure-related management commands")

    try:

        logger.info("Importing measures")
        import_measures = ImportMeasuresCommand()
        import_measures.handle()
        logger.info("Successfully imported measures")

        logger.info("Getting measure VMPs")
        get_measure_vmps = GetMeasureVMPsCommand()
        
        for measure in Measure.objects.all():
            try:
                logger.info(f"Processing VMPs for measure: {measure.slug}")
                get_measure_vmps.handle(measure=measure.slug)
                logger.info(f"Successfully processed VMPs for measure: {measure.slug}")
            except Exception as e:
                logger.warning(f"Failed to process VMPs for measure {measure.slug}: {e}")
                continue 
        logger.info("Completed processing measure VMPs")

        logger.info("Computing measures")
        compute_measures = ComputeMeasuresCommand()
        for measure in Measure.objects.all():
            logger.info(f"Computing measure: {measure.slug}")
            compute_measures.handle(measure=measure.slug)
        logger.info("Successfully computed measures")

    except Exception as e:
        logger.error(f"Error in measure-related commands: {e}")
        raise e

    logger.info("Completed generating measures")


if __name__ == "__main__":
    generate_measures() 