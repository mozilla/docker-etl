from argparse import ArgumentParser
import logging
from workday_netsuite.api.workday import WorkDayRaaService
from api.util import Util

 
class WorkdayToNetsuiteIntegration():
    """Integration class for syncing data from Workday to Netsuite.

    Args:
        args (Args): Arguments for the integration.
    """
    def __init__(self,) -> None:
        self.workday_service = WorkDayRaaService()
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        """Run all the steps of the integration"""

        # Step 1: Get list of workers from workday
        self.logger.info("Step 1: Gathering data to run the transformations. ")
        wd_workers = self.workday_service.get_listing_of_workers()
        self.logger.info(f"{len(wd_workers)} workers returned from Workday.")

        # Step 2: Perform data transformations
        self.logger.info("Step 2: Transforming Workday data.")


if __name__ == "__main__":
    parser = ArgumentParser(description="Slack Channels Integration ")

    parser.add_argument(
        "-l",
        "--level",
        action="store",
        help="log level (debug, info, warning, error, or critical)",
        type=str,
        default="info",
    )
   
    parser.add_argument(
        "-f",
        "--force", 
        action="store",
        type=int,
        help="If true, the script will run and delete and archive channels, otherwise it will only report the channels",
        default=40
    )
    args = None
    args = parser.parse_args()
    
    log_level = Util.set_up_logging(args.level)

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
    logger.info(f"force={args.force}")
    
    WD = WorkdayToNetsuiteIntegration()

    logger = logging.getLogger("main")
    logger.info('Starting Workday to Netsuite Integration ...')

    WD.run()
