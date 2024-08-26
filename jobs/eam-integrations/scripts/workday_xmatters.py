from __future__ import division
from api import XMatters, Workday
from api.util import Util
import sys
import os
import re
import argparse
import logging

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")

# from integrations.api.connectors import XMatters, Workday
# from integrations.api.connectors import Util


def user_data_matches(wd_user, xm_user):
    site_key = (
        wd_user.get("User_Home_Country", "")
        + ":"
        + wd_user.get("User_Home_Postal_Code", "")
    )
    try:
        if wd_user["User_Preferred_First_Name"] != xm_user["firstName"]:
            logger.info(
                "MISMATCH (first name): %s <-> %s"
                % (wd_user["User_Preferred_First_Name"], xm_user["firstName"])
            )
            return False
        elif wd_user["User_Preferred_Last_Name"] != xm_user["lastName"]:
            logger.info(
                "MISMATCH (last name): %s <-> %s"
                % (wd_user["User_Preferred_Last_Name"], xm_user["lastName"])
            )
            return False
        elif site_key != xm_user["site"]["name"]:
            logger.info(
                "MISMATCH (site name): %s <-> %s" % (site_key, xm_user["site"]["name"])
            )
            return False
        elif wd_user.get("User_Home_City", "") != xm_user["properties"]["Home City"]:
            logger.info(
                "MISMATCH (home city): %s <-> %s"
                % (wd_user["User_Home_City"], xm_user["properties"]["Home City"])
            )
            return False
        elif (
            wd_user.get("User_Home_Country", "")
            != xm_user["properties"]["Home Country"]
        ):
            logger.info(
                "MISMATCH (home country): %s <-> %s"
                % (wd_user["User_Home_Country"], xm_user["properties"]["Home Country"])
            )
            return False
        elif (
            wd_user.get("User_Home_Postal_Code", "")
            != xm_user["properties"]["Home Zipcode"]
        ):
            logger.info(
                "MISMATCH (home zipcode): %s <-> %s"
                % (
                    wd_user["User_Home_Postal_Code"],
                    xm_user["properties"]["Home Zipcode"],
                )
            )
            return False
        else:
            return True
    except KeyError:
        logger.warning("Some key was not found, assuming a missing field in XMatters")
        return False


def iterate_thru_wd_users(wd_users, xm_users, xm_sites, limit):
    wd_users_seen = {}
    xm_add_users = []
    num_changes = 0
    for user in wd_users:
        if "User_Email_Address" not in user:
            logger.info(
                "Workday User ID %s (%s) has no email address! Skipping."
                % (
                    user["User_Employee_ID"],
                    user["User_Preferred_First_Name"]
                    + " "
                    + user["User_Preferred_Last_Name"],
                )
            )
            continue
        elif not re.search(
            "(?:mozilla.com|mozillafoundation.org)$",
            user["User_Email_Address"],
        ):
            logger.info(
                "User {} has non-matching email. Skipping.".format(
                    user["User_Email_Address"]
                )
            )
            continue
        wd_users_seen[user["User_Email_Address"]] = 1
        if user["User_Email_Address"] in xm_users:
            logger.debug("User %s found in XM" % user["User_Email_Address"])
            if not user_data_matches(user, xm_users[user["User_Email_Address"]]):                
                if num_changes < limit:
                    logger.debug("USER DATA NO MATCHES!")
                    XMatters.update_user(
                        user, xm_users[user["User_Email_Address"]], xm_sites
                    )
                    num_changes +=1
            else:
                logger.debug("%s good" % user["User_Email_Address"])
        else:
            # add user to XM
            # XMatters.add_user(user, xm_sites)
            xm_add_users.append(user)
            # time.sleep(5)
         
   
    logger.info(f"Number of updated users:{num_changes}")
    return wd_users_seen, xm_add_users


def get_wd_sites_from_users(users):
    unique_sites = {}
    for user in users:
        city = user.get("User_Home_City", "")
        country = user.get("User_Home_Country", "")
        postal = user.get("User_Home_Postal_Code", "")
        unq_key = country + ":" + postal
        if not country:
            logger.debug("NO COUNTRY!!")
            logger.debug(user)
            country = "United States of America"
        if not postal:
            logger.debug("NO POSTAL!!")
            logger.debug(user)
            postal = "97209"

        if unq_key not in unique_sites:
            unique_sites[unq_key] = {
                "name": unq_key,
                "country": country,
                "city": city,
                "postal_code": postal,
            }

    return unique_sites


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Sync up XMatters with Workday")

    parser.add_argument(
        "-l",
        "--level",
        action="store",
        help="log level (debug, info, warning, error, or critical)",
        type=str,
        default="info",
    )
     
    parser.add_argument(
        "-m",
        "--max_limit", 
        action="store",
        type=int,
        help="limit the number of changes in Everfi",        
        default=20
    )
    args = parser.parse_args()

    Util.set_up_logging(args.level)

    logger = logging.getLogger(__name__)

    logger.info("Starting...")

    # get all sites in xmatters
    xm_sites, xm_sites_inactive = XMatters.get_all_sites()
    

    # get all users from workday
    wd_users = Workday.get_users()
    

    # get the new style (zipcodes) sites from the user list
    wd_sites = get_wd_sites_from_users(wd_users)

    logger.info(f"Number of XMatters sites: {len(xm_sites)}")
    logger.info(f"Number of Workday sites: {len(wd_sites)}")
    logger.info(f"Number of Workday users: {len(wd_users)}")

    #  # get list of sites from workday users
    #  wd_sites = Workday.get_sites()

    # sites_percentage = len(xm_sites) / len(wd_sites)
    # if sites_percentage > 1.1 or sites_percentage < 0.9:
    #     logger.critical(
    #         "The number of sites in Workday vs XMatters is \
    #              different by more than 10%% (%.02f%%)."
    #         % (abs(100 - sites_percentage * 100))
    #     )
    #     logger.critical("Stopping unless --force")
    #     if not args.force:
    #         exit(42)

    # add_task any sites in workday that aren't in xmatters to xmatters
    xm_sites_in_wd = XMatters.add_new_sites(wd_sites, xm_sites, xm_sites_inactive, args.max_limit)

    # delete any sites NOT in workday that ARE in xmatters
    XMatters.delete_sites(xm_sites, xm_sites_in_wd, args.max_limit)

    # re-get all sites in xmatters
    xm_sites, xm_sites_inactive = XMatters.get_all_sites()

    # get all users from xmatters
    xm_users = XMatters.get_all_people()
    logger.info(f"Number of XMatters users: {len(xm_users)}")
    # users_percentage = len(xm_users) / len(wd_users)
    # if users_percentage > 1.1 or users_percentage < 0.9:
    #     logger.critical(
    #         "The number of users in Workday vs XMatters is \
    #         different by more than 10%% (%.02f%%)."
    #         % (abs(100 - users_percentage * 100))
    #     )
    #     logger.critical("Stopping unless --force")
    #     if not args.force:
    #         exit(42)

    # iterate thru users in workday:
    #   if not in xmatters, add_task to xmatters
    #   if data doesn't match xmatters, update xmatters
    #   mark-as-seen in xmatters
    users_seen_in_workday, xm_add_users = iterate_thru_wd_users(wd_users,
                                                                xm_users, xm_sites,
                                                                args.max_limit)

    # iterate through xmatters users who aren't marked-as-seen
    #   remove from xmatters
    XMatters.delete_users(xm_users, users_seen_in_workday, args.max_limit)

    for user in xm_add_users[:args.max_limit]:
        logger.info(f"Adding user: {user['User_Email_Address']}")
        XMatters.add_user(user, xm_sites)

    logger.info(f"Number of users added:{len(xm_add_users[:args.max_limit])}")
    logger.info("Finished.")
