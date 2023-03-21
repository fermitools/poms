import argparse
import logging
import time
import psycopg2

def parse_command_line():
    doc = "One time update script for tags/campaignName conversions."
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument('host', help="Host database is on.")
    parser.add_argument('port', type=int, help="Database Port number.")
    parser.add_argument('dbname', help="Database to connect to.")
    parser.add_argument('user', help="User to connect as")
    parser.add_argument('-v', '--verbose', action="store_true", help='Output log data to screen')
    parser.add_argument('-l', '--log_dir', help="Output directory for log file.")
    return parser.parse_args()

def set_logging(log_dir, verbose):
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.DEBUG)
    if log_dir:
        log_file = "%s/addusers-%s.log" % (log_dir, time.strftime('%Y-%m-%d-%H%M%S'))
        fileHandler = logging.FileHandler(log_file)
        fileHandler.setFormatter(logFormatter)
        fileHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(fileHandler)
    if verbose:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        consoleHandler.setLevel(logging.DEBUG)
        rootLogger.addHandler(consoleHandler)

def campaigns_with_zero_tags(conn, cursor):
    sql = """
        select campaign_stage_id, name, experiment, creator, creator_role from campaign_stages
        where campaign_stage_id in (
            select campaign_stage_id from campaign_stages
            except
            select campaign_id from campaigns_tags_old)
        """   # because of naming issues, campaign_id really had campagign_stage_id in it
    logging.debug("zero_tags - getting stages with zero tags")
    cursor.execute(sql)
    logging.debug("zero_tags - creating campaigns and updating campaign_stages.campaign_id")
    for [campaign_stage_id, name, experiment, creator, creator_role] in cursor.fetchall():
        logging.debug("zero_tags - add campaign, experiment: %s name: %s creator: %s creator_role: %s", experiment, name, creator, creator_role)
        isql = """insert into campaigns
                  (experiment, name, creator, creator_role)
                  values('%s', '%s', %s, '%s')
                  returning campaign_id
            """ % (experiment, name, creator, creator_role)
        cursor.execute(isql)
        id_of_new_row = cursor.fetchone()[0]
        logging.debug("zero_tags - update stage,  campaign_stage_id: %s campaign_id: %s", campaign_stage_id, id_of_new_row)
        usql = "update campaign_stages set campaign_id=%s where campaign_stage_id=%s" % (id_of_new_row, campaign_stage_id)
        cursor.execute(usql)
    conn.commit()

def campaigns_with_one_tag(conn, cursor):
    # Stages with one tag, but that tag can be used on multiple stages.  Meaning, stages
    # with the same tag are part of the same campaign.  Use the tag as the campaign_name.
    sql = """
        select cs.campaign_stage_id, cs.experiment, cs.creator, cs.creator_role, ct.tag_id, t.tag_name
        from campaign_stages cs, campaigns_tags_old ct, tags t
        where campaign_stage_id in (
            select campaign_id
            from campaigns_tags_old
            group by campaign_id
            having count(*) = 1)
        and cs.campaign_stage_id =  ct.campaign_id
        and ct.tag_id = t.tag_id
        order by ct.tag_id"""   # campaign_id really holds stage ids - at this point
    logging.debug("one_tag - get stages with only one tag")
    cursor.execute(sql)
    logging.debug("one_tag - inserting campaings, updating stages and insert into campaings_tags")
    campaign_name = None
    new_campaign_id = None
    current_experiment = None
    for [campaign_stage_id, experiment, creator, creator_role, tag_id, tag_name] in cursor.fetchall():
        if (current_experiment != experiment) or (campaign_name != tag_name):
            logging.debug("one_tag - add campaign, experiment: %s creator: %s creator_role: %s", experiment, creator, creator_role)
            isql = """insert into campaigns
                    (experiment, creator, creator_role, name)
                    values('%s', %s, '%s', '%s')
                    returning campaign_id
                """ % (experiment, creator, creator_role, tag_name)
            cursor.execute(isql)
            new_campaign_id = cursor.fetchone()[0]

            logging.debug("one_tag - add campaigns_tags, tag_name: %s campaign_id: %s tag_id: %s", tag_name, new_campaign_id, tag_id)
            isql2 = """ insert into campaigns_tags
                        (campaign_id, tag_id)
                        values(%s, %s) """ % (new_campaign_id, tag_id)
            cursor.execute(isql2)

            current_experiment = experiment
            campaign_name = tag_name
        logging.debug("one_tag - update stage, campaign_stage_id: %s campaign_id:%s", campaign_stage_id, new_campaign_id)
        usql = "update campaign_stages set campaign_id=%s where campaign_stage_id=%s" % (new_campaign_id, campaign_stage_id)
        cursor.execute(usql)

    conn.commit()

def campaigns_with_many_tags():
    comment = """
        In case you need to clean up and re-run:
            delete from campaigns_tags;
            update campaign_stages set campaign_id=null where campaign_id is not null;
            delete from campaigns;

        **** Now you must handle the campaigns with many tags by hand.

        This query will give all campaigns with more then one tag:
            select campaign_id
            from campaigns_tags_old
            group by campaign_id
            having count(*) > 1;

        This query will give the tag names for each campaign stage:
            select ct.campaign_id as campaign_stage_id, cs.name, ct.tag_id, t.tag_name
            from campaigns_tags_old ct, tags t, campaign_stages cs
            where ct.tag_id = t.tag_id
                and cs.campaign_stage_id = ct.campaign_id
                and ct.campaign_id in (
                select campaign_id
                from campaigns_tags_old
                group by campaign_id
                having count(*) > 1);

        Update example with the above query:
            pomsint=> select * from campaign_dependencies
            where needs_campaign_stage_id=1406 or provides_campaign_stage_id=1406;
            campaign_dep_id | needs_campaign_stage_id | provides_campaign_stage_id | file_patterns
            -----------------+-------------------------+----------------------------+---------------
            (0 rows)

            pomsint=>
            pomsint=> insert into campaigns (experiment, name, creator, creator_role)
            values('dune', 'dc2_mc_keepup_no_poms', 5, 'production') returning campaign_id;
            campaign_id
            -------------
                    1372
            (1 row)

            INSERT 0 1
            pomsint=> update campaign_stages set campaign_id = 1372
            where campaign_id is null and campaign_stage_id in(1406);
            UPDATE 1
            pomsint=>

        """
    logging.debug("\n%s", comment)

def main():
    args = parse_command_line()
    set_logging(args.log_dir, args.verbose)
    logging.debug("main:POMS Database: %s Host: %s Port:%s", args.dbname, args.host, args.port)
    conn = psycopg2.connect("dbname=%s host=%s port=%s user=%s" % (args.dbname, args.host, args.port, args.user))
    cursor = conn.cursor()
    logging.debug('main - here we go...')
    campaigns_with_zero_tags(conn, cursor)
    campaigns_with_one_tag(conn, cursor)
    campaigns_with_many_tags()

    cursor.close()
    conn.close()
    logging.debug('main - done')

if __name__ == "__main__":
    main()
