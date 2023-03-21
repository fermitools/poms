import DBHandle
import datetime
import time
import sys
import glob
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import CampaignStage
from os.path import basename
import os

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig
from mock_Ctx import Ctx

sesshandle = camp_seshandle
samhandle = samweb_lite()

dbh = DBHandle.DBHandle()
dbhandle = dbh.get()

import utils
from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import logging
from poms.webservice.jobsub_fetcher import jobsub_fetcher

logger = logging.getLogger("cherrypy.error")
# when I get one...


mps = mock_poms_service()
config = utils.get_config()


#
# This bears a little explanation.
#
# make_split makes and returns a split testing function that tests
# a split spec on a definition, and makes sure it gets 3 sets of
# files from it.
#
# Then we use make_split to make a split test for each of the splits.
#
# In this case we also use our gen_cfg dataset, which while it only has
# 5 fake .fcl files in it, they have as well:
# * differing run numbers 1 through 3
# * differing end_time values one day apart
# in their metadata so the tests will all find something.
#


def make_split(ds, split, should_hit_end):
    def test_splits():

        ctx = Ctx(db=dbhandle, sam=samhandle)
        myds = ds
        check_end = should_hit_end
        if should_hit_end == 2:
            # special case for staged_files which takes a project...
            projname = "test_poms_%d" % time.time()
            print("starting project %s" % projname)

            os.system("samweb -e samdev prestage-dataset --name %s --defname %s" % (projname, ds))
            time.sleep(5)
            myds = projname
            check_end = 1

        cs = dbhandle.query(CampaignStage).filter(CampaignStage.name == "mwm_test_splits").first()
        cs.dataset = myds
        cs.cs_split_type = split
        dbhandle.commit()
        mps.stagesPOMS.reset_campaign_split(ctx, cs.campaign_stage_id)
        # logger.debug("testing %s on %s" % (split, ds))
        print("testing %s on %s" % (split, ds))
        hit_end = 0
        for i in range(1, 6):
            try:
                res = mps.stagesPOMS.get_dataset_for(ctx, cs, False)
            except AssertionError:
                print("Hit end!")
                hit_end = 1
                break

            n = samhandle.count_files(cs.experiment, "defname:" + res)
            print("got %s with %d files" % (res, n))

            if check_end and hit_end:
                assert n > 0

        if check_end:
            assert hit_end == check_end
        else:
            assert True

    return test_splits


split_table = [
    ["a,b,gen_cfg", "list()", 1],
    ['[["a","b"],["c","d"],["e","f"]]', "multiparam()", 0],
    ["gen_cfg", "byrun(low=1,high=4)", 0],
    ["gen_cfg", "byrun( low=1, high=4 ) ", 0],
    ["gen_cfg", "byrun(low=1,high=3)", 1],  # make sure we hit the end..
    ["gen_cfg", "byexistingruns() ", 0],
    ["gen_cfg", "draining", 0],
    ["gen_cfg", "mod(3)", 1],
    ["gen_cfg", "mod( 3 )", 1],
    ["gen_cfg_slice0_of_3,gen_cfg_slice1_of_3,gen_cfg_slice2_of_3", "list", 1],
    ["gen_cfg_slice0_of_3, gen_cfg_slice1_of_3, gen_cfg_slice2_of_3", "list", 1],
    ["gen_cfg", "new(firsttime=1475280000,window=1d,lasttime=1475539200)", 1],
    ["gen_cfg", "new( firsttime=1475280000, window=1d, lasttime=1475539200 )", 1],
    ["gen_cfg", "nfiles(2)", 1],
    ["gen_cfg", "drainingn(2)", 1],
    ["gen_cfg", "stagedfiles(2)", 2],
    ["gen_cfg", "limitn(2)", 0],
]

for ds, splitt, should_hit_end in split_table:
    if splitt.find("(") > 0:
        base = splitt[: splitt.find("(")]
    else:
        base = splitt
    name = "test_split_" + base
    #
    # this is the magic to actually define the test function of that name
    # at the global scope
    #
    globals()[name] = make_split(ds, splitt, should_hit_end)


def test_coverage():
    """
        Make sure we have tests for everything in our split_types directory
    """
    have_test_for = {}
    for ds, splitt, should_hit_end in split_table:
        if splitt.find("(") > 0:
            base = splitt[: splitt.find("(")]
        else:
            base = splitt
        have_test_for[base + ".py"] = True
        print("have_test_for %s.py" % base)

    for f in glob.glob("../webservice/split_types/[a-z]*.py"):
        bn = basename(f)
        print("checking for coverage of %s" % bn)
        assert have_test_for.get(bn, False)
