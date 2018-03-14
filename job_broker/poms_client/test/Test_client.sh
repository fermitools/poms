#!usr/bin/env bash

source unittest.bash

test_launch_template_add() {
    launch_template_edit -v --launch_name poms_client_test_1 --host novagpvm02 --user ahandres --action add --experiment samdev --test_client True --setup echo
}

test_launch_template_edit() {
    launch_template_edit -v --launch_name poms_client_test_1 --host novagpvm02 --user ahandres --user ahandres --action edit --experiment samdev --test_client True --setup echo
}

test_campaign_add() {

    campaign_edit -v --action add --campaign_name test_from_client1 --user ahandres --experiment samdev --vo_role Analysis --dataset novat_test --launch_name poms_client_test_1 --job_type test --state True --split_type None --software_version V1 --completion_type located --completion 50 --test_client True

}


test_campaign_edit() {

    campaign_edit -v --action edit --campaign_name test_from_client1 --user ahandres --experiment samdev --vo_role Analysis --dataset novat_test --launch_name poms_client_test_1 --job_type test --state True --split_type None --software_version V1 --completion_type located --completion 50 --test_client True

}

test_register_poms_campaign() {
  
    register_poms_campaign --campaign="mwm_demo_test" --user=$USER --experiment=samdev --version=v1_0 --campaign-definition=test_launch_mock_job --dataset=gen_cfg --test --poms_role=production
}

testsuite command_tests \
    test_launch_template_add \
    test_launch_template_edit \
    test_campaign_add \
    test_campaign_edit \
    test_register_poms_campaign \
    

command_tests "$@"
