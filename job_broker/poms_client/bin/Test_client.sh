#!usr/bin/env bash
echo "launch template edit"
python launch_template_edit --name my_test_may17 --host novagpvm02 --user_account ahandres --email ahandres@fnal.gov --action add --experiment samdev --test_client True --setup echo

sleep 1000
echo "job type"

python campaign_edit --action add --campaign_name test_from_client1 --email ahandres@fnal.gov --experiment samdev --vo_role Analysis --dataset novat_test --launch_name test_3 --job_type test --state Active --split_type None --software_version V1 --completion_type located --completion 50 --test_client True

echo "campagin_edit add"
sleep 1000


python campaign_edit --action edit --campaign_name test_from_client1 --email ahandres@fnal.gov --experiment samdev --vo_role Analysis --dataset novat_test --launch_name nova_test --job_type test --state Active --split_type None --software_version V1 --completion_type located --completion 50 --test_client True
echo "campaign_edit edit"
sleep 1000
