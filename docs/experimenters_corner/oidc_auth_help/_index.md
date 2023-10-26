**This is a feature that may come to fruition in a future poms release.**

The basic use is for Analysis users to be able to acquire a vault token via the POMS webservice, rather than with the use of poms_client.

Currently, poms_client uses _htvaulttoken_ to get a vault token, where the user receives an OIDC/OIDC-Kerberos link to follow.

Once the user authenticates via the link provided, poms_client receives a vault token from the vault server, and uploads it to the poms #UPLOADS directory. This can be seen as an Analysis user on the main menu under "User Data" -> "Uploaded Files"