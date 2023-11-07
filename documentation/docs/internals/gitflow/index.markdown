---
layout: page
title: Gitflow
---
* TOC
{:toc}
GitFlow is branching model for Git. After POMS project joined more developers, we decided to use this model to organize the development task.

## Briefly explanation of the model:

In the project, the model you can find 5 kinds of branches. (Figure 1)

**Master:** This branch is like the production branch. You should never develop on that one. For POMS case, this is the one that is going to be running in https://pomsgpvm01.fnal.gov/poms/  
**Develop:** Developers merge their new features into that branch every time they completed them.  
**Features:** Everytime you want to create a new feature or fix a previous bug. You should create a new feature and do the development on that branch. In cases where more than one person is working on the same features, they can publish the feature in order to share with the others developers. When a feature is complete, it should be merged into the develop branch.  
**Release:** After all the features that belong to certain release are completed, a release branch should be created. You should not develop new features on the release branch but you can fix bugs. Bugfixes from release branch may be continuously merged into develop branch. After the team decided the release is completely ready, the release branch is merged into master.  
**Hotfix:** If a bug is found after merging into the master branch, and it is an emergency fix. The developer should use the hotfix branch in order to solve the problem.

![GitFlowComplete.png]({{ site.url }}/docs/images/GitFlowComplete.png)
**Figure 1.**  
Image extracted from [1]

## Most common commands in GitFlow:
 
      git flow init
  >* Initializing git inside an existing git repository    


      git flow feature start MYFEATURE 
  >* This action creates a new feature branch based on 'develop' and switches to it.

      git flow feature publish MYFEATURE
  >* Publish a feature to the remote server so it can be used by other users.

      git flow feature pull origin MYFEATURE
  >* Get a feature published by another user.


      git flow feature finish MYFEATURE
  >* Merges MYFEATURE into 'develop'
  >* Removes the feature branch
  >* Switches back to 'develop' branch

      
      git flow release start RELEASE [BASE]
  >* It creates a release branch created from the 'develop' branch. (You may also publish a release after you create it) 


      git flow release publish RELEASE
  >* Publish the release branch 


      git flow release finish RELEASE
  >* Merges the release branch back into 'master'
  >* Tags the release with its name
  >* Back-merges the release into 'develop'
  >* Removes the release branch


      git flow hotfix start VERSION [BASENAME]
  >* Create a new branch to fix the bug found into the master branch. The version argument hereby marks the new hotfix release name. 
Optionally you can specify a basename to start from.



      git flow hotfix finish VERSION
  >* The hotfix it gets merged back into develop and master. 
  >* The hotfix it gets merged back into master
  >* The master merge is tagged with the hotfix version.


## How to install GitFlow Linux

    $ curl -OL https://raw.github.com/nvie/gitflow/develop/contrib/gitflow-installer.sh
    $ chmod +x gitflow-installer.sh
    $ sudo ./gitflow-installer.sh

#### References:
[1][https://datasift.github.io/gitflow/IntroducingGitFlow.html](https://datasift.github.io/gitflow/IntroducingGitFlow.html)  
[2] [https://danielkummer.github.io/git-flow-cheatsheet/](https://danielkummer.github.io/git-flow-cheatsheet/)